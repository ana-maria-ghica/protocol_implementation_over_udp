#include <pthread.h>
#include <cstdlib>
#include <map>
#include <cstdint>
#include "lib.h"
#include "utils.h"
#include "protocol.h"
#include <poll.h>
#include <cassert>
#include <sys/timerfd.h>
#include <cerrno>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <unistd.h>

using namespace std;

std::map<int, struct connection *> cons;

struct pollfd data_fds[MAX_CONNECTIONS];
// used for timers per connection
struct pollfd timer_fds[MAX_CONNECTIONS];
int fdmax = 0;

// global vars
int recv_buffer_dim, conn_cnt, listenfd;

struct list_node {
    char payload[MAX_DATA_SIZE];
    int payload_len;
    int seq_num;
    list_node* next;
};

struct list {
    int size;
    int capacity;
    list_node* head;
};

// segments that arrived ahead of expected_seq, kept per connection
// (conn_id -> (seq_num -> segment)) until the gap before them is filled
std::map<int, std::map<int, list_node*>> out_of_order;

struct list* create_window() {
    struct list* window = (struct list*)calloc(1, sizeof(struct list));
    window->head = NULL;
    window->size = 0;
    window->capacity = 0;

    return window;
}

// add a segment at the end of the in-order list read by recv_data()
static void append_node(struct list* l, list_node* node)
{
    node->next = NULL;
    if (l->head == NULL) {
        l->head = node;
    } else {
        list_node* iter = l->head;
        while (iter->next != NULL)
            iter = iter->next;
        iter->next = node;
    }
    l->size++;
}

// free space in the receive buffer, in segments
// this is the window advertised to the sender in every ack (flow control)
static int free_window(struct connection* con)
{
    int capacity = recv_buffer_dim / MAX_DATA_SIZE;
    int used = con->recv_list->size + (int)out_of_order[con->conn_id].size();
    return used >= capacity ? 0 : capacity - used;
}

// cumulative ack: ack_num is the next seq_num expected in order
static void send_ack(struct connection* con)
{
    struct poli_tcp_ctrl_hdr ack;
    memset(&ack, 0, sizeof(struct poli_tcp_ctrl_hdr));
    ack.protocol_id = POLI_PROTOCOL_ID;
    ack.conn_id = con->conn_id;
    ack.type = ACK_TYPE;
    ack.ack_num = con->expected_seq;
    ack.recv_window = free_window(con);

    sendto(con->sockfd, &ack, sizeof(struct poli_tcp_ctrl_hdr), 0,
           (struct sockaddr*)&con->servaddr, sizeof(struct sockaddr_in));
}

int recv_data(int conn_id, char *buffer, int len)
{
    int size = 0;

    // lock, so we don't have sync problems with receiver_handler
    pthread_mutex_lock(&cons[conn_id]->con_lock);

    // take the connection from map
    struct connection *current_connection = cons[conn_id];
    list* current_list = current_connection->recv_list;

    // blocking function - if no data to read
    while (current_list->head == NULL) {
        pthread_mutex_unlock(&cons[conn_id]->con_lock);
        usleep(1000);
        pthread_mutex_lock(&cons[conn_id]->con_lock);
    }

    // if the window was closed (0), the sender is waiting for an ack to continue
    bool window_was_closed = free_window(current_connection) == 0;

    struct list_node* head = current_list->head;
    while (head != NULL) {
        if (size + head->payload_len > len)
            break;
        memcpy(buffer + size, head->payload, head->payload_len);
        size += head->payload_len;
        struct list_node* to_free = head;
        head = head->next;

        // free the node
        free(to_free);
        current_list->size--;
    }

    current_list->head = head;

    // window update: tell the sender right away that space was freed
    if (window_was_closed && size > 0)
        send_ack(current_connection);

    pthread_mutex_unlock(&cons[conn_id]->con_lock);

    return size;
}

void *receiver_handler(void *arg)
{

    char segment[MAX_SEGMENT_SIZE];
    int res;
    DEBUG_PRINT("Starting recviver handler\n");

    while (1) {

        int conn_id = -1;
        do {
            res = recv_message_or_timeout(segment, MAX_SEGMENT_SIZE, &conn_id);
        } while(res == -14);

        // handle the segment received from the sender between locks,
        // so we don't have sync problems with recv_data (main thread)
        pthread_mutex_lock(&cons[conn_id]->con_lock);

        // current connection
        struct connection *con = cons[conn_id];

        if (res == -1) {
            // timer tick => advertise the window again, so a sender stuck
            // on a full (zero) window finds out when space was freed
            send_ack(con);
        } else if (res >= (int)sizeof(struct poli_tcp_data_hdr)) {
            // header of the received segment
            struct poli_tcp_data_hdr *hdr = (struct poli_tcp_data_hdr*)segment;

            // accept only data segments of our protocol, with a valid length
            bool valid = hdr->protocol_id == POLI_PROTOCOL_ID &&
                         hdr->type == DATA_TYPE &&
                         hdr->len <= MAX_DATA_SIZE &&
                         res >= (int)(sizeof(struct poli_tcp_data_hdr) + hdr->len);

            if (valid) {
                int seq = hdr->seq_num;
                int capacity = recv_buffer_dim / MAX_DATA_SIZE;
                std::map<int, list_node*> &pending = out_of_order[conn_id];

                // keep the segment only if it fits in the receive window and
                // was not already received
                bool in_window = seq >= con->expected_seq &&
                                 seq < con->expected_seq + capacity;
                bool duplicate = pending.count(seq) > 0;

                if (in_window && !duplicate && free_window(con) > 0) {
                    list_node *node = (list_node*)calloc(1, sizeof(list_node));
                    memcpy(node->payload, segment + sizeof(struct poli_tcp_data_hdr), hdr->len);
                    node->payload_len = hdr->len;
                    node->seq_num = seq;

                    if (seq == con->expected_seq) {
                        // in order => deliver it, then every buffered segment
                        // that now follows without a gap
                        append_node(con->recv_list, node);
                        con->expected_seq++;

                        auto it = pending.find(con->expected_seq);
                        while (it != pending.end()) {
                            append_node(con->recv_list, it->second);
                            pending.erase(it);
                            con->expected_seq++;
                            it = pending.find(con->expected_seq);
                        }
                    } else {
                        // after a gap => keep it until the gap is filled
                        pending[seq] = node;
                    }
                }

                // always answer, even for duplicates: the previous ack
                // may have been the packet that got lost
                send_ack(con);
            }
        }

        pthread_mutex_unlock(&cons[conn_id]->con_lock);
    }


}

int wait4connect(uint32_t ip, uint16_t port)
{
    // receiver side of the three way handshake, blocks until the connection is established

    struct connection *con = (struct connection *)malloc(sizeof(struct connection));
    con->conn_id = conn_cnt;
    conn_cnt++;
    con->recv_list = create_window();
    con->expected_seq = 0;

    con->sockfd = socket(AF_INET, SOCK_DGRAM, 0);

    // receive timeout on the socket, used during the handshake
    struct timeval tv;
    tv.tv_sec = 2;
    tv.tv_usec = 100000;
    if (setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO,&tv,sizeof(tv)) < 0) {
        perror("Error");
    }

    // receive syn on the listening socket, create a new socket bound to a free port
    // and send that port to the client in the syn ack

    // client address
    struct sockaddr_in client_addr;
    memset(&client_addr, 0, sizeof(struct sockaddr_in));
    client_addr.sin_family = AF_INET;
    client_addr.sin_port = port;
    client_addr.sin_addr.s_addr = ip;

    // three way handshake
    // 1. receive syn (from client)
    char syn_buffer[MAX_SEGMENT_SIZE];
    socklen_t client_addr_len = sizeof(struct sockaddr_in);
    struct poli_tcp_ctrl_hdr syn;
    int rc;
    while(1) {
        rc = recvfrom(listenfd, syn_buffer, MAX_SEGMENT_SIZE, 0, (struct sockaddr*)&client_addr, &client_addr_len);
        if (rc < 0) {
            perror("recv failed");
            exit(EXIT_FAILURE);
        }

        // extract header from received buffer
        syn = *(struct poli_tcp_ctrl_hdr*)syn_buffer;
        if (syn.type != SYN_TYPE) {
            continue;
        } else {
            break;
        }
    }
    con->servaddr = client_addr;

    // bind the connection socket (port 0 => the kernel picks a free port)
    struct sockaddr_in connection_addr;
    socklen_t connection_addr_len = sizeof(struct sockaddr_in);
    memset(&connection_addr, 0, sizeof(struct sockaddr_in));
    connection_addr.sin_family = AF_INET;
    connection_addr.sin_port = 0;
    connection_addr.sin_addr.s_addr = ip;
    rc = bind(con->sockfd, (struct sockaddr*)&connection_addr, sizeof(struct sockaddr_in));
    if (rc < 0) {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }

    // find the port used for connection
    getsockname(con->sockfd, (struct sockaddr*)&connection_addr, &connection_addr_len);
    uint16_t new_connection_port = connection_addr.sin_port;

    // 2. construct syn ack to send to client
    int syn_ack_buffer_len = sizeof(struct poli_tcp_ctrl_hdr) + sizeof(uint16_t);
    char syn_ack_buffer[syn_ack_buffer_len];

    struct poli_tcp_ctrl_hdr syn_ack;
    memset(&syn_ack, 0, sizeof(struct poli_tcp_ctrl_hdr));
    syn_ack.protocol_id = POLI_PROTOCOL_ID;
    syn_ack.conn_id = con->conn_id;
    syn_ack.type = SYN_WITH_ACK_TYPE;
    syn_ack.ack_num = syn.seq_num + 1;
    syn_ack.recv_window = recv_buffer_dim / MAX_DATA_SIZE; // in segments, same as in the acks
    syn_ack.seq_num = 0;
    memcpy(syn_ack_buffer, &syn_ack, sizeof(struct poli_tcp_ctrl_hdr));

    // store in the buffer the new port used for connection
    memcpy(syn_ack_buffer + sizeof(struct poli_tcp_ctrl_hdr), &new_connection_port, sizeof(uint16_t));

    rc = sendto(listenfd, syn_ack_buffer, syn_ack_buffer_len, 0, (struct sockaddr*)&client_addr, client_addr_len);
    if (rc < 0) {
        perror("recv failed");
        exit(EXIT_FAILURE);
    }

    // 3. receive ack from client
    char ack_buffer[MAX_SEGMENT_SIZE];
    while(1) {
        rc = recvfrom(con->sockfd, ack_buffer, MAX_SEGMENT_SIZE, 0, (struct sockaddr*)&client_addr, &client_addr_len);
        if (rc < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // timeout => resend syn ack
                rc = sendto(listenfd, syn_ack_buffer, syn_ack_buffer_len, 0, (struct sockaddr*)&client_addr, client_addr_len);
            } else {
                // recvfrom error
                perror("recv failed");
                exit(EXIT_FAILURE);
            }
        } else {
            // take header from received buf
            struct poli_tcp_ctrl_hdr ack = *(struct poli_tcp_ctrl_hdr*)ack_buffer;

            // a data segment also confirms the connection: the client only sends
            // data after it got the syn ack (the final ack may have been lost)
            if (ack.type != ACK_TYPE && ack.type != DATA_TYPE) {
                rc = sendto(con->sockfd, syn_ack_buffer, syn_ack_buffer_len, 0, (struct sockaddr*)&client_addr, client_addr_len);
            } else {
                break;
            }
        }
    }

    // we can have multiple connections, so we use poll to know if data is available on this socket
    data_fds[fdmax].fd = con->sockfd;
    data_fds[fdmax].events = POLLIN;

    // timer for this connection, triggers every 1 sec (used for periodic window updates)
    timer_fds[fdmax].fd = timerfd_create(CLOCK_REALTIME,  0);
    timer_fds[fdmax].events = POLLIN;
    struct itimerspec spec;
    spec.it_value.tv_sec = 1;
    spec.it_value.tv_nsec = 0;
    spec.it_interval.tv_sec = 1;
    spec.it_interval.tv_nsec = 0;
    timerfd_settime(timer_fds[fdmax].fd, 0, &spec, NULL);
    fdmax++;

    // mutex initializer
    pthread_mutex_init(&con->con_lock, NULL);
    cons.insert({con->conn_id, con});

    DEBUG_PRINT("Connection established!");

    return con->conn_id;
}

void init_receiver(int recv_buffer_bytes)
{
    pthread_t thread1;
    int ret;

    // create the listening socket and bind it to port 8032
    listenfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (listenfd < 0) {
        perror("socket failed");
        exit(EXIT_FAILURE);
    }

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(struct sockaddr_in));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(8032);
    serv_addr.sin_addr.s_addr = INADDR_ANY;

    int rc;
    rc = bind(listenfd, (struct sockaddr*)&serv_addr, sizeof(struct sockaddr_in));
    if (rc < 0) {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }

    // save the receive buffer size
    recv_buffer_dim = recv_buffer_bytes;

    // create the receiver thread
    ret = pthread_create( &thread1, NULL, receiver_handler, NULL);
    assert(ret == 0);
}