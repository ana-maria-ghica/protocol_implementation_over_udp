#include <pthread.h>
#include <cstdlib>
#include <map>
#include <cstdint>
#include "lib.h"
#include "utils.h"
#include "protocol.h"
#include <cassert>
#include <poll.h>
#include <sys/timerfd.h>
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <cerrno>
#include <algorithm>
#include <unistd.h>

using namespace std;

std::map<int, struct connection *> cons;

#define MAX_CHUNK_SIZE 1024
#define SEND_BUFFER_MAX 1024

// number of duplicate acks that triggers a fast retransmit
#define DUP_ACK_THRESHOLD 3

struct pollfd data_fds[MAX_CONNECTIONS];
// used for timers per connection
struct pollfd timer_fds[MAX_CONNECTIONS];
int fdmax = 0;

// global vars
int bdp, conn_cnt;
int timeout_ms; // retransmission interval, computed from the link delay

struct list_node {
    char payload[MAX_DATA_SIZE];
    int payload_len;
    int seq_num;
    bool sent;
    bool acked;
    list_node* next;
};

struct list {
    int size;
    int capacity;
    list_node* head;
    list_node* next_to_send;
};

struct list* create_window() {
    struct list* window = (struct list*)calloc(1, sizeof(struct list));
    window->head = NULL;
    window->next_to_send = NULL;
    window->size = 0;
    window->capacity = 0;

    return window;
}

void add_element(struct list* window, char *buffer, int len) {
    // sanity check
    if (window == NULL) {
        return;
    }

    struct list_node* head = window->head;
    if (head == NULL) {
        // empty list => add the first elem
        head = (struct list_node*)calloc(1, sizeof(struct list_node));

        memcpy(head->payload, buffer, len);
        head->payload_len = len;
        head->seq_num = 0; // the real value is set when the segment is sent
        head->sent = false;
        head->acked = false;
        head->next = NULL;

        // update window
        window->head = head;
        window->next_to_send = head;
        window->size++;
        return;

    } else {
        // if any element in list => iterate through the list until the final pos
        struct list_node* iter = head;
        struct list_node* prev = iter;
        while (iter) {
            prev = iter;
            iter = iter->next;
        }

        // add in the prev "position"
        struct list_node* new_node = (struct list_node*)calloc(1, sizeof(struct list_node));
        memcpy(new_node->payload, buffer, len);
        new_node->payload_len = len;
        new_node->seq_num = 0; // the real value is set when the segment is sent
        new_node->sent = false;
        new_node->acked = false;
        new_node->next = NULL;

        // connect the node
        prev->next = new_node;

        // if every node was already sent, this one is the next to send
        if (window->next_to_send == NULL)
            window->next_to_send = new_node;

        window->size++;
        return;
    }
}

int get_in_flight_nodes(struct list* window, struct list_node* head, struct list_node* next_to_send) {
    struct list_node* iter = head;
    int count = 0;
    while (iter != next_to_send) {
        count++;
        iter = iter->next;
    }
    return count;
}

// build a data segment (header + payload) for a window node and send it
static void send_segment(struct connection *con, struct list_node *node)
{
    char buffer[sizeof(struct poli_tcp_data_hdr) + MAX_DATA_SIZE];

    // set the header
    struct poli_tcp_data_hdr data_header;
    memset(&data_header, 0, sizeof(struct poli_tcp_data_hdr));
    data_header.protocol_id = POLI_PROTOCOL_ID;
    data_header.conn_id = con->conn_id;
    data_header.type = DATA_TYPE;
    data_header.seq_num = node->seq_num;
    data_header.len = node->payload_len;

    memcpy(buffer, &data_header, sizeof(struct poli_tcp_data_hdr));

    // set the payload after the header
    memcpy(buffer + sizeof(struct poli_tcp_data_hdr), node->payload, node->payload_len);

    // send the segment
    int buffer_len = sizeof(struct poli_tcp_data_hdr) + node->payload_len;
    int rc = sendto(con->sockfd, buffer, buffer_len, 0,
                    (struct sockaddr*)&con->servaddr, sizeof(struct sockaddr_in));
    if (rc < 0) {
        perror("sendto failed");
        exit(EXIT_FAILURE);
    }
}

// send new segments while the number in flight stays below min(recv_window, bdp window)
// must be called with con_lock held
static void send_new_segments(struct connection *con)
{
    list* window = con->window;
    int in_flight = get_in_flight_nodes(window, window->head, window->next_to_send);
    int limit = std::min(con->recv_window, con->max_window_seq);

    struct list_node* node = window->next_to_send;
    while (in_flight < limit && node != NULL) {
        // the seq_num is assigned only now, when the segment actually leaves
        node->seq_num = con->next_seq;
        con->next_seq++;
        node->sent = true;

        send_segment(con, node);

        in_flight++;
        node = node->next;
    }
    window->next_to_send = node;
}

int send_data(int conn_id, char *buffer, int len)
{
    // lock, so we don't have sync problems with sender_handler
    pthread_mutex_lock(&cons[conn_id]->con_lock);

    // take the connection from map
    struct connection *current_connection = cons[conn_id];
    list* current_list = current_connection->window;

    // split the data into segments of at most MAX_DATA_SIZE bytes
    int chunks = (len + MAX_DATA_SIZE - 1) / MAX_DATA_SIZE;

    // check if space is available
    if (current_list->size + chunks > SEND_BUFFER_MAX) {
        pthread_mutex_unlock(&cons[conn_id]->con_lock);
        // buffer full => let the sender thread take the lock and free some space
        usleep(1000);
        return -1;
    }

    for (int offset = 0; offset < len; offset += MAX_DATA_SIZE) {
        int chunk = std::min(MAX_DATA_SIZE, len - offset);
        add_element(current_list, buffer + offset, chunk);
    }

    // send right away as much as the window allows
    send_new_segments(current_connection);

    pthread_mutex_unlock(&cons[conn_id]->con_lock);

    // return the number of bytes taken from the buffer
    return len;
}

void *sender_handler(void *arg)
{
    int res = 0;
    char buf[MAX_SEGMENT_SIZE];

    while (1) {
        if (cons.size() == 0) {
            continue;
        }
        int conn_id = -1;
        do {
            res = recv_message_or_timeout(buf, MAX_SEGMENT_SIZE, &conn_id);
        } while(res == -14);

        // handle the segment received from the receiver between locks,
        // so we don't have sync problems with send_data (main thread)
        pthread_mutex_lock(&cons[conn_id]->con_lock);

        // current connection
        struct connection *current_connection = cons[conn_id];
        list* window = current_connection->window;

        if (res == -1) {
            // timeout => resend only the segments that were sent but not acked
            struct list_node* node = window->head;
            while (node != NULL && node->sent) {
                if (!node->acked)
                    send_segment(current_connection, node);
                node = node->next;
            }
        } else if (res >= (int)sizeof(struct poli_tcp_ctrl_hdr)) {
            // header of the received segment
            struct poli_tcp_ctrl_hdr *ack_received = (struct poli_tcp_ctrl_hdr*)buf;

            // process it only if it is an ack of our protocol, otherwise drop it
            if (ack_received->protocol_id == POLI_PROTOCOL_ID &&
                ack_received->type == ACK_TYPE) {
                int ack_num = ack_received->ack_num;
                current_connection->recv_window = ack_received->recv_window;

                // fast retransmit: the ack does not advance, so the receiver is getting
                // segments past a gap => resend the missing one without waiting for the timeout
                if (ack_num == current_connection->base && window->head != NULL &&
                    window->head->sent && !window->head->acked) {
                    current_connection->dup_acks++;
                    if (current_connection->dup_acks == DUP_ACK_THRESHOLD)
                        send_segment(current_connection, window->head);
                } else if (ack_num > current_connection->base) {
                    current_connection->dup_acks = 0;
                }

                // cumulative ack: mark every sent segment with seq_num < ack_num
                struct list_node* node = window->head;
                while (node != NULL && node->sent && node->seq_num < ack_num) {
                    node->acked = true;
                    node = node->next;
                }

                // delete from the head of the list only consecutive nodes with acked == true
                node = window->head;
                int deleted_nodes = 0;
                while (node != NULL && node->acked) {
                    struct list_node *prev = node;
                    node = node->next;
                    free(prev);
                    deleted_nodes++;
                }

                // update window after delete
                window->head = node;
                window->size -= deleted_nodes;
                current_connection->base = node != NULL ? node->seq_num
                                                        : current_connection->next_seq;
            }
        }

        // after an ack (space freed in the window) or a timeout, send whatever is allowed
        send_new_segments(current_connection);

        pthread_mutex_unlock(&cons[conn_id]->con_lock);
    }
}

int setup_connection(uint32_t ip, uint16_t port)
{
    // sender side of the three way handshake, blocks until the connection is established

    // create the connection
    struct connection *con = (struct connection *)malloc(sizeof(struct connection));

    // struct connection params
    con->sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    con->conn_id = conn_cnt;
    conn_cnt++;
    con->max_window_seq = bdp;
    con->next_seq = 0;
    con->base = 0;
    con->recv_window = 0;
    con->dup_acks = 0;

    // server address
    struct sockaddr_in servaddr;
    memset(&servaddr, 0, sizeof(struct sockaddr_in));
    servaddr.sin_family = AF_INET;
    servaddr.sin_port = port;
    servaddr.sin_addr.s_addr = ip;
    con->servaddr = servaddr;
    con->window = create_window();

    // receive timeout on the socket, used during the handshake
    struct timeval tv;
    tv.tv_sec = 2;
    tv.tv_usec = 100000;
    if (setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO,&tv,sizeof(tv)) < 0) {
        perror("Error");
    }

    // three way handshake
    // 1. send syn to server
    struct poli_tcp_ctrl_hdr syn;
    memset(&syn, 0, sizeof(struct poli_tcp_ctrl_hdr));
    syn.protocol_id = POLI_PROTOCOL_ID;
    syn.conn_id = con->conn_id;
    syn.type = SYN_TYPE;
    syn.ack_num = 0;
    syn.seq_num = 0;

    int rc;
    rc = sendto(con->sockfd, &syn, sizeof(struct poli_tcp_ctrl_hdr), 0, (struct sockaddr *)&con->servaddr, sizeof(struct sockaddr_in));
    if (rc < 0) {
        perror("sendto failed");
        exit(EXIT_FAILURE);
    }

    // 2. wait for syn ack from server
    int buffer_size = sizeof(struct poli_tcp_ctrl_hdr) + sizeof(uint16_t);
    char *buffer = (char*)calloc(buffer_size, sizeof(char)); // ctrl header + the new port
    socklen_t server_address_len = sizeof(struct sockaddr_in);
    while(1) {
        // receive the message from server
        rc = recvfrom(con->sockfd, buffer, buffer_size, 0, (struct sockaddr*)&con->servaddr, &server_address_len);

        if (rc < 0) {
            // if timeout => resend syn
            if (errno == EWOULDBLOCK || errno == EAGAIN) {
                rc = sendto(con->sockfd, &syn, sizeof(struct poli_tcp_ctrl_hdr), 0, (struct sockaddr *)&con->servaddr, sizeof(struct sockaddr_in));
                if (rc < 0) {
                    perror("sendto failed");
                    exit(EXIT_FAILURE);
                }
                continue;
            } else {
                // message error
                perror("recvfrom failed");
                exit(EXIT_FAILURE);
            }
        }

        struct poli_tcp_ctrl_hdr syn_ack = *(struct poli_tcp_ctrl_hdr*)buffer;
        // resend syn: if not "SYN_WITH_ACK_TYPE 101" or if wrong ack
        if (syn_ack.type != SYN_WITH_ACK_TYPE || syn_ack.ack_num != syn.seq_num + 1) {
            rc = sendto(con->sockfd, &syn, sizeof(struct poli_tcp_ctrl_hdr), 0, (struct sockaddr *)&con->servaddr, sizeof(struct sockaddr_in));
            if (rc < 0) {
                perror("sendto failed");
                exit(EXIT_FAILURE);
            }
        } else {
            // 3. send ack to server
            struct poli_tcp_ctrl_hdr ack;
            memset(&ack, 0, sizeof(struct poli_tcp_ctrl_hdr));
            ack.protocol_id = POLI_PROTOCOL_ID;
            ack.conn_id = con->conn_id;
            ack.type = ACK_TYPE;
            ack.ack_num = syn_ack.seq_num+ 1;
            ack.seq_num = syn_ack.seq_num; // no sent data, so it's the same

            // modify the connection port - with the received port from server
            uint16_t new_port = *(uint16_t*)(buffer + sizeof(struct poli_tcp_ctrl_hdr));
            con->servaddr.sin_port = new_port;
            con->recv_window = syn_ack.recv_window;
            rc = sendto(con->sockfd, &ack, sizeof(struct poli_tcp_ctrl_hdr), 0, (struct sockaddr*)&con->servaddr, sizeof(struct sockaddr_in));
            if (rc < 0) {
                perror("sendto failed");
                exit(EXIT_FAILURE);
            }
            break;
        }
    }
    free(buffer);

    // we can have multiple connections, so we use poll to know if data is available on this socket
    data_fds[fdmax].fd = con->sockfd;
    data_fds[fdmax].events = POLLIN;

    // retransmission timer for this connection, triggers every timeout_ms
    timer_fds[fdmax].fd = timerfd_create(CLOCK_REALTIME,  0);
    timer_fds[fdmax].events = POLLIN;
    struct itimerspec spec;
    spec.it_value.tv_sec = timeout_ms / 1000;
    spec.it_value.tv_nsec = (timeout_ms % 1000) * 1000000L;
    spec.it_interval = spec.it_value;
    timerfd_settime(timer_fds[fdmax].fd, 0, &spec, NULL);
    fdmax++;

    // mutex initializer
    pthread_mutex_init(&con->con_lock, NULL);

    cons.insert({con->conn_id, con});

    DEBUG_PRINT("Connection established!");
    return con->conn_id;
}

void init_sender(int speed, int delay)
{
    pthread_t thread1;
    int ret;

    // [speed] = Mb/s => conversion to bits/s
    double speed_bits = speed * 1000000.0;

    // rtt = 2 * delay, [delay] = ms => conversion to s
    double rtt = (delay * 2.0) / 1000.0;

    // bandwidth delay product, in bits
    double bdp_bits = speed_bits * rtt;

    // how many segments can be in flight
    int eth_hdr_len = 14;
    int ip_hdr_len = 20;
    int udp_hdr_len = 8;
    int frame_size = eth_hdr_len + ip_hdr_len + udp_hdr_len + MAX_SEGMENT_SIZE;

    int window_size = (int) (bdp_bits / (8.0 * frame_size));
    if (window_size < 1)
        window_size = 1;

    bdp = window_size;

    // retransmission timeout (ms), using the macro from utils.h
    timeout_ms = TIMEOUT_SEND(delay);

    // create the sender thread
    ret = pthread_create(&thread1, NULL, sender_handler, NULL);
    assert(ret == 0);
}