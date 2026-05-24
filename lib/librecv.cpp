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
/* Used for timers per connection */
struct pollfd timer_fds[MAX_CONNECTIONS];
int fdmax = 0;

// variabilele globale
int recv_buffer_dim, conn_cnt, listen_fd;

// nod in lista ----------------------------------------------
struct list_node {
    char payload[MAX_DATA_SIZE];
    int payload_len;
    int seq_num;
    list_node* next;
};

// lista -----------------------------------------------------
struct list {
    int size;
    int capacity;
    list_node* head;
};

// creez window -----------------------------------------------------
struct list* create_window() {
    struct list* window = (struct list*)calloc(1, sizeof(struct list));
    window->head = NULL;
    window->size = 0;
    window->capacity = 0;

    return window;
}

int recv_data(int conn_id, char *buffer, int len)
{
    int size = 0;

    pthread_mutex_lock(&cons[conn_id]->con_lock);
    
    /* We will write code here as to not have sync problems with recv_handler */

    // scop: conn_id->recv_list si de acolo ia din fiecare nod payload-ul si il copiaza in buffer; la final returneaza bytes copiati

    // iau conexiunea din map
    struct connection *current_connection = cons[conn_id];
    list* current_list = current_connection->recv_list;

    // functia trebuie sa fie blocanta - daca nu sunt date de citit inca
    while (current_list->head == NULL) {
        pthread_mutex_unlock(&cons[conn_id]->con_lock);
        usleep(1000);
        pthread_mutex_lock(&cons[conn_id]->con_lock);
    }

    struct list_node* head = current_list->head;
    while (head != NULL) {
        if (size + head->payload_len > len)
            break;
        memcpy(buffer + size, head->payload, head->payload_len);
        size += head->payload_len;
        struct list_node* to_free = head;
        head = head->next;

        // dealoc nodul
        free(to_free);
    }

    // actualizez head-ul
    current_list->head = head;

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

        pthread_mutex_lock(&cons[conn_id]->con_lock);

        /* Handle segment received from the sender. We use this between locks
        as to not have synchronization issues with the recv_data calls which are
        on the main thread */

        // primesc un segment -> verific  if (res != -1 && ((poli_tcp_data_hdr *) segment)->type == DATA) {



        pthread_mutex_unlock(&cons[conn_id]->con_lock);
    }

    
}

int wait4connect(uint32_t ip, uint16_t port)
{
    /* TODO: Implement the Three Way Handshake on the receiver part. This blocks
     * until a connection is established. */

    struct connection *con = (struct connection *)malloc(sizeof(struct connection));
    con->conn_id = conn_cnt;
    conn_cnt++;

    /* This can be used to set a timer on a socket, useful once we received a
     * SYN. You may want to disable by setting the time to 0 (tv_sec = 0,
     * tv_usec = 0)
    struct timeval tv;
    tv.tv_sec = 2;
    tv.tv_usec = 100000;
    if (setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO,&tv,sizeof(tv)) < 0) {
        perror("Error");
    } */

    /* Receive SYN on the connection socket. Create a new socket and bind it to
     * the chosen port. Send the data port number via SYN-ACK to the client */
    con->sockfd = socket(AF_INET, SOCK_DGRAM, 0);

    // adresa clientului care se va conecta la server
    struct sockaddr_in client_addr;
    memset(&client_addr, 0, sizeof(struct sockaddr_in));
    client_addr.sin_family = AF_INET;
    client_addr.sin_port = port;
    client_addr.sin_addr.s_addr = ip;

    // three way handshake -----------------------------------------------------------
    // 1. primesc SYN de la client -----------------------------------------------------------
    char syn_buffer[MAX_SEGMENT_SIZE];
    socklen_t client_addr_len = sizeof(struct sockaddr_in);
    struct poli_tcp_ctrl_hdr syn;
    int rc;
    while(1) {
        rc = recvfrom(listen_fd, syn_buffer, MAX_SEGMENT_SIZE, 0, (struct sockaddr*)&client_addr, &client_addr_len);
        if (rc < 0) {
            perror("recv failed");
            exit(EXIT_FAILURE);
        }

        // din bufferul primit preiau header-ul
        syn = *(struct poli_tcp_ctrl_hdr*)syn_buffer;
        if (syn.type != SYN_TYPE) {
            continue;
        } else {
            break;
        }
    }

    // conectez serverul la retea pe acest socket - si initial portul e 0
    struct sockaddr_in connection_addr;
    socklen_t connection_addr_len = sizeof(struct sockaddr_in);
    memset(&connection_addr, 0, sizeof(struct sockaddr_in));
    connection_addr.sin_family = AF_INET;
    connection_addr.sin_port = 0;  // ca sa creez socket nou pe un port aleator
    connection_addr.sin_addr.s_addr = ip;
    rc = bind(con->sockfd, (struct sockaddr*)&connection_addr, sizeof(struct sockaddr_in));
    if (rc < 0) {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }
    // vreau sa determin portul pe care s-a format conexiunea
    getsockname(con->sockfd, (struct sockaddr*)&connection_addr, &connection_addr_len);
    uint16_t new_connection_port = connection_addr.sin_port;

    
    // 2. construiesc SYN_ACK si trimit la client -----------------------------------------------------------
    int syn_ack_buffer_len = sizeof(struct poli_tcp_ctrl_hdr) + sizeof(uint16_t);
    char syn_ack_buffer[syn_ack_buffer_len];

    struct poli_tcp_ctrl_hdr syn_ack;
    memset(&syn_ack, 0, sizeof(struct poli_tcp_ctrl_hdr));
    syn_ack.protocol_id = POLI_PROTOCOL_ID;
    syn_ack.conn_id = con->conn_id;
    syn_ack.type = SYN_WITH_ACK_TYPE;
    syn_ack.ack_num = syn.seq_num + 1;
    syn_ack.recv_window = recv_buffer_dim;
    syn_ack.seq_num = 0;
    memcpy(syn_ack_buffer, &syn_ack, sizeof(struct poli_tcp_ctrl_hdr));

    // trebuie sa pun in buffer NOUL port pe care deschid conexiunea
    memcpy(syn_ack_buffer + sizeof(struct poli_tcp_ctrl_hdr), &new_connection_port, sizeof(uint16_t));

    rc = sendto(con->sockfd, syn_ack_buffer, syn_ack_buffer_len, 0, (struct sockaddr*)&client_addr, client_addr_len);
    if (rc < 0) {
        perror("recv failed");
        exit(EXIT_FAILURE);
    }

    // 3. primesc ACK de la client -----------------------------------------------------------
    char ack_buffer[MAX_SEGMENT_SIZE];
    while(1) {
        rc = recvfrom(con->sockfd, ack_buffer, MAX_SEGMENT_SIZE, 0, (struct sockaddr*)&client_addr, &client_addr_len);
        if (rc < 0) {
            // ori am timeout
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // la timeout - trmit iar syn_ack
                rc = sendto(con->sockfd, syn_ack_buffer, syn_ack_buffer_len, 0, (struct sockaddr*)&client_addr, client_addr_len);
            } else {
                // eroare la recvfrom
                perror("recv failed");
                exit(EXIT_FAILURE);
            }
        } else {            
            // din bufferul primit preiau header-ul
            struct poli_tcp_ctrl_hdr ack = *(struct poli_tcp_ctrl_hdr*)ack_buffer;
            if (ack.type != ACK_TYPE) {
                rc = sendto(con->sockfd, syn_ack_buffer, syn_ack_buffer_len, 0, (struct sockaddr*)&client_addr, client_addr_len);
            } else {
                break;
            }
        }
    }



    /* Since we can have multiple connection, we want to know if data is available
       on the socket used by a given connection. We use POLL for this */
    data_fds[fdmax].fd = con->sockfd;    
    data_fds[fdmax].events = POLLIN;    
    
    /* This creates a timer and sets it to trigger every 1 sec. We use this
       to know if a timeout has happend on a connection */
    timer_fds[fdmax].fd = timerfd_create(CLOCK_REALTIME,  0);    
    timer_fds[fdmax].events = POLLIN;    
    struct itimerspec spec;     
    spec.it_value.tv_sec = 1;    
    spec.it_value.tv_nsec = 0;    
    spec.it_interval.tv_sec = 1;    
    spec.it_interval.tv_nsec = 0;    
    timerfd_settime(timer_fds[fdmax].fd, 0, &spec, NULL);    
    fdmax++;    

    pthread_mutex_init(&con->con_lock, NULL);
    cons.insert({con->conn_id, con});

    DEBUG_PRINT("Connection established!");

    return 0;
}

void init_receiver(int recv_buffer_bytes)
{
    pthread_t thread1;
    int ret;

    /* TODO: Create the connection socket and bind it to 8031 */
    int listenfd = socket(AF_INET, SOCK_DGRAM, 0);
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

    // setez variabila recv_buffer_bytes
    recv_buffer_dim = recv_buffer_bytes;

    ret = pthread_create( &thread1, NULL, receiver_handler, NULL);
    assert(ret == 0);
}
