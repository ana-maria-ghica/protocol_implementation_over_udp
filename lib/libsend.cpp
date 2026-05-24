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

using namespace std;

std::map<int, struct connection *> cons;

#define MAX_CHUNK_SIZE 1024
#define SEND_BUFFER_MAX 1024

struct pollfd data_fds[MAX_CONNECTIONS];
/* Used for timers per connection */
struct pollfd timer_fds[MAX_CONNECTIONS];
int fdmax = 0;

// variabile globale
int bdp, conn_cnt;

// nod in lista ----------------------------------------------
struct list_node {
    char payload[MAX_DATA_SIZE];
    int payload_len;
    int seq_num;
    bool sent;
    bool acked;
    list_node* next;
};

// lista -----------------------------------------------------
struct list {
    int size;
    int capacity;
    list_node* head;
    list_node* next_to_send;
};

// creez window -----------------------------------------------------
struct list* create_window() {
    struct list* window = (struct list*)calloc(1, sizeof(struct list));
    window->head = NULL;
    window->next_to_send = NULL;
    window->size = 0;
    window->capacity = 0;

    return window;
}

// adaug element in lista -----------------------------------------------------
void add_element(struct list* window, char *buffer, int len) {
    // sanity check
    if (window == NULL) {
        return;
    }

    struct list_node* head = window->head;
    if (head == NULL) {
        // asta e inceputul listei => eu adaug primul element
        head = (struct list_node*)calloc(1, sizeof(struct list_node));

        memcpy(head->payload, buffer, len);
        head->payload_len = len;
        head->seq_num = 0; // setez valoarea lui in sender_handler() cand trimit pachetul
        head->sent = false;
        head->acked = false;
        head->next = NULL;

        // actualizez window
        window->head = head;
        window->next_to_send = head;
        window->size++;
        return;

    } else {
        // mai am elemente in lista => parcurg pana ajung pe pozitia finala
        struct list_node* iter = head;
        struct list_node* prev = iter;
        while (iter) {
            prev = iter;
            iter = iter->next;
        }

        // eu adaug pe "pozitia" prev
        struct list_node* new_node = (struct list_node*)calloc(1, sizeof(struct list_node));
        memcpy(new_node->payload, buffer, len);
        new_node->payload_len = len;
        new_node->seq_num = 0; // setez valoarea lui in sender_handler() cand trimit pachetul
        new_node->sent = false;
        new_node->acked = false;
        new_node->next = NULL;

        // leg nodul anterior de el
        prev->next = new_node;

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

int send_data(int conn_id, char *buffer, int len)
{
    // returnez - cati bytes am copiat din buffer in nod
    int size = 0;

    /* We will write code here as to not have sync problems with sender_handler */
    pthread_mutex_lock(&cons[conn_id]->con_lock);   

    // iau conexiunea din map
    struct connection *current_connection = cons[conn_id];
    list* current_list = current_connection->window;
    
    // verific intai daca mai am loc sa pun in lista
    if (current_list->size >= SEND_BUFFER_MAX) {
        pthread_mutex_unlock(&cons[conn_id]->con_lock);
        return -1;
    }

    char buf[MAX_DATA_SIZE];
    memcpy(buf, buffer, len);
    add_element(current_list, buf, len);

    pthread_mutex_unlock(&cons[conn_id]->con_lock);

    size = len;
    return size;
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

        pthread_mutex_lock(&cons[conn_id]->con_lock);

        /* Handle segment received from the receiver. We use this between locks
        as to not have synchronization issues with the send_data calls which are
        on the main thread */

        // conexiunea curenta
        struct connection *current_connection = cons[conn_id];
        list* window = current_connection->window;

        if (res == -1) {
            // timeout => retrimit doar segmentele trimise dar neconfirmate
            struct list_node* head = window->head;
            while (head != NULL) {
                if (head->sent && !head->acked) {
                    // eu trebuie sa merge-uiesc head-ul si header-ul de poli_tcp_data_hdr
                    char buffer[sizeof(struct poli_tcp_data_hdr) + MAX_DATA_SIZE];

                    // setez in buffer partea de header
                    struct poli_tcp_data_hdr data_header;
                    memset(&data_header, 0, sizeof(struct poli_tcp_data_hdr));
                    data_header.protocol_id = POLI_PROTOCOL_ID;
                    data_header.conn_id = conn_id;
                    data_header.type = DATA_TYPE;
                    data_header.seq_num = head->seq_num;
                    data_header.len = head->payload_len;

                    memcpy(buffer, &data_header, sizeof(struct poli_tcp_data_hdr));

                    // setez in buffer (dupa header) partea de payload
                    memcpy(buffer + sizeof(struct poli_tcp_data_hdr), head->payload, head->payload_len);

                    // am construit segmentul complet (header + payload)
                    // trimit segmentul

                    int buffer_len = sizeof(struct poli_tcp_data_hdr) + head->payload_len;
                    int rc = sendto(current_connection->sockfd, buffer, buffer_len, 0, (struct sockaddr*)&current_connection->servaddr, sizeof(struct sockaddr_in));
                    if (rc < 0) {
                        perror("sendto failed");
                        exit(EXIT_FAILURE);
                    }
                }
                head = head->next;
            } 
        } else {
            // header-ul segmentului primit
            struct poli_tcp_ctrl_hdr *ack_received = (struct poli_tcp_ctrl_hdr*)buf;

            // verific ca e de tip ack, altfel arunc segmentul
            if (ack_received->type != ACK_TYPE) {
                continue;
            }

            int ack_num = ack_received->ack_num;
            current_connection->recv_window = ack_received->recv_window;

            // marchez nodurile confirmate
            struct list_node* head = window->head;
            while (head != NULL && head->seq_num < ack_num) {
                head->acked = true;
                head = head->next;
            }

            // sterg din capul listei doar nodurile consecutive cu acked == true
            head = window->head;
            int deleted_nodes = 0;
            while (head != NULL && head->acked) {
                struct list_node *prev = head;
                head = head->next;
                free(prev);
                deleted_nodes++;
            }
            // actualizez window dupa stergere
            window->head = head;
            window->size -= deleted_nodes;
            if (head != NULL) {
                current_connection->base = head->seq_num;
            } else {
                current_connection->base = current_connection->next_seq;
            }

            // verific ca next to send sa pointeze corect (in urma stergerii nodurilor)
            if (window->next_to_send == NULL && window->head != NULL) {
                window->next_to_send = window->head;
            }


            // trimit pachetele (max_window_size sau cate exista in lista, daca sunt mai putine)
            int current_size = get_in_flight_nodes(window, window->head, window->next_to_send);
            head = window->next_to_send;

            // trebuie sa trimit minimul dintre (recv_window si max_window)
            int min_rwnd_cwnd = std::min(current_connection->recv_window, current_connection->max_window_seq);
            while (current_size < min_rwnd_cwnd && head != NULL) {
                // eu trebuie sa merge-uiesc head-ul si header-ul de poli_tcp_data_hdr
                char buffer[sizeof(struct poli_tcp_data_hdr) + MAX_DATA_SIZE];

                // setez in buffer partea de header
                struct poli_tcp_data_hdr data_header;
                memset(&data_header, 0, sizeof(struct poli_tcp_data_hdr));
                data_header.protocol_id = POLI_PROTOCOL_ID;
                data_header.conn_id = conn_id;
                data_header.type = DATA_TYPE;

                head->seq_num = current_connection->next_seq;
                current_connection->next_seq++;
                head->sent = true;
                data_header.seq_num = head->seq_num;

                data_header.len = head->payload_len;

                memcpy(buffer, &data_header, sizeof(struct poli_tcp_data_hdr));

                // setez in buffer (dupa header) partea de payload
                memcpy(buffer + sizeof(struct poli_tcp_data_hdr), head->payload, head->payload_len);

                // am construit segmentul complet (header + payload)
                // trimit segmentul
                int buffer_len = sizeof(struct poli_tcp_data_hdr) + head->payload_len;
                int rc = sendto(current_connection->sockfd, buffer, buffer_len, 0, (struct sockaddr*)&current_connection->servaddr, sizeof(struct sockaddr_in));
                if (rc < 0) {
                    // eroare de trimitere
                    perror("sendto failed");
                    exit(EXIT_FAILURE);
                }
                current_size++;
                head = head->next;
            } 
            window->next_to_send = head;
        }

        pthread_mutex_unlock(&cons[conn_id]->con_lock);
    }
}

int setup_connection(uint32_t ip, uint16_t port)
{
    /* Implement the sender part of the Three Way Handshake. Blocks
    until the connection is established */

    // creez conexiunea ------------------------------------------------------------------------------
    struct connection *con = (struct connection *)malloc(sizeof(struct connection));

    // parametrii din "struct connection"
    con->sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    con->conn_id = conn_cnt;
    conn_cnt++;
    con->max_window_seq = bdp;
    con->next_seq = 0;
    con->base = 0;
    con->recv_window = 0;
    

    // adresa serverului
    struct sockaddr_in servaddr;
    memset(&servaddr, 0, sizeof(struct sockaddr_in));
    servaddr.sin_family = AF_INET;
    servaddr.sin_port = port;
    servaddr.sin_addr.s_addr = ip;
    con->servaddr = servaddr;
    con->window = create_window();  // da free la window cand inchizi conexiunea !!!!!


    // This can be used to set a timer on a socket -----------------------------------------------------
    struct timeval tv;
    tv.tv_sec = 2;
    tv.tv_usec = 100000;
    if (setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO,&tv,sizeof(tv)) < 0) {
        perror("Error");
    }

    // creez structurile pentru a implementa "three way handshake" ------------------------------------------
    // 1. Trimit SYN la server
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

    // 2. Astept SYN ACK de la server
    int buffer_size = sizeof(struct poli_tcp_ctrl_hdr) + sizeof(uint16_t);
    char *buffer = (char*)calloc(buffer_size, sizeof(char)); // buffer contine header-ul ctrl + noul port
    socklen_t server_address_len = sizeof(struct sockaddr_in);
    while(1) {
        // primesc mesajul de la server
        rc = recvfrom(con->sockfd, buffer, buffer_size, 0, (struct sockaddr*)&con->servaddr, &server_address_len);

        if (rc < 0) {
            // daca primesc timeout
            if (errno == EWOULDBLOCK || errno == EAGAIN) {
                rc = sendto(con->sockfd, &syn, sizeof(struct poli_tcp_ctrl_hdr), 0, (struct sockaddr *)&con->servaddr, sizeof(struct sockaddr_in));
                if (rc < 0) {
                    perror("sendto failed");
                    exit(EXIT_FAILURE);
                }
                continue;
            } else {
                // daca nu s-a primit mesajul [eroare]
                perror("recvfrom failed");
                exit(EXIT_FAILURE);
            }
        }

        struct poli_tcp_ctrl_hdr syn_ack = *(struct poli_tcp_ctrl_hdr*)buffer;
        // daca segmentul primit nu are type-ul "SYN_WITH_ACK_TYPE 101" sau daca ack-ul este gresit, retrimit SYN
        if (syn_ack.type != SYN_WITH_ACK_TYPE || syn_ack.ack_num != syn.seq_num + 1) {
            rc = sendto(con->sockfd, &syn, sizeof(struct poli_tcp_ctrl_hdr), 0, (struct sockaddr *)&con->servaddr, sizeof(struct sockaddr_in));
            if (rc < 0) {
                perror("sendto failed");
                exit(EXIT_FAILURE);
            }
        } else {
            // 3. Trimit ACK catre server
            struct poli_tcp_ctrl_hdr ack;
            memset(&ack, 0, sizeof(struct poli_tcp_ctrl_hdr));
            ack.protocol_id = POLI_PROTOCOL_ID;
            ack.conn_id = con->conn_id;
            ack.type = ACK_TYPE;
            ack.ack_num = syn_ack.seq_num+ 1;
            ack.seq_num = syn_ack.seq_num; // nu am mai trimis date deci ramane la fel

            // modific portul conexiunii cu ceea ce primesc de la server
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

    /* Since we can have multiple connection, we want to know if data is available on the socket used by a given connection. We use POLL for this */
    data_fds[fdmax].fd = con->sockfd;    
    data_fds[fdmax].events = POLLIN;    
    
    // This creates a timer and sets it to trigger every 1 sec. We use this to know if a timeout has happend on our connection
    timer_fds[fdmax].fd = timerfd_create(CLOCK_REALTIME,  0);    
    timer_fds[fdmax].events = POLLIN;    
    struct itimerspec spec;     
    spec.it_value.tv_sec = 1;    
    spec.it_value.tv_nsec = 0;    
    spec.it_interval.tv_sec = 1;    
    spec.it_interval.tv_nsec = 0;    
    timerfd_settime(timer_fds[fdmax].fd, 0, &spec, NULL);    
    fdmax++;


    // initializez mutex-ul (ca sa nu se lucreze in paralel pe aceeasi conexiune) ------------------------
    pthread_mutex_init(&con->con_lock, NULL);

    // obs: vezi ca am schimbat din conn_id in conn_cnt (aka con->conn_id)
    cons.insert({con->conn_id, con});

    DEBUG_PRINT("Connection established!");
    return con->conn_id;
}

void init_sender(int speed, int delay)
{
    pthread_t thread1;
    int ret;

    // [speed in biti] = Mb/s => fac conversie
    double speed_bits = speed * 1000000.0;

    // [rtt] = 2 * [delay] => ms => fac conversie
    double rtt = (delay * 2.0) / 1000.0;

    // calculez bandwidth (in biti)
    double bdp_bits = speed_bits * rtt;

    // cate segmente pot fi inflight
    int eth_hdr_len = 14;
    int ip_hdr_len = 20;
    int udp_hdr_len = 8;
    int frame_size = eth_hdr_len + ip_hdr_len + udp_hdr_len + MAX_SEGMENT_SIZE;

    int window_size = (int) (bdp_bits / (8.0 * frame_size));
    if (window_size < 1)
        window_size = 1;
    
    bdp = window_size;

    /* Create a thread that will*/
    ret = pthread_create(&thread1, NULL, sender_handler, NULL);
    assert(ret == 0);
}
