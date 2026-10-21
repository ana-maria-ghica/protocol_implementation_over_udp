#pragma once

#include <cstdint>
#include "utils.h"
#include <arpa/inet.h>

/* Maximum segment size, change as you see fit */
#define MAX_DATA_SIZE 512
#define MAX_SEGMENT_SIZE (MAX_DATA_SIZE + sizeof(poli_tcp_data_hdr))

#define MAX_CONNECTIONS 32

/* Protocol control block. Used track different parameters about a connection. 
 * Will need to be extenden to solve the homework with other parameters such as
 * last_ack or status depending on how you implement your protocol. */
struct connection {
    /* common window for both the sender and receiver. */
    /* list window: A window representation */
    int sockfd; /* socket used for this connection */
    int conn_id; /* connection identifier */
    struct sockaddr_in servaddr; /* used to identify the destination */
    pthread_mutex_t con_lock; /* Used for syncronization with the handler thread and read/send calls.*/

    
    // parametrii folositi de sender - libsend.cpp
    int max_window_seq; /* max number of packets that can be inflight, since we can have many more packets in our window */
    int next_seq; // urmatorul seq_num de atribuit
    int base; // primul seq_num neconfirmat
    int recv_window; // ultimul recv_window primit de la server
    int dup_acks; // ACK-uri duplicate consecutive (pentru fast retransmit)
    struct list* window;

    
    // parametrii folositi de receiver - librecv.cpp    
    int expected_seq; // urmatorul seq_num asteptat in ordine
    int recv_buffer_dim; // spatiul total al buffer-ului
    struct list* recv_list;
};

/* ########## API that we expose to the application ########### */

/* Equivalent of listen. Ran by the server to waits for a connection from a
 * client. Returns a connection id. Blocking untill it receives a connection
 * request */
int wait4connect(uint32_t ip, uint16_t port);

/* Equivalent of connect. Used by the client to connect to a server. */
int setup_connection(uint32_t ip, uint16_t port);

/* Equivalent to recv. Blocking if there is no data to be written in buffer */
int recv_data(int connectionid, char *buffer, int len);

/* Equivalent to send. Used by the client to send a stream of bytes as segments */
int send_data(int conn_id, char *buffer, int len);

/* Used to initialize your protocol on the receiver side. */
void init_receiver(int recv_buffer_bytes);

/* Used to initialize your protocol on the sender side */
void init_sender(int speed, int delay);

/* ######### Internal API used by sender and receiver ########### */
int recv_message_or_timeout(char *buff, size_t len, int *conn_id);