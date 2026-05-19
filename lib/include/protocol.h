#include <cstdint>

/* Values used for protocol ID and type. */
#define POLI_PROTOCOL_ID 42

// macro-urile definite de mine
#define SYN_TYPE 100
#define SYN_WITH_ACK_TYPE 101
#define ACK_TYPE 102
#define DATA_TYPE 103

/* Header for Data segments. Must be used in your implementation as it is. */
struct __attribute__((packed)) poli_tcp_data_hdr {
    uint8_t protocol_id;
    uint8_t conn_id;
    uint8_t type;
    uint16_t seq_num;
    uint16_t len;
};

/* Header for Control segments. Must be used in your implementation as it is. */
struct __attribute__((packed)) poli_tcp_ctrl_hdr {
    uint8_t protocol_id;
    uint8_t conn_id;
    uint8_t type;
    uint16_t ack_num;
    uint16_t recv_window;

    // adaugat de mine - pentru three-way-handshake
    uint16_t seq_num;
};
