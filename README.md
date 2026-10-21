# Reliable Transport Protocol over UDP

A connection-oriented, reliable transport protocol implemented in C/C++ on top of UDP sockets. UDP delivers datagrams with no guarantees: they may be lost, duplicated or reordered, and the sender has no feedback on whether anything arrived. This project builds, in userspace, the mechanisms TCP uses to turn such a channel into an ordered, lossless byte stream: connection establishment, sequence numbers, acknowledgements, retransmission, a sliding window and flow control.

The protocol is exercised by a file transfer application: one or more clients send files to a server over an emulated link with limited bandwidth, propagation delay and packet loss, and the server must reconstruct every file byte for byte.

It was developed as an assignment for the Network Protocols course at POLITEHNICA Bucharest (2026). The application-facing API, the packet header layout and the test topology were provided by the course; the protocol logic behind them is the subject of the implementation.

## Protocol overview

### Segment format

Two kinds of segments travel over the wire, both identified by a common protocol ID and a connection ID:

```
data segment     | protocol_id | conn_id | type | seq_num | len | payload (≤ 512 B) |
control segment  | protocol_id | conn_id | type | ack_num | recv_window | seq_num |
```

The `type` field distinguishes `SYN`, `SYN-ACK`, `ACK` and `DATA`. Sequence numbers count segments rather than bytes, and the receive window is likewise expressed in segments.

### Connection establishment

Connections are opened with a three-way handshake modelled on TCP's:

1. the client sends a `SYN` to the server's well-known port;
2. the server creates a dedicated socket for the connection, lets the kernel bind it to a free port, and answers with a `SYN-ACK` that carries both the acknowledgement and the number of that new port, together with its initial receive window;
3. the client switches to the new port and confirms with an `ACK`.

Giving every connection its own socket is what allows a single server to serve several clients at once, in the same way a TCP server obtains a new socket from `accept()` while the listening one stays open. Each step is protected by a receive timeout: a lost `SYN` or `SYN-ACK` is retransmitted, and if the final `ACK` is lost, the arrival of the first data segment is accepted as proof that the client has completed the handshake.

### Sender: sliding window

Outgoing data is split into segments of at most 512 bytes and queued in a send buffer. A segment receives its sequence number only when it actually leaves, and the sender keeps transmitting while the number of unacknowledged segments stays below

```
min(receiver window, BDP window)
```

The BDP window is derived from the bandwidth-delay product of the link: at 8 Mb/s with a 2 ms one-way delay, the pipe holds about 32 kbit, or roughly 7 full-size frames once Ethernet, IP and UDP headers are accounted for. Keeping that many segments in flight is what allows the sender to use the link fully instead of stopping to wait after every packet.

### Reliability

The receiver answers every data segment with a **cumulative acknowledgement**: `ack_num` is the next sequence number it expects in order, so a single ACK confirms everything before it, and a lost ACK is covered by the next one. Acknowledged segments are released from the head of the send buffer.

Lost segments are recovered by two complementary mechanisms:

- **Retransmission timeout.** A per-connection timer, implemented with `timerfd` and set to roughly half the delay plus a 30 ms margin, triggers the retransmission of every segment that has been sent but not yet acknowledged.
- **Fast retransmit.** When a segment is lost but later ones arrive, the receiver keeps acknowledging the same sequence number. After three duplicate ACKs, the sender retransmits the missing segment immediately, without waiting for the timer. The idea is the same as in TCP: duplicate ACKs are evidence of a hole, not of congestion, so there is no reason to wait.

### Receiver: ordering and flow control

Segments that arrive in order are appended to a delivery queue read by the application. Segments that arrive ahead of a gap are not discarded: they are held in a per-connection out-of-order buffer, and as soon as the missing segment arrives, it is delivered together with every buffered segment that now follows it without a gap. Duplicates and segments outside the receive window are dropped but still acknowledged, since the earlier ACK may itself have been lost.

The receiver has a fixed buffer (9 KB in the test configuration, i.e. 18 segments). Every ACK advertises how much of it is still free, and the sender never exceeds that figure, so a slow reader cannot be overrun by a fast sender. When the buffer fills completely, the advertised window drops to zero and the sender stops. To avoid the classic deadlock in which both sides wait for each other, the receiver sends a **window update** as soon as the application reads data out of a full buffer, and periodically re-advertises its window as a fallback.

### Concurrency

Each side runs the protocol in a dedicated thread, separate from the application. The application calls `send_data()` and `recv_data()`, while the protocol thread multiplexes all connections with `poll()` over their sockets and timers. The state shared by the two threads (windows, buffers, sequence numbers) belongs to each connection and is protected by a per-connection mutex, so that work on one connection never blocks another.

## Application interface

The protocol is exposed through an API that mirrors the BSD socket calls:

```c
void init_sender(int speed, int delay);          // link parameters for window sizing
int  setup_connection(uint32_t ip, uint16_t port); // like connect()
int  send_data(int conn_id, char *buf, int len);   // like send()

void init_receiver(int recv_buffer_bytes);
int  wait4connect(uint32_t ip, uint16_t port);     // like accept()
int  recv_data(int conn_id, char *buf, int len);   // like recv(), blocking
```

`client.cpp` uses it to send a file, prefixed by its size; `server.cpp` writes each incoming stream to `file.out`, or to `fileN.out` when it serves several clients.

## Building and running

```
make
./server                 # one client
./server 3               # three simultaneous clients
./client <file>          # in another terminal or host
```

The client connects to `172.16.0.100:8032`, the server's address in the test topology.

## Testing

The course provides a Mininet topology (`checker/topo.py`) with two hosts connected through a router, on links of 8 Mb/s with 1 ms delay and 5% packet loss each, and a set of text and image files of different sizes in `checker/tests/`. A transfer is correct only if the received file is identical to the original.

The protocol was also tested on a local loopback interface with packet loss injected at the socket level in both directions. A 5.8 MB file was transferred intact in under one second without loss and in about 25 seconds with 5% loss. With three clients transferring different files at once, all three were reconstructed exactly.

## Repository layout

```
lib/include/protocol.h   segment headers and segment types
lib/include/lib.h        connection control block and the public API
lib/libsend.cpp          sender: handshake, sliding window, retransmission
lib/librecv.cpp          receiver: handshake, reordering, acknowledgements, flow control
lib/libcommon.cpp        poll-based multiplexing of sockets and timers
client.cpp, server.cpp   file transfer application
checker/                 Mininet topology, test files and test script
```
