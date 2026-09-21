#pragma once

#include <chrono>
#include <string>
#include <cstdint>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#include <mswsock.h>
#pragma comment(lib, "ws2_32.lib")
#pragma comment(lib, "mswsock.lib")
typedef int socklen_t;
typedef SSIZE_T ssize_t;
#else
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <unistd.h>
#endif

namespace flashshare {

// Winsock global init/shutdown (no-op on Linux)
bool net_init();
void net_shutdown();

/**
 * RAII wrapper around a POSIX socket fd with high-performance tuning.
 * Supports zero-copy sendfile, large socket buffers, TCP_CORK.
 */
class Socket {
public:
    Socket();
    explicit Socket(int fd);
    ~Socket();

    Socket(const Socket&) = delete;
    Socket& operator=(const Socket&) = delete;
    Socket(Socket&& other) noexcept;
    Socket& operator=(Socket&& other) noexcept;

    bool is_valid() const { return fd_ >= 0; }
    int fd() const { return fd_; }
    void release();

    // Create a TCP socket
    bool create();

    // Bind to address:port (empty address = INADDR_ANY)
    bool bind(const std::string& address, uint16_t port);

    // Listen for connections
    bool listen(int backlog = 128);

    // Accept a connection (returns new Socket)
    Socket accept();

    // Connect to remote address:port
    bool connect(const std::string& address, uint16_t port, int timeout_sec = 10);
    bool relay_connect(const std::string& address, uint16_t port, std::chrono::milliseconds timeout);

    // Set socket buffer sizes (SO_SNDBUF / SO_RCVBUF)
    bool set_buffer_size(int send_buf, int recv_buf);

    // Enable TCP_CORK (batch small writes)
    bool set_cork(bool enable);

    // Enable TCP_NODELAY (disable Nagle)
    bool set_nodelay(bool enable);

    // Set SO_REUSEADDR
    bool set_reuseaddr(bool enable);

    // Get peer address
    std::string peer_address() const;
    uint16_t peer_port() const;

    // Raw send/recv
    ssize_t send(const void* buf, size_t len, int flags = 0);
    ssize_t recv(void* buf, size_t len, int flags = 0);

    // Send all bytes (loops until all sent)
    bool send_all(const void* buf, size_t len);

    // Receive exactly len bytes
    bool recv_all(void* buf, size_t len);

    // Zero-copy: send file content directly from kernel
    // Returns bytes sent, or -1 on error
    ssize_t sendfile(int file_fd, off_t* offset, size_t count);

    // Set receive timeout
    bool set_recv_timeout(int sec);
    bool relay_set_recv_timeout(std::chrono::milliseconds timeout);

    // Set send timeout
    bool set_send_timeout(int sec);
    bool relay_set_send_timeout(std::chrono::milliseconds timeout);

    // Shutdown
    void shutdown_write();

    // wake a worker blocked in recv()/sendfile() before its owner destroys it.
    void shutdown_both();
    void close();

private:
    int fd_;
};

} // namespace flashshare
