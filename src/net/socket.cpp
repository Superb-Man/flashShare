#include "net/socket.h"
#include "util/logger.h"

#include <cerrno>
#include <cstring>

/**
 * Credit - Cline
 * 
 */

#ifdef _WIN32
// Winsock2 path
#include <io.h>
#include <fcntl.h>
#define CLOSE_SOCKET closesocket
#define GET_ERRNO WSAGetLastError()
#define EINTR WSAEINTR
#define EAGAIN WSAEWOULDBLOCK
#define EINPROGRESS WSAEWOULDBLOCK
#define MSG_NOSIGNAL 0

// Windows doesn't have TCP_CORK
#define TCP_CORK -1

// Windows TransmitFile for zero-copy
#ifndef TF_USE_KERNEL_APC
#define TF_USE_KERNEL_APC 0x00000002
#endif

#else
// Linux path
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/sendfile.h>
#define CLOSE_SOCKET ::close
#define GET_ERRNO errno
#endif

namespace flashshare {

// --- Winsock global init/shutdown ---

bool net_init() {
#ifdef _WIN32
    WSADATA wsaData;
    int result = WSAStartup(MAKEWORD(2, 2), &wsaData);
    if (result != 0) {
        LOG_ERROR("WSAStartup failed: %d", result);
        return false;
    }
#endif
    return true;
}

void net_shutdown() {
#ifdef _WIN32
    WSACleanup();
#endif
}

// --- Socket implementation ---

Socket::Socket() : fd_(-1) {}

Socket::Socket(int fd) : fd_(fd) {}

Socket::~Socket() {
    close();
}

Socket::Socket(Socket&& other) noexcept : fd_(other.fd_) {
    other.fd_ = -1;
}

Socket& Socket::operator=(Socket&& other) noexcept {
    if (this != &other) {
        close();
        fd_ = other.fd_;
        other.fd_ = -1;
    }
    return *this;
}

void Socket::release() {
    fd_ = -1;
}

bool Socket::create() {
    fd_ = static_cast<int>(::socket(AF_INET, SOCK_STREAM, 0));
    if (fd_ < 0) {
        LOG_ERROR("socket() failed: %s", strerror(GET_ERRNO));
        return false;
    }
    return true;
}

bool Socket::bind(const std::string& address, uint16_t port) {
    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);

    if (address.empty() || address == "0.0.0.0") {
        addr.sin_addr.s_addr = INADDR_ANY;
    } else {
        if (inet_pton(AF_INET, address.c_str(), &addr.sin_addr) <= 0) {
            LOG_ERROR("Invalid address: %s", address.c_str());
            return false;
        }
    }

    if (::bind(fd_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        LOG_ERROR("bind() failed: %s", strerror(GET_ERRNO));
        return false;
    }
    return true;
}

bool Socket::listen(int backlog) {
    if (::listen(fd_, backlog) < 0) {
        LOG_ERROR("listen() failed: %s", strerror(GET_ERRNO));
        return false;
    }
    return true;
}

Socket Socket::accept() {
    struct sockaddr_in client_addr{};
    socklen_t addr_len = sizeof(client_addr);
    int client_fd = static_cast<int>(::accept(fd_, reinterpret_cast<struct sockaddr*>(&client_addr), &addr_len));
    if (client_fd < 0) {
        LOG_ERROR("accept() failed: %s", strerror(GET_ERRNO));
        return Socket(-1);
    }
    return Socket(client_fd);
}

bool Socket::connect(const std::string& address, uint16_t port, int timeout_sec) {
    return relay_connect(address, port, std::chrono::seconds(timeout_sec));
}

bool Socket::relay_connect_blocking(const std::string& address, uint16_t port) {
    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);

    if (inet_pton(AF_INET, address.c_str(), &addr.sin_addr) != 1) {
        LOG_ERROR("Invalid address: %s", address.c_str());
        return false;
    }

    if (::connect(fd_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        LOG_ERROR("connect() failed: %s", strerror(GET_ERRNO));
        return false;
    }

    return true;
}

bool Socket::relay_connect(const std::string& address, uint16_t port,
                           std::chrono::milliseconds timeout) {
    const auto timeout_ms = timeout.count();
    if (timeout_ms <= 0) {
        LOG_ERROR("connect() deadline expired");
        return false;
    }

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);

    if (inet_pton(AF_INET, address.c_str(), &addr.sin_addr) <= 0) {
        LOG_ERROR("Invalid address: %s", address.c_str());
        return false;
    }

    // Set non-blocking for timeout connect
#ifdef _WIN32
    u_long mode = 1;
    ioctlsocket(fd_, FIONBIO, &mode);
#else
    int flags = fcntl(fd_, F_GETFL, 0);
    fcntl(fd_, F_SETFL, flags | O_NONBLOCK);
#endif

    int ret = ::connect(fd_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr));
    if (ret < 0 && GET_ERRNO != EINPROGRESS) {
        LOG_ERROR("connect() failed: %s", strerror(GET_ERRNO));
#ifdef _WIN32
        mode = 0;
        ioctlsocket(fd_, FIONBIO, &mode);
#else
        fcntl(fd_, F_SETFL, flags);
#endif
        return false;
    }

    if (ret == 0) {
        // Connected immediately
#ifdef _WIN32
        mode = 0;
        ioctlsocket(fd_, FIONBIO, &mode);
#else
        fcntl(fd_, F_SETFL, flags);
#endif
        return true;
    }

    // Wait for connection with timeout
    fd_set write_fds;
    FD_ZERO(&write_fds);
    FD_SET(fd_, &write_fds);

    struct timeval tv;
    tv.tv_sec = static_cast<long>(timeout_ms / 1000);
    tv.tv_usec = static_cast<long>((timeout_ms % 1000) * 1000);

    ret = ::select(fd_ + 1, nullptr, &write_fds, nullptr, &tv);
    if (ret <= 0) {
        LOG_ERROR("connect() timeout or error");
#ifdef _WIN32
        mode = 0;
        ioctlsocket(fd_, FIONBIO, &mode);
#else
        fcntl(fd_, F_SETFL, flags);
#endif
        return false;
    }

    int err = 0;
    socklen_t err_len = sizeof(err);
    getsockopt(fd_, SOL_SOCKET, SO_ERROR, reinterpret_cast<char*>(&err), &err_len);
#ifdef _WIN32
    mode = 0;
    ioctlsocket(fd_, FIONBIO, &mode); // Restore blocking mode
#else
    fcntl(fd_, F_SETFL, flags); // Restore blocking mode
#endif

    if (err != 0) {
        LOG_ERROR("connect() failed: %s", strerror(err));
        return false;
    }

    return true;
}

bool Socket::set_buffer_size(int send_buf, int recv_buf) {
    bool ok = true;
    if (send_buf > 0) {
        if (setsockopt(fd_, SOL_SOCKET, SO_SNDBUF, reinterpret_cast<const char*>(&send_buf), sizeof(send_buf)) < 0) {
            LOG_WARN("setsockopt SO_SNDBUF failed: %s", strerror(GET_ERRNO));
            ok = false;
        }
    }
    if (recv_buf > 0) {
        if (setsockopt(fd_, SOL_SOCKET, SO_RCVBUF, reinterpret_cast<const char*>(&recv_buf), sizeof(recv_buf)) < 0) {
            LOG_WARN("setsockopt SO_RCVBUF failed: %s", strerror(GET_ERRNO));
            ok = false;
        }
    }
    return ok;
}

bool Socket::set_cork(bool enable) {
#ifdef _WIN32
    // Windows has no TCP_CORK equivalent — TCP_NODELAY off achieves similar batching
    // We just ignore cork on Windows (TCP_NODELAY is already set)
    (void)enable;
    return true;
#else
    int val = enable ? 1 : 0;
    if (setsockopt(fd_, IPPROTO_TCP, TCP_CORK, &val, sizeof(val)) < 0) {
        LOG_WARN("setsockopt TCP_CORK failed: %s", strerror(errno));
        return false;
    }
    return true;
#endif
}

bool Socket::set_nodelay(bool enable) {
    int val = enable ? 1 : 0;
    if (setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<const char*>(&val), sizeof(val)) < 0) {
        LOG_WARN("setsockopt TCP_NODELAY failed: %s", strerror(GET_ERRNO));
        return false;
    }
    return true;
}

bool Socket::set_reuseaddr(bool enable) {
    int val = enable ? 1 : 0;
    if (setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&val), sizeof(val)) < 0) {
        LOG_WARN("setsockopt SO_REUSEADDR failed: %s", strerror(GET_ERRNO));
        return false;
    }
    return true;
}

std::string Socket::peer_address() const {
    struct sockaddr_in addr{};
    socklen_t len = sizeof(addr);
    if (getpeername(fd_, reinterpret_cast<struct sockaddr*>(&addr), &len) < 0) {
        return "";
    }
    char buf[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &addr.sin_addr, buf, sizeof(buf));
    return std::string(buf);
}

uint16_t Socket::peer_port() const {
    struct sockaddr_in addr{};
    socklen_t len = sizeof(addr);
    if (getpeername(fd_, reinterpret_cast<struct sockaddr*>(&addr), &len) < 0) {
        return 0;
    }
    return ntohs(addr.sin_port);
}

ssize_t Socket::send(const void* buf, size_t len, int flags) {
#ifdef _WIN32
    return ::send(fd_, reinterpret_cast<const char*>(buf), static_cast<int>(len), flags);
#else
    return ::send(fd_, buf, len, flags | MSG_NOSIGNAL);
#endif
}

ssize_t Socket::recv(void* buf, size_t len, int flags) {
#ifdef _WIN32
    return ::recv(fd_, reinterpret_cast<char*>(buf), static_cast<int>(len), flags);
#else
    return ::recv(fd_, buf, len, flags);
#endif
}

bool Socket::send_all(const void* buf, size_t len) {
    const char* p = static_cast<const char*>(buf);
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = send(p + sent, len - sent);
        if (n < 0) {
            if (GET_ERRNO == EINTR) continue;
            LOG_ERROR("send_all failed: %s", strerror(GET_ERRNO));
            return false;
        }
        if (n == 0) return false;
        sent += static_cast<size_t>(n);
    }
    return true;
}

bool Socket::recv_all(void* buf, size_t len) {
    char* p = static_cast<char*>(buf);
    size_t received = 0;
    while (received < len) {
        ssize_t n = recv(p + received, len - received);
        if (n < 0) {
            if (GET_ERRNO == EINTR) continue;
            LOG_ERROR("recv_all failed: %s", strerror(GET_ERRNO));
            return false;
        }
        if (n == 0) return false; // Connection closed
        received += static_cast<size_t>(n);
    }
    return true;
}

ssize_t Socket::sendfile(int file_fd, off_t* offset, size_t count) {
#ifdef _WIN32
    // Windows: use TransmitFile for zero-copy
    HANDLE hFile = reinterpret_cast<HANDLE>(_get_osfhandle(file_fd));
    if (hFile == INVALID_HANDLE_VALUE) {
        LOG_ERROR("TransmitFile: invalid file handle");
        return -1;
    }

    // TransmitFile sends from file offset, not a pointer
    // We need to seek the file to *offset first, then transmit
    LARGE_INTEGER li;
    li.QuadPart = *offset;
    ::SetFilePointerEx(hFile, li, nullptr, FILE_BEGIN);

    DWORD sent = 0;
    BOOL ok = TransmitFile(fd_, hFile, static_cast<DWORD>(count), 0, nullptr, nullptr,
                           TF_USE_KERNEL_APC);
    if (!ok) {
        LOG_ERROR("TransmitFile failed: %d", WSAGetLastError());
        return -1;
    }

    // TransmitFile doesn't return bytes sent reliably; use count as approximation
    // Actually we need to check how many bytes were sent
    // TransmitFile sends the entire requested amount or fails
    *offset += count;
    return static_cast<ssize_t>(count);
#elif defined(__linux__)
    return ::sendfile(fd_, file_fd, offset, count);
#else
    // macOS: sendfile has different signature
    off_t len = static_cast<off_t>(count);
    int ret = ::sendfile(fd_, file_fd, offset, len, nullptr, nullptr, 0);
    if (ret < 0) return -1;
    return static_cast<ssize_t>(len);
#endif
}

bool Socket::set_recv_timeout(int sec) {
    return relay_set_recv_timeout(std::chrono::seconds(sec));
}

bool Socket::relay_set_recv_timeout(std::chrono::milliseconds timeout) {
    const auto timeout_ms = timeout.count();
    if (timeout_ms <= 0) {
        return false;
    }

#ifdef _WIN32
    DWORD value = static_cast<DWORD>(timeout_ms);
    return setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO,
                      reinterpret_cast<const char*>(&value), sizeof(value)) == 0;
#else
    struct timeval tv;
    tv.tv_sec = static_cast<long>(timeout_ms / 1000);
    tv.tv_usec = static_cast<long>((timeout_ms % 1000) * 1000);
    return setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, reinterpret_cast<const char*>(&tv), sizeof(tv)) == 0;
#endif
}

bool Socket::set_send_timeout(int sec) {
    return relay_set_send_timeout(std::chrono::seconds(sec));
}

bool Socket::relay_set_send_timeout(std::chrono::milliseconds timeout) {
    const auto timeout_ms = timeout.count();
    if (timeout_ms <= 0) {
        return false;
    }

#ifdef _WIN32
    DWORD value = static_cast<DWORD>(timeout_ms);
    return setsockopt(fd_, SOL_SOCKET, SO_SNDTIMEO,
                      reinterpret_cast<const char*>(&value), sizeof(value)) == 0;
#else
    struct timeval tv;
    tv.tv_sec = static_cast<long>(timeout_ms / 1000);
    tv.tv_usec = static_cast<long>((timeout_ms % 1000) * 1000);
    return setsockopt(fd_, SOL_SOCKET, SO_SNDTIMEO, reinterpret_cast<const char*>(&tv), sizeof(tv)) == 0;
#endif
}

void Socket::shutdown_write() {
    if (fd_ >= 0) {
#ifdef _WIN32
        ::shutdown(fd_, SD_SEND);
#else
        ::shutdown(fd_, SHUT_WR);
#endif
    }
}

void Socket::shutdown_both() {
    if (fd_ >= 0) {
#ifdef _WIN32
        ::shutdown(fd_, SD_BOTH);
#else
        ::shutdown(fd_, SHUT_RDWR);
#endif
    }
}

void Socket::close() {
    if (fd_ >= 0) {
        CLOSE_SOCKET(fd_);
        fd_ = -1;
    }
}

}
