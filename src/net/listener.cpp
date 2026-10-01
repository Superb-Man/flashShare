#include "net/listener.h"
#include "util/logger.h"

#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <poll.h>

// Opens a TCP listening socket and accepts incoming connections.
// Both blocking and deadline-bounded accept paths are used by receiver and
// relay control listeners.
// Accepted sockets are handed to their caller for transfer or control
// handling; the listener itself does not process protocol messages.

namespace flashshare {

constexpr int DEFAULT_SEND_BUF = 4 * 1024 * 1024; // 4 MB
constexpr int DEFAULT_RECV_BUF = 4 * 1024 * 1024; // 4 MB

Listener::Listener() : running_(false), port_(0) {}

Listener::~Listener() {
    stop();
}

bool Listener::start(const std::string& address, uint16_t port, int backlog) {
    if (!listen_socket_.create()) {
        return false;
    }

    listen_socket_.set_reuseaddr(true);

    if (!listen_socket_.bind(address, port)) {
        return false;
    }

    if (!listen_socket_.listen(backlog)) {
        return false;
    }

    sockaddr_in bound_address{};
    socklen_t address_length = sizeof(bound_address);
    
    if (getsockname(listen_socket_.fd(), reinterpret_cast<sockaddr*>(&bound_address), &address_length) != 0) {
        listen_socket_.close();
        return false;
    }

    port_ = ntohs(bound_address.sin_port);
    running_ = true;
    LOG_INFO("Listening on %s:%u", address.empty() ? "0.0.0.0" : address.c_str(), port_);
    return true;
}

Socket Listener::accept_one() {
    Socket client = listen_socket_.accept();
    if (client.is_valid()) {
        client.set_buffer_size(DEFAULT_SEND_BUF, DEFAULT_RECV_BUF);
        client.set_nodelay(true);
        client.set_cork(true);

        LOG_INFO("Connection from %s:%u", client.peer_address().c_str(), client.peer_port());
    }
    return client;
}

Socket Listener::accept_one_for(std::chrono::milliseconds timeout) {
    pollfd event{listen_socket_.fd(), POLLIN, 0};
    if (::poll(&event, 1, static_cast<int>(timeout.count())) <= 0 ||
        !(event.revents & POLLIN)) {
        return Socket{};
    }
    return accept_one();
}

void Listener::accept_loop(OnConnectCallback callback) {
    LOG_INFO("Accept loop started");

    while (running_) {
        Socket client = accept_one();
        if (!client.is_valid()) {
            if (running_) {
                LOG_WARN("Accept failed, continuing...");
            }
            continue;
        }
        callback(std::move(client));
    }

    LOG_INFO("Accept loop stopped");
}

void Listener::stop() {
    running_ = false;
    listen_socket_.close();
}

}
