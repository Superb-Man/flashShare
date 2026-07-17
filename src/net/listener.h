#pragma once

#include "net/socket.h"
#include <string>
#include <cstdint>
#include <functional>

namespace flashshare {

/**
 * TCP listener that accepts incoming connections with high-performance socket tuning.
 */
class Listener {
public:
    using OnConnectCallback = std::function<void(Socket client)>;

    Listener();
    ~Listener();

    bool start(const std::string& address, uint16_t port, int backlog = 128);
    Socket accept_one();
    void accept_loop(OnConnectCallback callback);
    void stop();

    bool is_running() const { return running_; }
    uint16_t port() const { return port_; }

private:
    Socket listen_socket_;
    bool running_;
    uint16_t port_;
};

}
