#pragma once

#include "net/connection.h"
#include "net/listener.h"
#include "net/threadpool.h"
#include "transfer/relay_membership.h"

#include <atomic>
#include <cstdint>
#include <memory>
#include <thread>

// Declares the listener for relay nodes requesting rejoin after interruption.
// It receives control connections independently of the sender's data stream
// and passes valid requests to RelayMembership.
// A dedicated control listener accepts requests without relying on the
// relay data connection that may have broken.

namespace flashshare {

// Accepts REJOIN_REQ messages on a sender-owned control port. Each accepted
// request only updates RelayMembership's pending set; the heartbeat deadline
// decides whether and when to rebuild the chain.
class RelayRejoinListener {
public:
    explicit RelayRejoinListener(std::shared_ptr<RelayMembership> membership);
    ~RelayRejoinListener();

    RelayRejoinListener(const RelayRejoinListener&) = delete;
    RelayRejoinListener& operator=(const RelayRejoinListener&) = delete;

    bool start();
    void stop();
    uint16_t port() const { return port_; }

private:
    void run();
    void handle_connection(Connection& connection);

    std::shared_ptr<RelayMembership> membership_;
    Listener listener_;
    std::unique_ptr<ThreadPool> thread_pool_;
    std::atomic<bool> running_{false};
    std::thread worker_;
    uint16_t port_ = 0;
};

} // namespace flashshare
