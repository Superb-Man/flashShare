#include "transfer/relay_rejoin_listener.h"

#include "net/relay_connection.h"
#include "util/logger.h"

#include <algorithm>
#include <chrono>
#include <utility>



// Accepts REJOIN_REQ messages on the coordinator's control listener.
// Known nodes enter the pending rejoin set and receive an acknowledgement;
// the heartbeat check decides when to rebuild the chain.
// Acknowledgement lets a receiver stop retrying once the coordinator has
// recorded its request in the pending set.

namespace flashshare {

RelayRejoinListener::RelayRejoinListener(std::shared_ptr<RelayMembership> membership)
    : membership_(std::move(membership)) {}

RelayRejoinListener::~RelayRejoinListener() {
    stop();
}

bool RelayRejoinListener::start() {
    if (!membership_ || running_ || !listener_.start("", 0, 16)) return false;

    port_ = listener_.port();
    thread_pool_ = std::make_unique<ThreadPool>(4, 16);
    running_ = true;

    try {
        worker_ = std::thread(&RelayRejoinListener::run, this);
    } catch (...) {
        running_ = false;
        listener_.stop();
        thread_pool_->stop(false);
        thread_pool_.reset();
        port_ = 0;
        return false;
    }
    LOG_INFO("Relay rejoin listener on port %u", port_);
    return true;
}

void RelayRejoinListener::stop() {
    running_ = false;
    if (worker_.joinable()) worker_.join();

    listener_.stop();

    if (thread_pool_) {
        thread_pool_->stop(true);
        thread_pool_.reset();
    }
    port_ = 0;
}

void RelayRejoinListener::run() {
    while (running_) {

        Socket client = listener_.accept_one_for(std::chrono::milliseconds(200));
        if (!running_ || !client.is_valid()) continue;

        auto connection = std::make_shared<Connection>(std::move(client));

        if (!thread_pool_->enqueue([this, connection] {
                handle_connection(*connection);
            })) {
            LOG_WARN("Relay rejoin request queue is full");
        }
    }
}

void RelayRejoinListener::handle_connection(Connection& connection) {
    const std::string peer_ip = connection.socket().peer_address();
    if (!connection.socket().relay_set_recv_timeout(std::chrono::seconds(1))) return;

    RelayConnection relay_connection(connection);
    RelayRejoinMessage request;

    if (!relay_connection.recv_rejoin_request(request)) return;

    const auto& known = membership_->relay_nodes();
    const auto found = std::find_if(known.begin(), known.end(),
        [&](const RelayEndpoint& node) { 
            return node.address == peer_ip; 
        }
    );

    if (found == known.end() || !membership_->queue_rejoin(request.transfer_id, *found)) {
        LOG_WARN("Ignored relay rejoin request from %s", peer_ip.c_str());
        return;
    }
    LOG_INFO("Queued relay rejoin request from %s for transfer %s", peer_ip.c_str(), request.transfer_id.c_str());
    
    connection.socket().relay_set_send_timeout(std::chrono::seconds(1));
    connection.socket().set_cork(false);

    relay_connection.send_rejoin_ack(request);
}

} // namespace flashshare
