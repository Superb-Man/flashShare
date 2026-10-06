#include "transfer/relay_membership.h"

#include <algorithm>
#include <stdexcept>
#include <utility>

// Maintains the initial and active node sets for one relay transfer.
// At each heartbeat deadline it combines timely replies with pending rejoin
// requests and decides whether to continue, rebuild, or close.
// Pending rejoin requests are deduplicated by node and are consumed only
// when a heartbeat decision starts a new build.

namespace flashshare {

RelayMembership::RelayMembership(std::string transfer_id,
                                 std::vector<RelayEndpoint> relay_nodes,
                                 std::chrono::milliseconds heartbeat_interval)
    : transfer_id_(std::move(transfer_id)),
      relay_nodes_(std::move(relay_nodes)),
      heartbeat_interval_(heartbeat_interval) {
    if (transfer_id_.empty() || relay_nodes_.empty() || heartbeat_interval_.count() <= 0) {
        throw std::invalid_argument("Relay membership requires an ID, nodes, and a positive interval");
    }

    std::unordered_set<std::string> addresses;
    for (const auto& node : relay_nodes_) {
        if (node.address.empty() || !addresses.insert(node.address).second) {
            throw std::invalid_argument("Relay nodes need distinct, nonempty IP addresses");
        }
    }
}

bool RelayMembership::same_endpoint(const RelayEndpoint& a, const RelayEndpoint& b) {
    return a.address == b.address && a.port == b.port;
}

bool RelayMembership::known_node(const RelayEndpoint& node) const {
    return std::any_of(relay_nodes_.begin(), relay_nodes_.end(),
        [&](const RelayEndpoint& candidate) { return same_endpoint(candidate, node); });
}

bool RelayMembership::activate(const RelayPlan& plan, Clock::time_point now) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (closed_ || !building_ || plan.transfer_id != transfer_id_ || plan.nodes.empty() ||
        (activated_once_ && plan.epoch <= epoch_)) {
        return false;
    }

    std::lock_guard<std::mutex> pending_lock(pending_mutex_);
    std::unordered_set<std::string> addresses;
    for (const auto& node : plan.nodes) {
        if (!known_node(node) || !addresses.insert(node.address).second ||
            pending_rejoins_.count(node.address) != 0) {
            return false;
        }
    }

    active_nodes_ = plan.nodes;
    epoch_ = plan.epoch;
    activated_once_ = true;
    replies_.clear();
    poll_open_ = false;
    next_heartbeat_ = now + heartbeat_interval_;
    building_ = false;
    // Rejoins received during the build stay pending for the next poll.
    return true;
}

RelayMembership::HeartbeatPoll RelayMembership::start_heartbeat(Clock::time_point now) {
    std::lock_guard<std::mutex> lock(mutex_);
    HeartbeatPoll poll;
    if (closed_ || building_ || poll_open_ || now < next_heartbeat_) return poll;

    replies_.clear();
    reply_deadline_ = now + REPLY_TIMEOUT;
    poll_open_ = true;

    poll.started = true;
    poll.transfer_id = transfer_id_;
    poll.epoch = epoch_;
    poll.nodes = active_nodes_;
    poll.deadline = reply_deadline_;
    return poll;
}

bool RelayMembership::heartbeat_reply(const std::string& transfer_id, uint64_t epoch,
                                      const RelayEndpoint& node, Clock::time_point now) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (closed_ || building_ || !poll_open_ || now > reply_deadline_ ||
        transfer_id != transfer_id_ || epoch != epoch_) {
        return false;
    }

    for (const auto& active : active_nodes_) {
        if (same_endpoint(active, node)) {
            replies_.insert(node.address);
            return true;
        }
    }
    return false;
}

RelayHealthCheck RelayMembership::finish_heartbeat(Clock::time_point now) {
    std::lock_guard<std::mutex> lock(mutex_);
    RelayHealthCheck result;
    if (closed_ || building_ || !poll_open_ || now < reply_deadline_) return result;

    result.evaluated = true;
    
    for (const auto& active : active_nodes_) {
        if (replies_.count(active.address) == 0) result.missing.push_back(active);
    }
    // Hold the pending lock through the whole membership update. Requests
    // arriving after this update remain pending for the next heartbeat round.
    std::lock_guard<std::mutex> pending_lock(pending_mutex_);
    result.had_pending_rejoin = !pending_rejoins_.empty();
    for (const auto& node : relay_nodes_) {
        if (pending_rejoins_.count(node.address) != 0) result.joining.push_back(node);
    }
    replies_.clear();
    poll_open_ = false;

    const bool no_replies = result.missing.size() == active_nodes_.size();
    if (no_replies && result.joining.empty()) {
        active_nodes_.clear();
        closed_ = true;
        result.close_coordinator = true;
        return result;
    }

    if (result.missing.empty() && result.joining.empty()) {
        next_heartbeat_ = now + heartbeat_interval_;
        return result;
    }

    active_nodes_.erase(std::remove_if(active_nodes_.begin(), active_nodes_.end(),
        [&](const RelayEndpoint& node) {
            return std::any_of(result.missing.begin(), result.missing.end(),
                [&](const RelayEndpoint& missing) { return same_endpoint(node, missing); });
        }), active_nodes_.end());
    for (const auto& node : result.joining) {
        if (std::none_of(active_nodes_.begin(), active_nodes_.end(),
                [&](const RelayEndpoint& active) { return same_endpoint(active, node); })) {
            active_nodes_.push_back(node);
        }
    }
    pending_rejoins_.clear();
    building_ = true;
    result.rebuild = true;
    return result;
}

bool RelayMembership::queue_rejoin(const std::string& transfer_id,
                                   const RelayEndpoint& node) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (closed_ || transfer_id != transfer_id_ || !known_node(node)) return false;
    // A receiver that restarts before the next heartbeat can still appear
    // active. Its explicit REJOIN_REQ must rebuild the lost data connection.
    std::lock_guard<std::mutex> pending_lock(pending_mutex_);
    pending_rejoins_.insert(node.address);
    return true;
}

bool RelayMembership::can_join_current_build(const RelayEndpoint& node) const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (closed_ || !building_ || !known_node(node)) return false;
    std::lock_guard<std::mutex> pending_lock(pending_mutex_);
    return pending_rejoins_.count(node.address) == 0;
}

bool RelayMembership::building() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return building_;
}

std::vector<RelayEndpoint> RelayMembership::active_nodes() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return active_nodes_;
}

} // namespace flashshare
