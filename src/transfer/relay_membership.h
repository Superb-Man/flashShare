#pragma once

#include "transfer/relay_plan.h"

#include <chrono>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_set>
#include <vector>

// Declares coordinator-owned relay membership and heartbeat state.
// It tracks the current epoch, active nodes, reply deadline, and pending
// rejoin set while a new chain is being built.
// Requests arriving during a chain build stay pending for the following
// heartbeat decision rather than entering that build.

namespace flashshare {

struct RelayHealthCheck {
    bool evaluated = false;
    bool rebuild = false;
    bool close_coordinator = false;
    bool had_pending_rejoin = false;
    std::vector<RelayEndpoint> missing;
    std::vector<RelayEndpoint> joining;
};

// Coordinator-owned state for one transfer. Network I/O is done by callers.
// Tracks which receivers belong to one relay transfer and decides when
// its chain needs re building
class RelayMembership {
public:
    using Clock = std::chrono::steady_clock;
    static constexpr std::chrono::seconds REPLY_TIMEOUT{2};

    struct HeartbeatPoll {
        bool started = false;
        std::string transfer_id;
        uint64_t epoch = 0;
        std::vector<RelayEndpoint> nodes;
        Clock::time_point deadline{};
    };

    RelayMembership(std::string transfer_id, std::vector<RelayEndpoint> relay_nodes,
         std::chrono::milliseconds heartbeat_interval
    );

    // Accepts a completed chain plan
    bool activate(const RelayPlan& plan, Clock::time_point now);

    // Initiates a new heartbeat round for the active nodes with a shared  2 second deadline
    HeartbeatPoll start_heartbeat(Clock::time_point now); 
    bool heartbeat_reply(const std::string& transfer_id, uint64_t epoch, const RelayEndpoint& node, Clock::time_point now);
    
    // Checks missing replies and pending rejoins. 
    // It either continues, requests a rebuild, or 
    // closes when nobody replied and nobody requested rejoin.
    RelayHealthCheck finish_heartbeat(Clock::time_point now); // 

    // Records a known node’s rejoin request, deduplicated by IP.
    bool queue_rejoin(const std::string& transfer_id, const RelayEndpoint& node);

    // Checks whether a node may enter the build underway
    // pending rejoins must wait for the next round.
    bool can_join_current_build(const RelayEndpoint& node) const;
    bool building() const;
    std::vector<RelayEndpoint> active_nodes() const;
    const std::vector<RelayEndpoint>& relay_nodes() const { return relay_nodes_; }

private:
    static bool same_endpoint(const RelayEndpoint& a, const RelayEndpoint& b);
    bool known_node(const RelayEndpoint& node) const;

    const std::string transfer_id_;
    const std::vector<RelayEndpoint> relay_nodes_;
    const std::chrono::milliseconds heartbeat_interval_;

    mutable std::mutex mutex_;
    mutable std::mutex pending_mutex_;
    std::vector<RelayEndpoint> active_nodes_;
    std::unordered_set<std::string> pending_rejoins_;
    std::unordered_set<std::string> replies_;
    Clock::time_point next_heartbeat_{};
    Clock::time_point reply_deadline_{};
    uint64_t epoch_ = 0;
    bool activated_once_ = false;
    bool building_ = true;
    bool poll_open_ = false;
    bool closed_ = false;
};

} // namespace flashshare
