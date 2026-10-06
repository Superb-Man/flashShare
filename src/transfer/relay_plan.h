#pragma once

#include <cstdint>
#include <string>
#include <vector>

// Defines candidate endpoints, discovery results, and an ordered relay plan.
// A plan carries the transfer identity and epoch used to distinguish a
// rebuilt chain from its earlier assignment.
// The coordinator uses the probe result to choose participants; nodes in
// the plan are ordered from sender-facing head to final receiver.

namespace flashshare {

struct RelayEndpoint {
    std::string address;
    uint16_t port = 5117;
};

// for connection
struct RelayProbeResult {
    RelayEndpoint endpoint; 
    bool responded = false; // whether the endpoint responded to the probe created by the relay coordinator
    bool available = false; // whether the endpoint is available for relay participation
    uint64_t offset = 0; // Offset reported by this node during relay discovery.
    std::string message;
};

struct RelayPlan {
    std::string discovery_id;
    std::string transfer_id;
    uint64_t epoch = 0; // Initial plan is 0; each rebuild gets a newer epoch.

    std::vector<RelayEndpoint> nodes;

    bool empty() const {
        return nodes.empty();
    }

    bool is_direct() const {
        return nodes.size() == 1;
    }

    bool is_chain() const {
        return nodes.size() > 1;
    }
};

} // namespace flashshare