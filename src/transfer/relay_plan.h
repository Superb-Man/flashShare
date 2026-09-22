#pragma once

#include <cstdint>
#include <string>
#include <vector>

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
    std::string message;
};

struct RelayPlan {
    std::string discovery_id;
    std::string transfer_id;

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