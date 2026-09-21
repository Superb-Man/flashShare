#pragma once

#include "net/relay_connection.h"
#include "transfer/relay_plan.h"

#include <chrono>
#include <vector>

namespace flashshare {

// Sends one relay-discovery request to every candidate concurrently.
class RelayDiscovery {
public:
    RelayDiscovery(
        std::vector<RelayEndpoint> candidates,
        std::chrono::milliseconds timeout = std::chrono::seconds(4)
    );

    std::vector<RelayProbeResult> run(const RelayDiscoveryRequest& request) const;

private:
    using Clock = std::chrono::steady_clock;

    std::vector<RelayEndpoint> candidates_;
    std::chrono::milliseconds timeout_;

    RelayProbeResult probe(
        const RelayEndpoint& endpoint,
        const RelayDiscoveryRequest& request,
        Clock::time_point deadline
    ) const;

    static std::chrono::milliseconds remaining_time(Clock::time_point deadline);
};

} // namespace flashshare
