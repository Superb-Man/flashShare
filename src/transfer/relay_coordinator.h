#pragma once

#include "transfer/prepared_manifest.h"
#include "transfer/relay_plan.h"

#include <chrono>
#include <memory>
#include <string>
#include <vector>

namespace flashshare {

// Prepares one transfer and discovers which requested receivers are available
// to participate in its relay chain.
class RelayCoordinator {
public:
    RelayCoordinator(
        std::vector<std::string> paths, 
        std::vector<RelayEndpoint> candidates,
        std::chrono::milliseconds discovery_timeout = std::chrono::seconds(4) // global timeout for relay discovery
    );

    bool build_plan(RelayPlan& plan);
    bool assign_plan(RelayPlan& plan);

    std::shared_ptr<const PreparedManifest> manifest() const {
        return manifest_;
    }

    const std::vector<RelayProbeResult>& probe_results() const {
        return probe_results_;
    }

    const std::string& last_error() const {
        return last_error_;
    }

private:
    std::vector<std::string> paths_;
    std::vector<RelayEndpoint> candidates_;
    std::chrono::milliseconds discovery_timeout_;

    std::shared_ptr<const PreparedManifest> manifest_;
    std::vector<RelayProbeResult> probe_results_;
    std::string last_error_;

    static std::string generate_id();
    bool send_assignment(
        const RelayPlan& plan,
        const RelayEndpoint& receiver,
        const RelayEndpoint* downstream
    ) const;
};

} // namespace flashshare
