#pragma once

#include "transfer/prepared_manifest.h"
#include "transfer/relay_membership.h"
#include "transfer/relay_plan.h"
#include "transfer/relay_rejoin_listener.h"

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

// Declares the sender-side coordinator for relay setup and health checks.
// It owns candidate endpoints and the shared manifest used by each rebuilt
// chain for the same transfer.
// It also owns the rejoin listener used while the sender streams into
// the head of the active relay chain.

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
    bool build_plan(RelayPlan& plan, const std::vector<RelayEndpoint>& nodes);
    bool assign_plan(RelayPlan& plan);

    // added by claude
    bool start_rejoin_listener(std::shared_ptr<RelayMembership> membership);
    uint16_t rejoin_port() const;
    void send_rejoin_hints(const std::vector<RelayEndpoint>& missing,
                           const std::string& transfer_id,
                           uint64_t epoch) const;
    using HeartbeatCheck = std::function<void(RelayHealthCheck)>;
    bool start_heartbeat_poll(std::shared_ptr<RelayMembership> membership, HeartbeatCheck on_check) const;

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
    std::string transfer_id_;
    uint64_t next_epoch_ = 0;
    std::vector<RelayProbeResult> probe_results_;
    std::string last_error_;
    std::unique_ptr<RelayRejoinListener> rejoin_listener_;

    static std::string generate_id();
    bool send_assignment(
        const RelayPlan& plan,
        const RelayEndpoint& receiver,
        const RelayEndpoint* downstream
    ) const;
};

} // namespace flashshare
