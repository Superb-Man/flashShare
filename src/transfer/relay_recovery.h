#pragma once

#include "transfer/relay_coordinator.h"
#include "transfer/relay_membership.h"
#include "transfer/relay_plan.h"

#include <memory>

// Declares coordinator-side recovery for one live relay transfer.
// It owns the active plan and controls pausing, rebuilding, and resuming
// the sender when relay membership changes.
// It retains the shared manifest through RelayCoordinator so later attempts
// can resume the same transfer without rehashing the input files.

namespace flashshare {

// Owns one live relay transfer. Heartbeats run while Sender streams; only a
// membership change stops the data connection and rebuilds the chain.
class RelayRecovery {
public:
    RelayRecovery(RelayCoordinator& coordinator, std::shared_ptr<RelayMembership> membership, RelayPlan plan);

    int run();

private:
    bool rebuild(const RelayHealthCheck& check);

    RelayCoordinator& coordinator_;
    std::shared_ptr<RelayMembership> membership_;
    RelayPlan plan_;
};

} // namespace flashshare
