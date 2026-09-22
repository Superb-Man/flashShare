#include "transfer/relay_coordinator.h"

#include "net/relay_connection.h"
#include "net/socket.h"
#include "transfer/relay_discovery.h"
#include "util/logger.h"

#include <algorithm>
#include <exception>
#include <iomanip>
#include <limits>
#include <random>
#include <sstream>
#include <stdexcept>
#include <utility>

namespace flashshare {

RelayCoordinator::RelayCoordinator(
    std::vector<std::string> paths,
    std::vector<RelayEndpoint> candidates,
    std::chrono::milliseconds discovery_timeout)
    : paths_(std::move(paths)),
      candidates_(std::move(candidates)),
      discovery_timeout_(discovery_timeout) {}

std::string RelayCoordinator::generate_id() {
    std::random_device random_device;
    std::mt19937_64 generator(random_device());
    std::uniform_int_distribution<uint64_t> distribution;

    std::ostringstream value;
    value << std::hex << std::setfill('0');
    value << std::setw(16) << distribution(generator);
    value << std::setw(16) << distribution(generator);
    return value.str();
}

bool RelayCoordinator::build_plan(RelayPlan& plan) {
    plan = RelayPlan{}; 
    manifest_.reset();
    probe_results_.clear();
    last_error_.clear();

    try {
        if (candidates_.empty()) {
            throw std::runtime_error("No relay candidates provided");
        }

        if (discovery_timeout_.count() <= 0) {
            throw std::runtime_error("Relay discovery timeout must be greater than zero");
        }

        for (const auto& candidate : candidates_) {
            if (candidate.address.empty()) {
                throw std::runtime_error("Empty relay candidate address");
            }

            if (candidate.port == 0) {
                throw std::runtime_error("Relay candidate port must be nonzero");
            }
        }

        manifest_ = flashshare::prepare_manifest(paths_);

        if (manifest_->files.size() > std::numeric_limits<uint32_t>::max()) {
            throw std::runtime_error("Relay manifest contains too many files");
        }

        plan.discovery_id = generate_id();
        plan.transfer_id = generate_id();

        RelayDiscoveryRequest request;
        request.version = PROTOCOL_VERSION;
        request.discovery_id = plan.discovery_id;
        request.total_size = manifest_->total_size;
        request.file_count = static_cast<uint32_t>(manifest_->files.size());

        RelayDiscovery discovery(candidates_, discovery_timeout_);
        probe_results_ = discovery.run(request);

        for (const auto& result : probe_results_) {
            if (result.responded && result.available) {
                plan.nodes.push_back(result.endpoint);
                LOG_INFO(
                    "[%s:%u] Available for relay discovery %s",
                    result.endpoint.address.c_str(),
                    result.endpoint.port,
                    plan.discovery_id.c_str()
                );
                continue;
            }

            LOG_WARN(
                "[%s:%u] Excluded from relay plan: %s",
                result.endpoint.address.c_str(),
                result.endpoint.port,
                result.message.c_str()
            );
        }

        if (plan.empty()) {
            last_error_ = "No receiver was available for the relay transfer";
            LOG_ERROR("%s", last_error_.c_str());
            return false;
        }

        LOG_INFO(
            "Built relay plan %s with %zu/%zu receiver(s)",
            plan.discovery_id.c_str(),
            plan.nodes.size(),
            candidates_.size()
        );

        return true;
    } catch (const std::exception& error) {
        last_error_ = error.what();
    } catch (...) {
        last_error_ = "Unexpected failure while building relay plan";
    }

    // Reset the plan and manifest in case of failure
    plan = RelayPlan{};
    manifest_.reset();
    LOG_ERROR("Cannot build relay plan: %s", last_error_.c_str());
    return false;
}

bool RelayCoordinator::send_assignment(
    const RelayPlan& plan,
    const RelayEndpoint& receiver,
    const RelayEndpoint* downstream
) const {
    Socket socket;
    if (!socket.create()) {
        LOG_ERROR(
            "[%s:%u] Cannot create relay-assignment socket",
            receiver.address.c_str(),
            receiver.port
        );
        return false;
    }

    if (!socket.relay_connect(
            receiver.address,
            receiver.port,
            discovery_timeout_)) {
        LOG_ERROR(
            "[%s:%u] Cannot connect for relay assignment",
            receiver.address.c_str(),
            receiver.port
        );
        return false;
    }

    if (!socket.relay_set_send_timeout(discovery_timeout_) ||
        !socket.relay_set_recv_timeout(discovery_timeout_)) {
        LOG_ERROR(
            "[%s:%u] Cannot configure relay-assignment timeout",
            receiver.address.c_str(),
            receiver.port
        );
        return false;
    }

    socket.set_nodelay(true);

    Connection connection(std::move(socket));
    RelayConnection relay_connection(connection);

    RelayAssignmentRequest request;
    request.version = PROTOCOL_VERSION;
    request.discovery_id = plan.discovery_id;
    request.transfer_id = plan.transfer_id;
    request.total_size = manifest_->total_size;
    request.file_count = static_cast<uint32_t>(manifest_->files.size());

    if (downstream != nullptr) {
        request.has_downstream = true;
        request.downstream_address = downstream->address;
        request.downstream_port = downstream->port;
    }

    if (!relay_connection.send_assignment_request(request)) {
        LOG_ERROR(
            "[%s:%u] Cannot send relay assignment",
            receiver.address.c_str(),
            receiver.port
        );
        return false;
    }

    RelayAssignmentResponse response;
    
    if (!relay_connection.recv_assignment_response(response)) {
        LOG_ERROR(
            "[%s:%u] Did not return a relay-assignment response",
            receiver.address.c_str(),
            receiver.port
        );
        return false;
    }

    if (response.discovery_id != plan.discovery_id ||
        response.transfer_id != plan.transfer_id) {
        LOG_ERROR(
            "[%s:%u] Returned a mismatched relay assignment response",
            receiver.address.c_str(),
            receiver.port
        );
        return false;
    }

    if (!response.accepted) {
        LOG_ERROR(
            "[%s:%u] Rejected relay assignment: %s",
            receiver.address.c_str(),
            receiver.port,
            response.message.c_str()
        );
        return false;
    }

    return true;
}

bool RelayCoordinator::assign_plan(RelayPlan& plan) {
    last_error_.clear();

    if (!manifest_) {
        last_error_ = "Relay manifest has not been prepared";
        LOG_ERROR("%s", last_error_.c_str());
        return false;
    }

    if (plan.discovery_id.empty() || plan.transfer_id.empty() || plan.empty()) {
        last_error_ = "Relay plan is incomplete";
        LOG_ERROR("%s", last_error_.c_str());
        return false;
    }

    if (plan.is_direct()) {
        LOG_INFO(
            "Relay plan has one receiver; using the normal direct transfer"
        );
        return true;
    }

    const std::size_t discovered_count = plan.nodes.size();
    std::vector<RelayEndpoint> assigned_reversed;
    assigned_reversed.reserve(discovered_count);

    // Work backward so an upstream receiver is only given a downstream
    // receiver that has already accepted its assignment.
    for (auto it = plan.nodes.rbegin(); it != plan.nodes.rend(); ++it) {
        const RelayEndpoint* downstream = assigned_reversed.empty()
            ? nullptr
            : &assigned_reversed.back();

        if (send_assignment(plan, *it, downstream)) {
            assigned_reversed.push_back(*it);
            continue;
        }

        LOG_WARN(
            "[%s:%u] Excluded because relay assignment failed",
            it->address.c_str(),
            it->port
        );
    }

    std::reverse(assigned_reversed.begin(), assigned_reversed.end());

    plan.nodes = std::move(assigned_reversed);

    if (plan.empty()) {
        last_error_ = "No receiver accepted its relay assignment";
        LOG_ERROR("%s", last_error_.c_str());
        return false;
    }

    LOG_INFO(
        "Assigned relay plan %s to %zu/%zu receiver(s)",
        plan.discovery_id.c_str(),
        plan.nodes.size(),
        discovered_count
    );

    return true;
}

} // namespace flashshare
