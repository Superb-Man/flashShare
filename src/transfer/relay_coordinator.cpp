#include "transfer/relay_coordinator.h"

#include "net/relay_connection.h"
#include "net/socket.h"
#include "net/threadpool.h"
#include "transfer/relay_discovery.h"
#include "transfer/relay_heartbeat.h"
#include "transfer/relay_timer.h"
#include "util/logger.h"

#include <algorithm>
#include <exception>
#include <iomanip>
#include <limits>
#include <map>
#include <random>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <utility>

// Prepares the manifest, discovers responding receivers, and assigns an
// ordered relay chain. It also sends health probes and rejoin hints so a
// changed membership can be rebuilt around durable offsets.
// Rebuilds keep the original transfer ID and assign a new epoch, allowing
// receivers to resume from their own confirmed offsets.

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
    return build_plan(plan, candidates_);
}

bool RelayCoordinator::build_plan(RelayPlan& plan,
                                  const std::vector<RelayEndpoint>& nodes) {
    plan = RelayPlan{}; 
    probe_results_.clear();
    last_error_.clear();

    try {
        if (nodes.empty()) {
            throw std::runtime_error("No relay candidates provided");
        }

        if (discovery_timeout_.count() <= 0) {
            throw std::runtime_error("Relay discovery timeout must be greater than zero");
        }

        for (const auto& candidate : nodes) {
            if (candidate.address.empty()) {
                throw std::runtime_error("Empty relay candidate address");
            }

            if (candidate.port == 0) {
                throw std::runtime_error("Relay candidate port must be nonzero");
            }
        }

        if (!manifest_) {
            manifest_ = flashshare::prepare_manifest(paths_);
        }

        if (manifest_->files.size() > std::numeric_limits<uint32_t>::max()) {
            throw std::runtime_error("Relay manifest contains too many files");
        }

        if (transfer_id_.empty()) {
            transfer_id_ = generate_id();
        }
        plan.discovery_id = generate_id();
        plan.transfer_id = transfer_id_;
        plan.epoch = next_epoch_++;

        RelayDiscoveryRequest request;
        request.version = PROTOCOL_VERSION;
        request.discovery_id = plan.discovery_id;
        request.transfer_id = plan.transfer_id;
        request.total_size = manifest_->total_size;
        request.file_count = static_cast<uint32_t>(manifest_->files.size());

        RelayDiscovery discovery(nodes, discovery_timeout_);
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
            nodes.size()
        );

        return true;
    } catch (const std::exception& error) {
        last_error_ = error.what();
    } catch (...) {
        last_error_ = "Unexpected failure while building relay plan";
    }

    // Keep the prepared manifest and transfer ID for a later rebuild.
    plan = RelayPlan{};
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

    // Bound only the control exchange; the data stream has its own lifetime.
    if (!socket.relay_connect(receiver.address, receiver.port,
                              std::chrono::seconds(2))) {
        LOG_ERROR(
            "[%s:%u] Cannot connect for relay assignment",
            receiver.address.c_str(),
            receiver.port
        );
        return false;
    }

    socket.set_nodelay(true);
    socket.relay_set_send_timeout(std::chrono::seconds(2));
    socket.relay_set_recv_timeout(std::chrono::seconds(2));

    Connection connection(std::move(socket));
    RelayConnection relay_connection(connection);

    RelayAssignmentRequest request;
    request.version = PROTOCOL_VERSION;
    request.discovery_id = plan.discovery_id;
    request.transfer_id = plan.transfer_id;
    request.epoch = plan.epoch;
    request.total_size = manifest_->total_size;
    request.file_count = static_cast<uint32_t>(manifest_->files.size());
    request.coordinator_rejoin_port = rejoin_port();

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
        response.transfer_id != plan.transfer_id ||
        response.epoch != plan.epoch) {
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
        if (rejoin_listener_ && !send_assignment(plan, plan.nodes.front(), nullptr)) {
            last_error_ = "Remaining relay receiver rejected its new assignment";
            return false;
        }
        LOG_INFO("Relay plan has one receiver; no forwarding hop is needed");
        return true;
    }

    // This is for recovery
    // Donot make any node wait to reach same offset
    // Instead build the network based on the reported offsets

    std::map<std::pair<std::string, uint16_t>, uint64_t> offsets;
    for (const auto& result : probe_results_) {
        if (result.responded && result.available) {
            offsets[{result.endpoint.address, result.endpoint.port}] = result.offset;
        }
    }

    const auto offset_for = [&](const RelayEndpoint& node) {
        const auto it = offsets.find({node.address, node.port});
        return it == offsets.end() ? uint64_t{0} : it->second;
    };

    std::stable_sort(plan.nodes.begin(), plan.nodes.end(),
        [&](const RelayEndpoint& upstream, const RelayEndpoint& downstream) {
            return offset_for(upstream) > offset_for(downstream);
        });

    const std::size_t discovered_count = plan.nodes.size();
    std::vector<RelayEndpoint> assigned_reversed;
    assigned_reversed.reserve(discovered_count);

    // Work backward so an upstream receiver is only given a downstream
    // receiver that has already accepted its assignment.
    for (auto it = plan.nodes.rbegin(); it != plan.nodes.rend(); ++it) {
        const RelayEndpoint* downstream = assigned_reversed.empty() ? nullptr : &assigned_reversed.back();

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

bool RelayCoordinator::start_rejoin_listener(std::shared_ptr<RelayMembership> membership) {
    if (rejoin_listener_ || !membership) return false;
    auto listener = std::make_unique<RelayRejoinListener>(std::move(membership));
    if (!listener->start()) return false;
    rejoin_listener_ = std::move(listener);
    return true;
}

uint16_t RelayCoordinator::rejoin_port() const {
    return rejoin_listener_ ? rejoin_listener_->port() : 0;
}

// Case explanation
/**
 * R3 misses a heartbeat. The coordinator tries to tell it: “Please request to rejoin.”
 * If R3 is still running, the message may reach it.
 * If R3 crashed, there is nobody to receive it. The 500 ms limit lets the coordinator stop trying quickly.
 */

void RelayCoordinator::send_rejoin_hints(
    const std::vector<RelayEndpoint>& missing,
    const std::string& transfer_id,
    uint64_t epoch
) const {
    if (missing.empty()) return;
    ThreadPool pool(missing.size(), missing.size());
    for (const auto& node : missing) {
        pool.enqueue([node, transfer_id, epoch] {
            Socket socket;
            if (!socket.create() ||
                !socket.relay_connect(node.address, node.port, std::chrono::milliseconds(500))) return;

            socket.relay_set_send_timeout(std::chrono::milliseconds(500));


            Connection connection(std::move(socket));
            
            RelayConnection relay(connection);
            RelayRejoinMessage hint;
            hint.transfer_id = transfer_id;
            hint.epoch = epoch;
            if (relay.send_rejoin_hint(hint)) {
                LOG_INFO("[%s:%u] Asked missed node to request relay rejoin",
                         node.address.c_str(), node.port);
            }
        });
    }
    pool.stop(true);
}

bool RelayCoordinator::start_heartbeat_poll(
    std::shared_ptr<RelayMembership> membership, // active nodes, transfer id, epoch, recorded replicas
    HeartbeatCheck on_check // A callback check after two seconds reply deadline with a RelayHealthCheck result
) const {
    if (!membership || !on_check) {
        throw std::invalid_argument("Relay membership and heartbeat check are required");
    }
    const auto poll = membership->start_heartbeat(RelayMembership::Clock::now());
    if (!poll.started) return false;

    // Heartbeat replies update membership as soon as each receiver answers.
    // Network I/O runs separately from both streaming and the timer event loop.
    try {
        std::thread([membership, poll] {
            try {
                RelayHeartbeatPoller poller;

                // sends the heart beat request to the membership nodes
                poller.run(poll, [membership, poll](const RelayHeartbeatReply& reply) { 
                    membership->heartbeat_reply(poll.transfer_id, poll.epoch, reply.endpoint, reply.received_at);
                });
            } catch (...) {
                // The timer callback will treat unresponsive nodes as missing.
            }
        }).detach();
    } catch (...) {
        // The timer callback will treat all nodes as missing.
    }

    // At the shared deadline, use the replies and pending rejoins collected
    // during this round. Replies arriving afterward are ignored by membership.
    RelayTimer::schedule(poll.deadline, [membership, poll, on_check = std::move(on_check)] {
        on_check(membership->finish_heartbeat(
            std::max(RelayMembership::Clock::now(), poll.deadline)
        ));
    });
    return true;
}

} // namespace flashshare
