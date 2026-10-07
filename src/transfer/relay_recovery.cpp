#include "transfer/relay_recovery.h"

#include "transfer/sender.h"
#include "util/logger.h"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <thread>
#include <utility>

// Runs the relay sender while health checks continue independently.
// When membership changes it stops data streaming, rebuilds and assigns a
// chain, then resumes the same transfer ID from saved offsets.
// The coordinator pauses the sender before changing assignments, then
// starts a new attempt against the rebuilt head node.

namespace flashshare {

namespace {

using Clock = RelayMembership::Clock;
constexpr auto HEARTBEAT_INTERVAL = std::chrono::seconds(6);

struct AttemptState {
    std::mutex mutex;
    std::condition_variable changed;
    bool sender_done = false;
    int sender_result = 1;
    std::optional<RelayHealthCheck> health;
};

} // namespace

/**
 * Simple Algorithm
 * 
 * 
 * initially active_nodes = relay_initial_nodes
 * pendingJoinReq = []
 * co-ordinator code -> HeartBeattimerInterruptCalled
 * def check() :
 *     broadcast(active_nodes, heart_beat)
 *     
 *     start timer of 2 seconds{
 *         recive heart_beat response
 *         check pendingRejoinReq // if any rejoin req comes to co-ordinator it will be added from different places. that's why //store the value here
 *     }
 *     
 *     if (no heart beat_response || no pendingJoinReq) close co-ordinator // for pendingJoinReq use stored previous value
 *     
 *     else {
 *         if heart_beat_responses == relay_nodes_inital.size {
 *             setHeartBeattimerInterrupt from now
 *             continue
 *         }
 *         else if heart_beat_responses == active_nodes and pendingJoinReq is empty { //// for pendingJoinReq use stored previous value
 *             setHeartBeattimerInterrupt from now
 *             continue
 *         }
 *         else {
 *             lock()
 *             active_node.remove(not_recivd_heart_beat) 
 *             add pendingJOinReq nodes to active_nodes // // pending join request can come
 *             pendingJoinReq.clear() // before starting this line another request can come, but it will be lost for next heart_beat check, if not guarded
 *             unlock() 
 *             block_transfer()
 *             add any rejoin request to pendingJoinReq
 *             startRealyBUilding like initial with active nodes
 *             ...
 *             ...
 *             relay Build done
 *             setHeartBeattimerInterrupt from now
 *         }
 *     }
 */

RelayRecovery::RelayRecovery(RelayCoordinator& coordinator,
                             std::shared_ptr<RelayMembership> membership,
                             RelayPlan plan)
    : coordinator_(coordinator),
      membership_(std::move(membership)),
      plan_(std::move(plan)) {}

bool RelayRecovery::rebuild(const RelayHealthCheck& check) {

    // A missed node that already requested rejoin does not need a hint.
    std::vector<RelayEndpoint> missed_without_request; 

    for (const auto& node : check.missing) {
        const bool already_joining = std::any_of(check.joining.begin(), check.joining.end(),
            [&](const RelayEndpoint& joining) {
                return joining.address == node.address && joining.port == node.port;
            });

        if (!already_joining) missed_without_request.push_back(node);
    }

    coordinator_.send_rejoin_hints(missed_without_request, plan_.transfer_id, plan_.epoch);

    // A rejoin received during discovery/assignment stays pending and cannot
    // enter this build. Retry with another epoch if that race changes the plan.
    for (unsigned int attempt = 0; attempt < 4; ++attempt) {
        std::vector<RelayEndpoint> candidates;
        for (const auto& node : membership_->active_nodes()) {
            if (membership_->can_join_current_build(node)) candidates.push_back(node);
        }

        if (candidates.empty()) {
            LOG_ERROR("No relay nodes remain available for rebuild");
            return false;
        }

        RelayPlan next;

        if (!coordinator_.build_plan(next, candidates)) return false;

        // Remember candidates that did not respond to discovery so we can ask them to request rejoin.
        std::vector<RelayEndpoint> excluded; 

        for (const auto& node : candidates) {
            const bool discovered = std::any_of(next.nodes.begin(), next.nodes.end(),
                [&](const RelayEndpoint& found) {
                    return found.address == node.address && found.port == node.port;
                });
            if (!discovered) excluded.push_back(node);
        }

        // A rejoin may arrive while discovery runs. Remove that node from
        // this plan; it must wait for the next build.

        next.nodes.erase(std::remove_if(next.nodes.begin(), next.nodes.end(),
            [this](const RelayEndpoint& node) {
                return !membership_->can_join_current_build(node);
            }), next.nodes.end());

        if (next.empty()) continue;

        const auto discovered = next.nodes;

        if (!coordinator_.assign_plan(next)) return false;
        for (const auto& node : discovered) {
            const bool assigned = std::any_of(next.nodes.begin(), next.nodes.end(),
                [&](const RelayEndpoint& found) {
                    return found.address == node.address && found.port == node.port;
                });
            if (!assigned) excluded.push_back(node);
        }

        coordinator_.send_rejoin_hints(excluded, plan_.transfer_id, plan_.epoch);

        // Activate the new epoch only after the chain has been assigned.
        if (membership_->activate(next, Clock::now())) {
            plan_ = std::move(next);
            LOG_INFO("Rebuilt relay transfer %s at epoch %llu with %zu node(s)",
                     plan_.transfer_id.c_str(),
                     static_cast<unsigned long long>(plan_.epoch),
                     plan_.nodes.size());
            return true;
        }
        LOG_WARN("Relay membership changed during assignment; rebuilding again");
    }
    return false;
}

int RelayRecovery::run() {
    unsigned int healthy_failures = 0;

    for (;;) {
        const RelayEndpoint head = plan_.nodes.front();
        
        auto sender = std::make_shared<Sender>(
            coordinator_.manifest(), head.address, head.port,
            false, true, true, plan_.transfer_id, true
        );

        auto state = std::make_shared<AttemptState>();
        
        // Run data transfer separately so heartbeat checks can continue
        // while Sender::run() is sending or waiting on the connection.
        std::thread send_worker([sender, state] {
            const int result = sender->run();
            {
                std::lock_guard<std::mutex> lock(state->mutex);
                state->sender_result = result;
                state->sender_done = true;
            }
            state->changed.notify_all();
        });

        bool sender_joined = false;
        bool sender_reported = false;
        bool heartbeat_running = false;
        auto next_heartbeat = Clock::now() + HEARTBEAT_INTERVAL;

        for (;;) {
            if (!heartbeat_running && Clock::now() >= next_heartbeat) {
                const bool started = coordinator_.start_heartbeat_poll(
                    membership_, [state](RelayHealthCheck check) {
                        {
                            std::lock_guard<std::mutex> lock(state->mutex);
                            state->health = std::move(check);
                        }
                        state->changed.notify_all();
                    });
                if (started) 
                    heartbeat_running = true;
                else 
                    next_heartbeat = Clock::now() + std::chrono::milliseconds(100);
            }

            std::optional<RelayHealthCheck> health;
            bool newly_finished = false;
            int sender_result = 1;
            {
                std::unique_lock<std::mutex> lock(state->mutex);

                // Wake when either a heartbeat decision arrives or the sender finishes.
                const auto ready = [&] {
                    return state->health.has_value() ||
                           (state->sender_done && !sender_reported);
                };

                if (heartbeat_running) 
                    state->changed.wait(lock, ready);
                else 
                    state->changed.wait_until(lock, next_heartbeat, ready);

                if (state->sender_done && !sender_reported) {
                    newly_finished = true;
                    sender_result = state->sender_result;
                    sender_reported = true;
                }

                if (state->health) {
                    health = std::move(state->health);
                    state->health.reset();
                }
            }

            if (newly_finished) {
                send_worker.join();
                sender_joined = true;
                if (sender_result == 0) return 0;
                LOG_WARN("Relay data connection failed; checking membership before resume");
            }

            if (!health) continue;
            heartbeat_running = false;

            if (!health->evaluated) {
                next_heartbeat = Clock::now() + std::chrono::milliseconds(100);
                continue;
            }

            if (health->close_coordinator) {
                LOG_ERROR("No relay node replied and no rejoin request is pending");
                sender->request_stop();
                
                if (!sender_joined) send_worker.join();
                
                return 1;
            }

            // Stop and join the old sender before changing the relay chain.
            if (health->rebuild) {
                LOG_WARN("Relay membership changed; pausing data before rebuild");
                sender->request_stop();
                
                if (!sender_joined) send_worker.join();
                if (!rebuild(*health)) return 1;

                healthy_failures = 0;
                break; // Start a new Sender at the rebuilt head.
            }

            // The chain is healthy, so retry the data connection with the same
            // transfer ID; stop after repeated failures.
            if (sender_joined) {
                if (++healthy_failures >= 5) {
                    LOG_ERROR("Relay sender failed repeatedly while every node was healthy");
                    return 1;
                }
                break; // Reconnect using the same chain and transfer ID.
            }
            next_heartbeat = Clock::now() + HEARTBEAT_INTERVAL;
        }
    }
}

} // namespace flashshare
