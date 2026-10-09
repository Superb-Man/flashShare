#pragma once

#include "net/connection.h"
#include "net/relay_connection.h"
#include "net/threadpool.h"
#include "transfer/relay_forwarder.h"
#include "cli/progress.h"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

// Declares receiver state and handlers for transfer and relay messages.
// Worker threads handle accepted connections while saved journals and relay
// assignments preserve recovery information across reconnects.
// It tracks accepted sockets for shutdown and retains assignment details
// needed to reconnect to the coordinator after restart.

namespace flashshare {

class TransferStore;
struct StoredTransfer;
struct StoredFileProgress;

/**
 * Receiver: accepts multiple sender connections and receives each connection
 * on a thread-pool worker. TransferStore persists per-file recovery state.
 */
class Receiver {
public:
    Receiver(uint16_t port, const std::string& out_dir, bool accept_all,
             bool daemon = false, const std::string& log_file = "");
    ~Receiver();

    int run();
    void stop();

private:
    uint16_t port_;
    std::string out_dir_;
    bool accept_all_;
    bool daemon_;
    std::string log_file_;
    std::atomic<bool> running_{false};

    std::unique_ptr<ThreadPool> thread_pool_;

    // For recovery
    std::unique_ptr<TransferStore> transfer_store_;

    // Accepted sockets remain here while queued or handled by a worker.
    // stop() shuts them all down so workers blocked in recv() can exit.
    std::mutex active_connections_mutex_;
    std::unordered_map<uint64_t, std::shared_ptr<Connection>> active_connections_;
    uint64_t next_connection_id_ = 1;


    // Active transfers is maintained for the case of network error
    std::mutex active_transfers_mutex_;
    std::unordered_map<std::string, Connection*> active_transfers_;

    struct RelayAssignment {
        std::string coordinator_ip;
        std::string receiver_ip;
        std::string discovery_id;
        uint64_t epoch = 0;
        uint64_t total_size = 0;
        uint32_t file_count = 0;
        uint16_t coordinator_rejoin_port = 0;
        bool has_downstream = false;
        std::string downstream_address;
        uint16_t downstream_port = DEFAULT_PORT;
    };

    std::string relay_assignment_path(const std::string& transfer_id) const;
    bool save_relay_assignment(const std::string& transfer_id,
                               const RelayAssignment& assignment) const;

    // For non-blocking relay transfer continuty
    void register_active_transfer(const std::string& transfer_id, Connection* connection);
    void unregister_active_transfer(const std::string& transfer_id);
    void preempt_active_transfer(const std::string& transfer_id);

    void load_relay_assignments();
    void remove_relay_assignment(const std::string& transfer_id) const;
    bool request_rejoin(const std::string& transfer_id,
                        const RelayAssignment& assignment) const;
    void queue_rejoin(const std::string& transfer_id,
                      const RelayAssignment& assignment);
    void rejoin_loop();

    std::mutex relay_assignments_mutex_;
    std::unordered_map<std::string, RelayAssignment> relay_assignments_;
    std::mutex rejoin_mutex_;
    std::condition_variable rejoin_changed_;
    std::unordered_map<std::string, RelayAssignment> rejoin_pending_;
    std::thread rejoin_worker_;

    // Returns the assignment stored for this transfer ID, if this node was
    // given one by a relay coordinator.
    bool find_relay_assignment(const std::string& transfer_id,
                               RelayAssignment& assignment);

    /*
     * Builds the forwarder for a transfer this node was assigned a downstream
     * for, and opens the downstream connection before accepting upstream. The
     * upstream manifest is forwarded under the same transfer ID, so
     * every node in the chain verifies against the original sender's hashes.
     *
     * Returns nullptr when this node is the final node
     */
    std::unique_ptr<RelayForwarder> start_relay_forwarder(
        const TransferRequest& request,
        const StoredTransfer& transfer,
        const std::string& upstream_ip,
        std::string& rejection_reason
    );

    uint64_t register_connection(std::shared_ptr<Connection> connection);
    void unregister_connection(uint64_t connection_id);
    void shutdown_active_connections();

    void handle_connection_thread(std::shared_ptr<Connection> connection,
                                  uint64_t connection_id);
    int handle_connection(Connection& conn);
    int handle_relay_discovery(
        Connection& conn,
        RelayConnection& relay_connection,
        const RelayDiscoveryRequest& request
    );

    int handle_relay_heartbeat(
        Connection& conn,
        RelayConnection& relay_connection,
        const RelayHeartbeatRequest& request
    );

    // This is post relay discovery action
    int handle_relay_assignment(
        Connection& conn,
        RelayConnection& relay_connection,
        const RelayAssignmentRequest& request
    );
    bool store_relay_assignment(
        const std::string& coordinator_ip,
        const std::string& receiver_ip,
        const RelayAssignmentRequest& request,
        std::string& rejection_reason
    );

    /*
     * Receives only the missing range of one file into its .part file.
     *
     * file_progress.durable_bytes is the receiver-confirmed resume offset.
     * It is checkpointed as data is safely written. This function must never
     * truncate the partial file during a resume.
     * 
     * IMPORTANT
     * 
     * forwarder is null unless this node is a mid-chain relay: a direct
     * transfer, a fan-out target, and the chain's final node all receive
     * without forwarding. 
     * 
     * When it is set, every written byte is published to
     * it so the forwarding thread can send that range onward immediately.
     */
    bool recv_file(Connection& conn, StoredTransfer& transfer, StoredFileProgress& file_progress, bool encrypted, ProgressBar& progress, RelayForwarder* forwarder);
    bool prompt_accept(const std::string& sender_ip, const TransferRequest& req);

    bool daemonize();
    bool write_pid_file();
    void remove_pid_file();
    std::string pid_file_path() const;
};

} // namespace flashshare
