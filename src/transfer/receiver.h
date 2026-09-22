#pragma once

#include "net/connection.h"
#include "net/relay_connection.h"
#include "net/threadpool.h"
#include "cli/progress.h"

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

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

    struct RelayAssignment {
        std::string coordinator_ip;
        std::string discovery_id;
        uint64_t total_size = 0;
        uint32_t file_count = 0;
        bool has_downstream = false;
        std::string downstream_address;
        uint16_t downstream_port = DEFAULT_PORT;
    };

    std::mutex relay_assignments_mutex_;
    std::unordered_map<std::string, RelayAssignment> relay_assignments_;

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

    // This is post relay discovery action
    int handle_relay_assignment(
        Connection& conn,
        RelayConnection& relay_connection,
        const RelayAssignmentRequest& request
    );
    bool store_relay_assignment(
        const std::string& coordinator_ip,
        const RelayAssignmentRequest& request,
        std::string& rejection_reason
    );

    /*
     * Receives only the missing range of one file into its .part file.
     *
     * file_progress.durable_bytes is the receiver-confirmed resume offset.
     * It is checkpointed as data is safely written. This function must never
     * truncate the partial file during a resume.
     */
    bool recv_file(Connection& conn, StoredTransfer& transfer, StoredFileProgress& file_progress, bool encrypted, ProgressBar& progress);
    bool prompt_accept(const std::string& sender_ip, const TransferRequest& req);

    bool daemonize();
    bool write_pid_file();
    void remove_pid_file();
    std::string pid_file_path() const;
};

} // namespace flashshare
