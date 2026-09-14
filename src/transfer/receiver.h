#pragma once

#include "net/connection.h"
#include "net/threadpool.h"
#include "cli/progress.h"

#include <atomic>
#include <memory>
#include <string>

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

    void handle_connection_thread(Socket client);
    int handle_connection(Connection& conn);

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