#pragma once

#include "net/connection.h"
#include "net/threadpool.h"
#include "cli/progress.h"
#include <string>
#include <atomic>
#include <memory>

namespace flashshare {

/**
 * Receiver: listens for incoming transfers and writes files to disk.
 * Supports concurrent transfers (one thread per connection) and daemon mode.
 */
class Receiver {
public:
    Receiver(uint16_t port, const std::string& out_dir, bool accept_all,
             bool daemon = false, const std::string& log_file = "");
    ~Receiver();

    // Start listening and handle transfers (blocks until stopped)
    int run();

    // Request graceful stop (signal-safe)
    void stop();

private:
    uint16_t port_;
    std::string out_dir_;
    bool accept_all_;
    bool daemon_;
    std::string log_file_;
    std::atomic<bool> running_{false};

    // Created after daemonization so fork() never occurs in a multithreaded process.
    std::unique_ptr<ThreadPool> thread_pool_;

    // Handle a single incoming connection (runs in its own thread)
    void handle_connection_thread(Socket client);

    // Handle a single incoming connection
    int handle_connection(Connection& conn);

    // Receive a single file
    bool recv_file(Connection& conn, const std::string& out_path,
                   uint64_t file_size, bool encrypted, ProgressBar& progress);

    // Prompt user to accept/reject
    bool prompt_accept(const std::string& sender_ip, const TransferRequest& req);

    // Daemonize the process (fork into background)
    bool daemonize();

    // Write PID file
    bool write_pid_file();
    void remove_pid_file();

    // Get PID file path
    std::string pid_file_path() const;
};

} // namespace flashshare
