#pragma once

#include "net/connection.h"
#include "cli/progress.h"
#include <string>
#include <vector>
#include <atomic>
#include "transfer/prepared_manifest.h"

namespace flashshare {

/**
 * Sender: connects to a receiver and sends files using zero-copy sendfile.
 */
class Sender {
public:
    Sender(const std::vector<std::string>& paths, const std::string& target_ip,
           uint16_t port, bool encrypt, bool resume);

    // Fanout multiple connections for parallel file transfer
    Sender(std::shared_ptr<const PreparedManifest> manifest,
           const std::string& target_ip,
           uint16_t port,
           bool encrypt,
           bool resume,
           bool show_progress = false,
           std::string transfer_id = "");

    // Run the send operation
    int run();

    const std::string& transfer_id() const {
        return session_.transfer_id;
    }
    const std::string& last_error() const {
        return last_error_;
    }
    size_t retry_count() const {
        return retry_count_;
    }
    std::uint64_t bytes_sent() const {
        return bytes_sent_;
    }
    double elapsed_time() const {
        return elapsed_time_;
    }

private:
    struct TransferSession {
        std::string transfer_id; // remains unchanged for reconnect attempts
        // std::vector<FileEntry> files;
        // uint64_t total_size = 0;
        std::shared_ptr<const PreparedManifest> manifest; // shared read only metadata for the transfer
    };

    std::vector<std::string> paths_;
    std::string target_ip_;
    uint16_t port_;
    bool encrypt_;
    bool resume_;
    bool show_progress_ = true;
    std::string requested_transfer_id_;

    TransferSession session_;
    std::string last_error_;
    size_t retry_count_ = 0;
    std::uint64_t bytes_sent_ = 0;
    double elapsed_time_ = 0.0;

    // std::string generate_transfer_id();
    // static uint64_t get_file_size(const std::string& path);
    // static std::string basename(const std::string& path);

    // Builds the file manifest by walking every given path: a single entry
    // for a regular file, or the full recursive listing (with directory
    // structure preserved in relpath) for a directory. Entries from all
    // paths are merged into one manifest. Computes a SHA-256 hash for each
    // file so the receiver can verify integrity after transfer.
    std::vector<FileEntry> build_manifest();

    /*
     * Sends one file beginning at receiver_confirmed_offset.
     * The FILE_HEADER still contains the full file size; the receiver knows
     * where to append from its recovery journal.
     */
    bool send_file(Connection& conn,
                   const FileEntry& file,
                   uint32_t file_index,
                   uint64_t receiver_confirmed_offset,
                   ProgressBar* progress);

    /*
     * Creates a new TCP connection, resends the same transfer ID and manifest,
     * then obtains authoritative per-file offsets from the receiver.
     */
    bool connect_and_negotiate(const TransferSession& session,
                               std::unique_ptr<Connection>& conn,
                               std::vector<uint64_t>& resume_offsets);

    /*
     * One connection attempt. A broken connection returns false; run() then
     * reconnects using the same TransferSession / transfer_id.
     */
    bool send_attempt(const TransferSession& session,
                      Connection& conn,
                      const std::vector<uint64_t>& resume_offsets);

    bool wait_before_retry(unsigned int failed_attempt) const;
};

} // namespace flashshare
