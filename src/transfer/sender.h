#pragma once

#include "net/connection.h"
#include "cli/progress.h"
#include <string>
#include <vector>
#include <atomic>

namespace flashshare {

/**
 * Sender: connects to a receiver and sends files using zero-copy sendfile.
 */
class Sender {
public:
    Sender(const std::vector<std::string>& paths, const std::string& target_ip,
           uint16_t port, bool encrypt, bool resume);

    // Run the send operation
    int run();

private:
    std::vector<std::string> paths_;
    std::string target_ip_;
    uint16_t port_;
    bool encrypt_;
    bool resume_;

    std::string generate_transfer_id();
    static uint64_t get_file_size(const std::string& path);
    static std::string basename(const std::string& path);

    // Builds the file manifest by walking every given path: a single entry
    // for a regular file, or the full recursive listing (with directory
    // structure preserved in relpath) for a directory. Entries from all
    // paths are merged into one manifest. Computes a SHA-256 hash for each
    // file so the receiver can verify integrity after transfer.
    std::vector<FileEntry> build_manifest();

    bool send_file(Connection& conn, const std::string& filepath,
                   uint32_t file_index, uint64_t file_size,
                   const std::string& filename, ProgressBar& progress);
};

} // namespace flashshare
