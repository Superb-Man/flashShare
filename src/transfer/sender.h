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
    Sender(const std::string& filepath, const std::string& target_ip,
           uint16_t port, bool encrypt, bool resume);

    // Run the send operation
    int run();

private:
    std::string filepath_;
    std::string target_ip_;
    uint16_t port_;
    bool encrypt_;
    bool resume_;

    std::string generate_transfer_id();
    static uint64_t get_file_size(const std::string& path);
    static std::string basename(const std::string& path);
    bool send_file(Connection& conn, const std::string& filepath,
                   uint32_t file_index, uint64_t file_size,
                   const std::string& filename, ProgressBar& progress);
};

} // namespace flashshare
