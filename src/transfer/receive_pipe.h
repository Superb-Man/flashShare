#pragma once

#include <cstddef>
#include <cstdint>
#include <condition_variable>
#include <mutex>
#include <sys/types.h>

// Linux pipe used by the unencrypted receiver. A relay duplicates incoming
// pipe buffers with tee(). The receive and forward threads then consume their
// own branches independently, without rereading the .part file.

namespace flashshare {

#ifdef __linux__
/**
 * A receive pipe used for live relay mode in the unencrypted receiver.
 * It allows incoming file data to be received directly into the file descriptor
 * while simultaneously supporting forwarding to another socket.
 */
class ReceivePipe {
public:
    explicit ReceivePipe(bool relay);
    ~ReceivePipe();

    ReceivePipe(const ReceivePipe&) = delete;
    ReceivePipe& operator=(const ReceivePipe&) = delete;

    bool valid() const { return valid_; }
    ssize_t receive_to_file(int socket_fd, int file_fd, size_t count);
    ssize_t forward_to_socket(int socket_fd, size_t count);
    void abort();

private:
    int receive_fds_[2]{-1, -1};         // Pipe for receiving data from the socket
    int forward_fds_[2]{-1, -1};         // Pipe for forwarding data to the socket
    bool relay_ = false;
    bool valid_ = false;
    uint32_t test_write_delay_us_ = 0;   // Test-only delay for writing to the .part file
    std::mutex mutex_;
    std::condition_variable changed_;    // Condition variable to signal changes in published or forwarded state
    uint64_t published_ = 0;
    uint64_t forwarded_ = 0;
    bool aborted_ = false;
};
#endif

} // namespace flashshare
