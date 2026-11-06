#pragma once

#include <cstddef>
#include <cstdint>
#include <condition_variable>
#include <mutex>
#include <sys/types.h>
#include <functional>
#include <thread>

// Linux pipes used by the unencrypted receiver. A relay tees incoming bytes
// to the downstream and queues the local branch for a separate .part writer.
// If the downstream pipe fills, forwarding continues from written .part bytes.

namespace flashshare {

#ifdef __linux__
/**
 * A receive pipe used for live relay mode in the unencrypted receiver.
 * It allows incoming file data to be received directly into the file descriptor
 * while simultaneously supporting forwarding to another socket.
 */
class ReceivePipe {
public:
    using WrittenCallback = std::function<bool(uint64_t)>;

    explicit ReceivePipe(bool relay);
    ~ReceivePipe();

    ReceivePipe(const ReceivePipe&) = delete;
    ReceivePipe& operator=(const ReceivePipe&) = delete;

    bool valid() const { return valid_; }
    ssize_t receive_to_file(int socket_fd, int file_fd, size_t count);
    ssize_t forward_to_socket(int socket_fd, size_t count);
    bool fallback_offset(uint64_t& offset);
    bool start_writer(int file_fd, WrittenCallback on_written);
    bool finish_writer();
    void abort();

private:
    void write_file();
    int receive_fds_[2]{-1, -1};         // Pipe for receiving data from the socket
    int forward_fds_[2]{-1, -1};         // Pipe for forwarding data to the socket
    int file_fds_[2]{-1, -1};
    int file_fd_ = -1; // owned by CheckpointWorker
    bool relay_ = false;
    bool valid_ = false;
    uint32_t test_write_delay_us_ = 0;   // Test-only delay for writing to the .part file
    std::thread writer_;
    WrittenCallback on_written_;
    uint64_t queued_ = 0;
    uint64_t written_ = 0;
    bool writer_closed_ = false;
    int writer_error_ = 0;
    std::mutex mutex_;
    std::condition_variable changed_;    // Condition variable to signal changes in published or forwarded state
    uint64_t published_ = 0;
    uint64_t forwarded_ = 0;
    uint64_t fallback_offset_ = 0;
    bool fallback_ = false;
    bool aborted_ = false;
};
#endif

} // namespace flashshare
