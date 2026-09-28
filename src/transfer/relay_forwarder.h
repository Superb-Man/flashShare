#pragma once

#include "net/connection.h"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace flashshare {

struct RelayFileFlow {
    uint32_t file_index = 0;

    // The relpath from the original sender's manifest, not this node's
    // deduplicated final name. Forwarding the original keeps every node in
    // the chain verifying against the same manifest.
    std::string relpath;

    // Where this node's bytes are while they are still arriving.
    std::string partial_path;

    // Set only for a file this node finished during an earlier connection:
    // complete_file() renames the .part away, so the finished file is then
    // the only remaining source for it.
    std::string final_path;

    uint64_t expected_size = 0;

    // Bytes this node had already stored before the current connection began.
    uint64_t initial_available = 0;

    bool completed = false;
};


class RelayForwarder {
public:
    RelayForwarder(std::string downstream_address,
                   uint16_t downstream_port,
                   TransferRequest upstream_request,
                   std::vector<RelayFileFlow> files);
    ~RelayForwarder();

    RelayForwarder(const RelayForwarder&) = delete;
    RelayForwarder& operator=(const RelayForwarder&) = delete;

    // Connects downstream, replays the upstream manifest under the same
    // transfer ID to learn the downstream's own resume offsets
    // starts the forwarding thread.
    bool start();

    // Called by the receive thread after each successful write() to the
    // .part file of this file index. Never blocks on the forwarding thread.
    void publish(uint32_t file_index, uint64_t available);

    // Called only after this node has verified and finalized the file.
    void publish_verified(uint32_t file_index);

    // Marks the upstream transfer complete, then waits for the downstream
    // chain to acknowledge its verified completion.
    bool finish();

    // Wakes and stops the forwarding thread without draining.
    void abort();

    std::string last_error() const;
    uint64_t bytes_forwarded() const { return bytes_forwarded_.load(); }

private:
    void forward_loop();
    bool forward_file(const RelayFileFlow& file, uint64_t start_offset);

    // Waits until this file has more than `sent` bytes stored locally.
    // Returns false if the flow was aborted while waiting.
    bool wait_for_bytes(uint32_t file_index, uint64_t sent, uint64_t& available);
    bool wait_for_verified(uint32_t file_index);
    bool wait_for_upstream_complete();

    void fail(const std::string& reason);

    std::string downstream_address_;
    uint16_t downstream_port_;
    TransferRequest upstream_request_;
    std::vector<RelayFileFlow> files_;

    std::unique_ptr<Connection> downstream_;
    std::vector<uint64_t> downstream_offsets_;

    std::mutex mutex_;
    std::condition_variable data_available_;
    std::vector<uint64_t> available_; // guarded by mutex_
    std::vector<bool> verified_;       // guarded by mutex_
    bool upstream_complete_ = false;   // guarded by mutex_
    bool aborted_ = false;            // guarded by mutex_

    std::thread thread_;
    std::atomic<uint64_t> bytes_forwarded_{0};
    std::atomic<bool> failed_{false};

    mutable std::mutex error_mutex_;
    std::string last_error_;
};

} // namespace flashshare
