#pragma once

#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>

// Runs one file's recovery checkpoints while its receiver continues reading.
// A checkpoint becomes durable only after the file sync and journal update
// succeed. The worker owns the partial-file descriptor and joins on exit.
namespace flashshare {

class TransferStore;
struct StoredTransfer;
struct StoredFileProgress;

class CheckpointWorker {
public:
    CheckpointWorker(int fd, TransferStore& store, const StoredTransfer& transfer, const StoredFileProgress& file);
    ~CheckpointWorker();

    CheckpointWorker(const CheckpointWorker&) = delete;
    CheckpointWorker& operator=(const CheckpointWorker&) = delete;

    bool request(uint64_t offset);
    bool wait_for(uint64_t offset);
    uint64_t durable() const;

private:
    void run();

    int fd_;
    TransferStore& store_;
    std::string sender_ip_;
    std::string transfer_id_;
    uint32_t file_index_;
    std::string partial_path_;
    mutable std::mutex mutex_;
    std::condition_variable changed_;
    std::thread worker_;
    uint64_t durable_;
    uint64_t pending_;
    bool stopping_ = false;
    bool failed_ = false;
};

} // namespace flashshare
