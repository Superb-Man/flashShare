#include "transfer/checkpoint_worker.h"
#include "transfer/transfer_store.h"
#include "util/logger.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <system_error>

#ifdef _WIN32
#include <io.h>
#define fsync _commit
#else
#include <unistd.h>
#endif

// Performs file sync and journal persistence in order for one partial file.
// Repeated requests are coalesced, and the receiver can wait for the final
// durable offset before verifying or completing the file.

namespace flashshare {

CheckpointWorker::CheckpointWorker(int fd, TransferStore& store,
                                   const StoredTransfer& transfer,
                                   const StoredFileProgress& file
                                )
    : fd_(fd), store_(store), sender_ip_(transfer.sender_public_ip),
      transfer_id_(transfer.transfer_id), file_index_(file.file_index),
      partial_path_(file.partial_path), durable_(file.durable_bytes),
      pending_(file.durable_bytes) {
    try {
        worker_ = std::thread([this] { 
            run(); 
        });
    } catch (const std::system_error& error) {
        LOG_ERROR("Cannot start checkpoint worker: %s", error.what());
        failed_ = true;
    }
}

CheckpointWorker::~CheckpointWorker() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stopping_ = true;
    }
    changed_.notify_all();
    if (worker_.joinable()) worker_.join();
    ::close(fd_);
}

bool CheckpointWorker::request(uint64_t offset) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (failed_) return false;
        pending_ = std::max(pending_, offset);
    }
    changed_.notify_one();
    return true;
}

bool CheckpointWorker::wait_for(uint64_t offset) {
    if (!request(offset)) return false;
    std::unique_lock<std::mutex> lock(mutex_);

    changed_.wait(lock, [&] { 
        return failed_ || durable_ >= offset; 
    });

    return !failed_;
}

uint64_t CheckpointWorker::durable() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return durable_;
}

void CheckpointWorker::run() {
    for (;;) {
        std::unique_lock<std::mutex> lock(mutex_);
        
        changed_.wait(lock, [&] { 
            return stopping_ || pending_ > durable_; 
        });

        if (stopping_ && pending_ <= durable_) return;
        const uint64_t offset = pending_;
        lock.unlock();

        bool ok = ::fsync(fd_) == 0;
        if (!ok) {
            LOG_ERROR("Cannot sync partial file %s: %s",
                      partial_path_.c_str(), strerror(errno));
        } else {
            ok = store_.checkpoint(sender_ip_, transfer_id_, file_index_, offset);
        }

        lock.lock();
        if (!ok) failed_ = true;
        else durable_ = offset;
        lock.unlock();
        changed_.notify_all();
        if (!ok) return;
    }
}

} // namespace flashshare
