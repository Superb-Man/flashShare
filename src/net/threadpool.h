#pragma once

#include <cstddef>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace flashshare {

class ThreadPool {
public:
    using Task = std::function<void()>; // for simplicity of

    explicit ThreadPool(size_t worker_count = 0, size_t max_queue_size = 128);
    ~ThreadPool();

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;
    bool enqueue(Task task);

    // Stops accepting work and joins all workers. When drain is false, queued
    // work is discarded; tasks already running are allowed to finish.
    void stop(bool drain = true);

private:
    void worker_loop();

    std::vector<std::thread> workers_;
    std::queue<Task> tasks_;
    std::mutex mutex_;
    std::condition_variable work_available_;
    size_t max_queue_size_;
    bool stopping_ = false;
    bool drain_ = true;
};

} // namespace flashshare
