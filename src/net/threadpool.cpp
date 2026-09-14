#include "net/threadpool.h"

#include <algorithm>

namespace flashshare {

namespace {

size_t default_worker_count() {
    const size_t hardware_threads = std::thread::hardware_concurrency();
    // A receiver is I/O-bound, but an unbounded number of workers is unsafe.
    return std::max<size_t>(2, std::min<size_t>(8, hardware_threads == 0 ? 2 : hardware_threads));
}

} // namespace

ThreadPool::ThreadPool(size_t worker_count, size_t max_queue_size)
    : max_queue_size_(std::max<size_t>(1, max_queue_size)) {
    if (worker_count == 0) {
        worker_count = default_worker_count();
    }

    workers_.reserve(worker_count);
    try {
        for (size_t i = 0; i < worker_count; ++i) {
            workers_.emplace_back(&ThreadPool::worker_loop, this);
        }
    } catch(...) {
        stop();
        throw;
    }

}

ThreadPool::~ThreadPool() {
    stop(false);
}

bool ThreadPool::enqueue(Task task) {
    if (!task) {
        return false;
    }

    {
        std::unique_lock<std::mutex> lock(mutex_);
        if (stopping_ || tasks_.size() >= max_queue_size_) {
            return false;
        }
        tasks_.emplace(std::move(task));
    }
    work_available_.notify_one();
    return true;
}

void ThreadPool::stop(bool drain) {
    {
        std::unique_lock<std::mutex> lock(mutex_);
        if (stopping_) {
            return;
        }
        stopping_ = true;
        drain_ = drain;
        if (!drain_) {
            std::queue<Task> empty;
            tasks_.swap(empty);
        }
    }

    work_available_.notify_all();

    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
}

void ThreadPool::worker_loop() {
    for(;;) {
        Task task;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            work_available_.wait(lock, [this] {
                return stopping_ || !tasks_.empty(); // sleep untill threads are not stopping and no tasks
            });

            if (stopping_ && (!drain_ || tasks_.empty())) {
                return;
            }

            task = std::move(tasks_.front());
            tasks_.pop();
        }

        // A bad connection must not permanently remove a worker from the pool.
        try {
            task();
        } catch (...) {
        }
    }
}

} // namespace flashshare
