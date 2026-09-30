#include "transfer/relay_timer.h"
#include "net/threadpool.h"

#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <fcntl.h>
#include <functional>
#include <mutex>
#include <queue>
#include <sys/select.h>
#include <sys/time.h>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

/***
 * Author - Kazi Istiak Toriqe
 * Simple Timer Dispatcher implementation using timer fd
 */

namespace flashshare {

// The signal handler writes to this file descriptor. It does not run callbacks.
volatile sig_atomic_t alarm_write_fd = -1;

/**
 * This is called by SIGALRM triggered from setitimer.
 * It writes to the file descriptor so the worker wakes from wait_for_event().
 */
void
alarm_handler(int) {
    const int saved_errno = errno;
    const int fd = alarm_write_fd;

    if (fd >= 0) {
        const uint8_t byte = 1;
        const ssize_t result = ::write(fd, &byte, sizeof(byte));
        (void)result;
    }

    errno = saved_errno;
}

/**
 * This is a timer Dispatcher.
 */
class SigAlarmDispatcher {
public:
    using Callback = std::function<void()>; // the function pointer/reference
    using Clock = RelayTimer::Clock;

    SigAlarmDispatcher(): thread_pool_(4) {
        if (::pipe(pipe_fds_) != 0) return;

        const int read_flags = ::fcntl(pipe_fds_[0], F_GETFL, 0);
        const int write_flags = ::fcntl(pipe_fds_[1], F_GETFL, 0);
        
        if (read_flags < 0 || write_flags < 0 ||
            ::fcntl(pipe_fds_[0], F_SETFL, read_flags | O_NONBLOCK) != 0 ||
            ::fcntl(pipe_fds_[1], F_SETFL, write_flags | O_NONBLOCK) != 0) return;

        struct sigaction action {}; // empty signal action
        action.sa_handler = alarm_handler; // registering the alarm handler
        ::sigemptyset(&action.sa_mask); // no extra signals blocked during SIGALRM handling
        action.sa_flags = SA_RESTART; // restart interrupted system calls when possible

        if (::sigaction(SIGALRM, &action, &previous_action_) != 0) return; // use nullptr for old action if not needed
        alarm_write_fd = pipe_fds_[1];

        bool expected = false;
        if (!running_.compare_exchange_strong(expected, true)) return;

        try {
            worker_ = std::thread(&SigAlarmDispatcher::worker_loop, this);
            ready_ = true;
        } catch (...) {
            expected = true;
            running_.compare_exchange_strong(expected, false);
            alarm_write_fd = -1;
            ::sigaction(SIGALRM, &previous_action_, nullptr);
        }
    }

    ~SigAlarmDispatcher() {
        stop();

        if (pipe_fds_[1] >= 0) ::close(pipe_fds_[1]);
        if (pipe_fds_[0] >= 0) ::close(pipe_fds_[0]);
    }

    bool
    schedule(Clock::time_point deadline, Callback callback) { // register a callback for a deadline
        if (!ready_ || !callback) return false;

        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!running_) return false;
            timers_.push({deadline, std::move(callback)});
        }

        wake_worker(); // this is for each schedule request: a new deadline may be earlier
        return true;
    }

private:
    struct Timer {
        Clock::time_point deadline;
        Callback callback;
    };

    struct TimerCompare {
        bool
        operator()(const Timer& a, const Timer& b) const {
            return a.deadline > b.deadline;
        }
    };

    void
    stop() {
        if (!ready_) return;

        bool expected = true;
        if (!running_.compare_exchange_strong(expected, false)) return;

        wake_worker();
        if (worker_.joinable()) worker_.join();

        struct itimerval stopped {};
        ::setitimer(ITIMER_REAL, &stopped, nullptr);
        alarm_write_fd = -1;
        ::sigaction(SIGALRM, &previous_action_, nullptr);
        thread_pool_.stop(true);
        ready_ = false;
    }

    void
    wake_worker() {
        const uint8_t byte = 1;
        // writes to the file descriptor
        // select() checks that the pipe has data through its read descriptor
        // then returns from wait_for_event() so the worker can drain the pipe
        const ssize_t result = ::write(pipe_fds_[1], &byte, sizeof(byte));
        (void)result; // A full nonblocking pipe is already readable.
    }

    void
    worker_loop() {
        while (running_) {
            wait_for_event(); // will sleep here until an event arrives
            if (!running_) break;

            drain_pipe();
            process_expired_timers(); // pop the expired timers
            arm_next_timer();
        }
    }

    /**
     * Create a set of file descriptors.
     * This waits for an event without consuming CPU in a busy loop.
     */
    void
    wait_for_event() {
        for (;;) {
            fd_set read_fds;
            FD_ZERO(&read_fds);
            FD_SET(pipe_fds_[0], &read_fds);

            const int result = ::select(pipe_fds_[0] + 1, &read_fds,
                                        nullptr, nullptr, nullptr);
            if (result >= 0 || errno != EINTR) return;
            // select() may change read_fds, so create it again after EINTR.
        }
    }

    void
    drain_pipe() {
        uint8_t buffer[64];
        while (::read(pipe_fds_[0], buffer, sizeof(buffer)) > 0) {
        }
    }

    void
    process_expired_timers() {
        for (;;) {
            Callback callback;

            {
                std::lock_guard<std::mutex> lock(mutex_);
                if (timers_.empty() || timers_.top().deadline > Clock::now()) {
                    return;
                }

                callback = std::move(timers_.top().callback); // invoke the scheduled function
                timers_.pop();
            }

            // A full pool must not silently lose a heartbeat deadline.
            if (!thread_pool_.enqueue(callback)) {
                try {
                    callback();
                } catch (...) {
                    // A callback must not stop the timer dispatcher.
                }
            }
        }
    }

    void
    arm_next_timer() {
        std::lock_guard<std::mutex> lock(mutex_);
        struct itimerval timer {};

        if (!timers_.empty()) {
            auto micros = std::chrono::ceil<std::chrono::microseconds>(
                timers_.top().deadline - Clock::now()
            ).count();

            if (micros <= 0) micros = 1;

            timer.it_value.tv_sec = micros / 1000000;
            timer.it_value.tv_usec = micros % 1000000;
        }

        // A zero timer disarms ITIMER_REAL when the queue is empty.
        ::setitimer(ITIMER_REAL, &timer, nullptr);
    }

    int pipe_fds_[2]{-1, -1}; // 0 for reading and 1 for writing
    struct sigaction previous_action_{};
    std::atomic<bool> running_{false};
    bool ready_ = false;
    std::thread worker_;
    std::mutex mutex_;
    std::priority_queue<Timer, std::vector<Timer>, TimerCompare> timers_;
    ThreadPool thread_pool_;
};

void
RelayTimer::schedule(Clock::time_point deadline, std::function<void()> callback) {
    static SigAlarmDispatcher dispatcher;
    if (dispatcher.schedule(deadline, callback)) return;

    // Keep the deadline usable if SIGALRM setup fails.
    std::thread([deadline, callback = std::move(callback)] {
        std::this_thread::sleep_until(deadline);
        try {
            callback();
        } catch (...) {
        }
    }).detach();
}

} // namespace flashshare
