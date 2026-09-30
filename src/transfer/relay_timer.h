#pragma once

#include <chrono>
#include <functional>

namespace flashshare {

// Schedules a one-shot SIGALRM deadline without blocking its caller.
class RelayTimer {
public:
    using Clock = std::chrono::steady_clock;
    static void schedule(Clock::time_point deadline, std::function<void()> callback);
};

} // namespace flashshare
