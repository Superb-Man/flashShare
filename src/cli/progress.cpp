/**
 * @file progress.cpp
 * @author Codex  Astra
 * 
 * @brief This is to seperate rendering on cli from data transfer path.
 * In multiclore multithreaded environments, this ensures that terminal rendering does not block data transfer operations.
 * @date 2026-10-31
 * 
 */


#include "cli/progress.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>
#include <sys/time.h>

#ifdef _WIN32
#include <io.h>
#define isatty _isatty
#define fileno _fileno
#else
#include <unistd.h>
#endif

// One renderer thread owns terminal output for all active progress bars.
// Transfer workers only update atomic byte counts, so drawing and flushing
// the terminal cannot pause a file read or write.

namespace flashshare {

struct ProgressState {
    ProgressState(std::string name, uint64_t size, uint64_t start)
        : label(std::move(name)), total(size), start_time_ms(start) {}

    mutable std::mutex label_mutex;
    std::string label;
    const uint64_t total;
    const uint64_t start_time_ms;
    std::atomic<uint64_t> current{0};
    std::atomic<bool> finished{false};
    std::atomic<bool> cancelled{false};
};

namespace {

uint64_t now_ms() {
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    return static_cast<uint64_t>(tv.tv_sec) * 1000 + tv.tv_usec / 1000;
}

struct ProgressSnapshot {
    std::string label;
    uint64_t total = 0;
    uint64_t current = 0;
    uint64_t start_time_ms = 0;
    bool finished = false;
};

ProgressSnapshot snapshot(const ProgressState& state) {
    ProgressSnapshot view;
    {
        std::lock_guard<std::mutex> lock(state.label_mutex);
        view.label = state.label;
    }
    view.total = state.total;
    view.start_time_ms = state.start_time_ms;
    view.finished = state.finished.load();
    view.current = view.finished ? view.total : state.current.load();
    return view;
}

std::string format_size(uint64_t bytes) {
    const char* units[] = {"B", "KB", "MB", "GB", "TB"};
    int unit = 0;
    double size = static_cast<double>(bytes);
    while (size >= 1024.0 && unit < 4) {
        size /= 1024.0;
        ++unit;
    }
    char buf[64];
    if (unit == 0) {
        snprintf(buf, sizeof(buf), "%llu %s",
                 static_cast<unsigned long long>(bytes), units[0]);
    } else {
        snprintf(buf, sizeof(buf), "%.1f %s", size, units[unit]);
    }
    return buf;
}

std::string format_speed(double bytes_per_sec) {
    const char* units[] = {"B/s", "KB/s", "MB/s", "GB/s", "TB/s"};
    int unit = 0;
    double speed = bytes_per_sec;
    while (speed >= 1024.0 && unit < 4) {
        speed /= 1024.0;
        ++unit;
    }
    char buf[64];
    snprintf(buf, sizeof(buf), "%.1f %s", speed, units[unit]);
    return buf;
}

void render(const ProgressSnapshot& view) {
    char label_buf[32];
    if (view.label.size() > 30) {
        snprintf(label_buf, sizeof(label_buf), "...%s",
                 view.label.c_str() + view.label.size() - 27);
    } else {
        snprintf(label_buf, sizeof(label_buf), "%s", view.label.c_str());
    }

    const double pct = view.total > 0
        ? static_cast<double>(view.current) / view.total * 100.0 : 0.0;
    const int filled = std::min(30, static_cast<int>(pct / 100.0 * 30));
    char numbers[64];
    snprintf(numbers, sizeof(numbers), "] %5.1f%% ", pct);

    char padded_label[31];
    snprintf(padded_label, sizeof(padded_label), "%-30s", label_buf);
    std::string line = isatty(fileno(stderr)) ? "\r\033[2K" : "\r";
    line += padded_label;
    line += "[";
    line.append(filled, '#');
    line.append(30 - filled, ' ');
    line += numbers;
    line += format_size(view.current) + " / " + format_size(view.total);

    const uint64_t elapsed = now_ms() - view.start_time_ms;
    if (elapsed > 0 && view.current > 0) {
        const double speed = static_cast<double>(view.current) / (elapsed / 1000.0);
        line += "  " + format_speed(speed);
        if (view.current < view.total && speed > 0) {
            const double remaining = (view.total - view.current) / speed;
            char eta[32];
            if (remaining < 60) snprintf(eta, sizeof(eta), " ETA %.0fs", remaining);
            else snprintf(eta, sizeof(eta), " ETA %.1fm", remaining / 60.0);
            line += eta;
        }
    }

    if (view.finished) line += '\n';
    fwrite(line.data(), 1, line.size(), stderr);
    fflush(stderr);
}

class ProgressRenderer {
public:
    ProgressRenderer() : worker_([this] { run(); }) {}

    ~ProgressRenderer() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stopping_ = true;
        }
        changed_.notify_one();
        worker_.join();
    }

    void add(std::shared_ptr<ProgressState> state) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            states_.push_back(std::move(state));
        }
        changed_.notify_one();
    }

    void wake() { changed_.notify_one(); }

private:
    void run() {
        std::unique_lock<std::mutex> lock(mutex_);
        for (;;) {
            if (states_.empty() && !stopping_) {
                changed_.wait(lock, [&] { return stopping_ || !states_.empty(); });
            } else {
                changed_.wait_for(lock, std::chrono::milliseconds(100));
            }
            const bool stopping = stopping_;
            const auto states = states_;
            lock.unlock();

            std::vector<std::shared_ptr<ProgressState>> completed;
            std::vector<ProgressSnapshot> active;
            bool rendered_final = false;
            for (const auto& state : states) {
                if (state->cancelled.load()) {
                    completed.push_back(state);
                    continue;
                }
                auto view = snapshot(*state);
                if (view.finished) {
                    render(view);
                    rendered_final = true;
                    completed.push_back(state);
                } else {
                    active.push_back(std::move(view));
                }
            }

            // The terminal has one live line. Rotate among concurrent files.
            if (!active.empty()) {
                render(active[next_active_++ % active.size()]);
            } else if (!completed.empty() && !rendered_final) {
                const char* clear = isatty(fileno(stderr)) ? "\r\033[2K\n" : "\n";
                fputs(clear, stderr);
                fflush(stderr);
            }

            lock.lock();
            states_.erase(std::remove_if(states_.begin(), states_.end(),
                [&](const auto& state) {
                    return std::find(completed.begin(), completed.end(), state)
                           != completed.end();
                }), states_.end());
            if (stopping) return;
        }
    }

    std::mutex mutex_;
    std::condition_variable changed_;
    std::vector<std::shared_ptr<ProgressState>> states_;
    size_t next_active_ = 0;
    bool stopping_ = false;
    std::thread worker_;
};

ProgressRenderer& renderer() {
    static ProgressRenderer instance;
    return instance;
}

} // namespace

ProgressBar::ProgressBar(const std::string& label, uint64_t total)
    : state_(std::make_shared<ProgressState>(label, total, now_ms())) {
    renderer().add(state_);
}

ProgressBar::~ProgressBar() {
    if (!state_->finished.load()) state_->cancelled.store(true);
    renderer().wake();
}

void ProgressBar::update(uint64_t current) {
    if (!state_->finished.load()) state_->current.store(current);
}

void ProgressBar::increment(uint64_t delta) {
    if (!state_->finished.load()) state_->current.fetch_add(delta);
}

void ProgressBar::finish() {
    state_->current.store(state_->total);
    state_->finished.store(true);
    renderer().wake();
}

void ProgressBar::set_label(const std::string& label) {
    std::lock_guard<std::mutex> lock(state_->label_mutex);
    state_->label = label;
}

} // namespace flashshare
