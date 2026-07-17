#pragma once

#include <cstdint>
#include <string>

namespace flashshare {

/**
 * ANSI terminal progress bar with speed display.
 * Thread-safe update via atomic counters.
 */
class ProgressBar {
public:
    ProgressBar(const std::string& label, uint64_t total);

    void update(uint64_t current);
    void increment(uint64_t delta);
    void finish();
    void set_label(const std::string& label);

private:
    std::string label_;
    uint64_t total_;
    uint64_t current_;
    uint64_t start_time_ms_;
    bool finished_;

    void render();
    std::string format_speed(double bytes_per_sec);
    std::string format_size(uint64_t bytes);
};

}
