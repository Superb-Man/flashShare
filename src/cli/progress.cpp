#include "cli/progress.h"

#include <cstdio>
#include <cstring>
#include <sys/time.h>
#include <cmath>

namespace flashshare {

static uint64_t now_ms() {
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    return static_cast<uint64_t>(tv.tv_sec) * 1000 + tv.tv_usec / 1000;
}

ProgressBar::ProgressBar(const std::string& label, uint64_t total)
    : label_(label), total_(total), current_(0), start_time_ms_(now_ms()), finished_(false) {
    render();
}

void ProgressBar::update(uint64_t current) {
    current_ = current;
    if (!finished_) render();
}

void ProgressBar::increment(uint64_t delta) {
    current_ += delta;
    if (!finished_) render();
}

void ProgressBar::finish() {
    finished_ = true;
    current_ = total_;
    render();
    fprintf(stderr, "\n");
}

void ProgressBar::set_label(const std::string& label) {
    label_ = label;
}

std::string ProgressBar::format_size(uint64_t bytes) {
    const char* units[] = {"B", "KB", "MB", "GB", "TB"};
    int unit = 0;
    double size = static_cast<double>(bytes);
    while (size >= 1024.0 && unit < 4) {
        size /= 1024.0;
        ++unit;
    }
    char buf[64];
    if (unit == 0) {
        snprintf(buf, sizeof(buf), "%llu %s", (unsigned long long)bytes, units[0]);
    } else {
        snprintf(buf, sizeof(buf), "%.1f %s", size, units[unit]);
    }
    return std::string(buf);
}

std::string ProgressBar::format_speed(double bytes_per_sec) {
    const char* units[] = {"B/s", "KB/s", "MB/s", "GB/s", "TB/s"};
    int unit = 0;
    double speed = bytes_per_sec;
    while (speed >= 1024.0 && unit < 4) {
        speed /= 1024.0;
        ++unit;
    }
    char buf[64];
    snprintf(buf, sizeof(buf), "%.1f %s", speed, units[unit]);
    return std::string(buf);
}

void ProgressBar::render() {
    // Carriage return to overwrite line
    fprintf(stderr, "\r");

    // Label (truncated to 30 chars)
    char label_buf[32];
    if (label_.size() > 30) {
        snprintf(label_buf, sizeof(label_buf), "...%s", label_.c_str() + label_.size() - 27);
    } else {
        snprintf(label_buf, sizeof(label_buf), "%s", label_.c_str());
    }

    // Progress percentage
    double pct = (total_ > 0) ? (static_cast<double>(current_) / static_cast<double>(total_) * 100.0) : 0.0;

    // Bar
    int bar_width = 30;
    int filled = static_cast<int>(pct / 100.0 * bar_width);
    if (filled > bar_width) filled = bar_width;

    fprintf(stderr, "%-30s [", label_buf);
    for (int i = 0; i < bar_width; ++i) {
        fputc(i < filled ? '#' : ' ', stderr);
    }
    fprintf(stderr, "] %5.1f%% ", pct);

    // Size info
    fprintf(stderr, "%s / %s", format_size(current_).c_str(), format_size(total_).c_str());

    // Speed
    uint64_t elapsed = now_ms() - start_time_ms_;
    if (elapsed > 0 && current_ > 0) {
        double speed = static_cast<double>(current_) / (elapsed / 1000.0);
        fprintf(stderr, "  %s", format_speed(speed).c_str());

        if (current_ < total_ && speed > 0) {
            double remaining = (total_ - current_) / speed;
            if (remaining < 60) {
                fprintf(stderr, " ETA %.0fs", remaining);
            } else {
                fprintf(stderr, " ETA %.1fm", remaining / 60.0);
            }
        }
    }

    fflush(stderr);
}

}
