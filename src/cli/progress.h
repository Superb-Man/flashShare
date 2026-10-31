#pragma once

#include <cstdint>
#include <memory>
#include <string>

namespace flashshare {

struct ProgressState;

/**
 * ANSI terminal progress bar with speed display.
 * Transfer workers publish byte counts; one renderer thread writes to stderr.
 */
class ProgressBar {
public:
    ProgressBar(const std::string& label, uint64_t total);
    ~ProgressBar();

    ProgressBar(const ProgressBar&) = delete;
    ProgressBar& operator=(const ProgressBar&) = delete;

    void update(uint64_t current);
    void increment(uint64_t delta);
    void finish();
    void set_label(const std::string& label);

private:
    std::shared_ptr<ProgressState> state_;
};

}
