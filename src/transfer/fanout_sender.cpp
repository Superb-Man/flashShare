#include "transfer/fanout_sender.h"

#include "transfer/sender.h"
#include "util/logger.h"

#include <algorithm>
#include <chrono>
#include <exception>
#include <memory>
#include <stdexcept>
#include <unordered_set>

namespace flashshare {

FanoutSender::FanoutSender(
    const std::vector<std::string>& paths,
    const std::vector<std::string>& targets,
    std::uint16_t port,
    bool encrypt,
    bool resume,
    std::size_t max_parallel)
    : paths_(paths),
      targets_(targets),
      port_(port),
      encrypt_(encrypt),
      resume_(resume),
      max_parallel_(max_parallel) {}

bool FanoutSender::prepare_manifest() {
    try {
        manifest_ = flashshare::prepare_manifest(paths_);
        return true;
    } catch (const std::exception& error) {
        LOG_ERROR("Cannot prepare fan-out manifest: %s", error.what());
    } catch (...) {
        LOG_ERROR("Unexpected failure preparing fan-out manifest");
    }

    manifest_.reset();
    return false;
}

bool FanoutSender::send_to_target(std::size_t target_index) {
    // results_ is fully allocated before workers start.
    // Only this worker writes to this entry.
    TargetResult& result = results_[target_index];

    try {
        Sender sender(
            manifest_,
            targets_[target_index], // Target address
            port_,
            encrypt_,
            resume_,
            false
        ); 

        const int exit_code = sender.run();

        result.target_id = sender.transfer_id();
        result.error_message = sender.last_error();

        if (exit_code != 0 && result.error_message.empty()) {
            result.error_message = "Sender failed without an error message";
        }

        LOG_INFO(
            "[%s:%u] %s: %llu payload bytes, %zu retries, %.2fs",
            result.target.c_str(),
            port_,
            exit_code == 0 ? "SENT" : "FAILED",
            static_cast<unsigned long long>(sender.bytes_sent()),
            sender.retry_count(),
            sender.elapsed_time()
        );

        result.success = (exit_code == 0);
        return result.success;
    } catch (const std::exception& error) {
        result.success = false;
        result.error_message = error.what();
    } catch (...) {
        result.success = false;
        result.error_message = "Unexpected destination worker failure";
    }

    LOG_ERROR("[%s:%u] %s", result.target.c_str(), port_, result.error_message.c_str()); // Disable overlapping animated progress bars.

    return false;
}

void FanoutSender::print_summary() const {
    const auto successful = static_cast<std::size_t>(
        std::count_if(
            results_.begin(),
            results_.end(),
            [](const TargetResult& result) {
                return result.success;
            }));

    LOG_INFO("Fan-out sender results: %zu/%zu sent, %.2fs elapsed", successful, results_.size(), elapsed_time_);

    for (const auto& result : results_) {
        if (result.success) {
            LOG_INFO("[%s:%u] SENT, transfer %s", result.target.c_str(), port_, result.target_id.c_str());
        } else {
            LOG_ERROR("[%s:%u] FAILED: %s", result.target.c_str(), port_, result.error_message.c_str());
        }
    }
}

int FanoutSender::run() {
    const auto started = std::chrono::steady_clock::now();

    elapsed_time_ = 0.0;
    results_.clear();
    manifest_.reset();

    // Also used after preparation or scheduling errors.
    // Any already-queued transfers finish before their state is read.
    auto finish = [&](int exit_code) {
        if (thread_pool_) {
            thread_pool_->stop(true);
            thread_pool_.reset();
        }

        elapsed_time_ = std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();

        if (!results_.empty()) {
            print_summary();
        }

        return exit_code;
    };

    try {
        if (targets_.empty()) {
            throw std::runtime_error("No fan-out destinations provided");
        }

        if (max_parallel_ == 0) {
            throw std::runtime_error(
                "Fan-out concurrency must be at least one");
        }

        if (port_ == 0) {
            throw std::runtime_error("Receiver port must be nonzero");
        }

        // to avoid sending duplicate copies.
        std::unordered_set<std::string> unique_targets;

        for (const auto& target : targets_) {
            if (target.empty()) {
                throw std::runtime_error("Empty fan-out destination");
            }

            if (!unique_targets.insert(target).second) {
                throw std::runtime_error("Duplicate fan-out destination: " + target);
            }
        }

        // No resizing or structural changes after workers start.
        results_.resize(targets_.size());

        for (std::size_t i = 0; i < targets_.size(); ++i) {
            results_[i].target = targets_[i];
            results_[i].error_message = "Transfer was not started";
        }

        if (!prepare_manifest()) {
            for (auto& result : results_) {
                result.error_message = "Source preparation failed";
            }

            return finish(1);
        }

        const std::size_t worker_count = std::min(max_parallel_, targets_.size());

        // Queue capacity fits this operation's fixed destination list.
        // Active transfers remain limited by worker_count.
        thread_pool_ = std::make_unique<ThreadPool>(worker_count, targets_.size());

        LOG_INFO("Starting fan-out to %zu receivers with %zu workers", targets_.size(), worker_count);

        for (std::size_t i = 0; i < targets_.size(); ++i) {
            // Capture the index by value.
            const bool queued = thread_pool_->enqueue([this, i] {
                send_to_target(i);
            });

            if (!queued) {
                // No worker owns this entry when enqueue returns false.
                results_[i].error_message = "Thread pool rejected the destination task";
            }
        }

        // Join before inspecting any worker-written result.
        thread_pool_->stop(true);
        thread_pool_.reset();

        const bool all_succeeded = std::all_of(
            results_.begin(),
            results_.end(),
            [](const TargetResult& result) {
                return result.success;
            }
        );

        return finish(all_succeeded ? 0 : 1);
    } catch (const std::exception& error) {
        LOG_ERROR("Fan-out coordinator failed: %s", error.what());
    } catch (...) {
        LOG_ERROR("Unexpected fan-out coordinator failure");
    }

    return finish(1);
}

} // namespace flashshare