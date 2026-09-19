#pragma once

#include <vector>
#include <string>
#include "prepared_manifest.h"
#include "net/threadpool.h"

namespace flashshare {

struct TargetResult {
    std::string target;
    bool success = false;
    std::string error_message;
    std::string target_id;
};

class FanoutSender {
public:
    FanoutSender(
        const std::vector<std::string>& paths,
        const std::vector<std::string>& targets,
        std::uint16_t port,
        bool encrypt,
        bool resume,
        std::size_t max_parallel = 4);

    int run();

private:
    bool prepare_manifest();
    bool send_to_target(std::size_t target_index);
    void print_summary() const;
    std::vector<TargetResult> results_;
    double elapsed_time_ = 0.0;
    std::vector<std::string> paths_;
    std::vector<std::string> targets_;
    std::uint16_t port_;
    bool encrypt_;
    bool resume_;
    std::size_t max_parallel_;
    std::shared_ptr<const PreparedManifest> manifest_;
    std::unique_ptr<ThreadPool> thread_pool_;
};

} // namespace flashshare