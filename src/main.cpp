#include "cli/args.h"
#include "util/logger.h"
#include "transfer/sender.h"
#include "transfer/fanout_sender.h"
#include "transfer/receiver.h"
#include "transfer/relay_coordinator.h"
#include "transfer/relay_plan.h"
#include "net/socket.h"
#include <signal.h>

#include <cstdio>
#include <string>
#include <utility>
#include <vector>

using namespace flashshare;

// Builds and assigns a relay chain across the --to receivers, then streams the
// transfer into its head node. Every downstream hop is driven by the relay
// nodes themselves, so the sender only ever transmits one copy of the file.
int run_relay_send(const Args& args) {
    std::vector<RelayEndpoint> candidates;
    candidates.reserve(args.targets.size());

    for (const auto& target : args.targets) {
        RelayEndpoint endpoint;
        endpoint.address = target;
        endpoint.port = args.port;
        candidates.push_back(std::move(endpoint));
    }

    RelayCoordinator coordinator(args.paths, std::move(candidates));

    RelayPlan plan;
    if (!coordinator.build_plan(plan)) {
        LOG_ERROR("Cannot start relay transfer: %s",
                  coordinator.last_error().c_str());
        return 1;
    }

    if (!coordinator.assign_plan(plan)) {
        LOG_ERROR("Cannot start relay transfer: %s",
                  coordinator.last_error().c_str());
        return 1;
    }

    std::string chain;
    for (const auto& node : plan.nodes) {
        chain += " -> " + node.address;
    }
    LOG_INFO("Relay chain for transfer %s: sender%s",
             plan.transfer_id.c_str(), chain.c_str());

    const RelayEndpoint& head = plan.nodes.front();

    Sender sender(
        coordinator.manifest(),
        head.address,
        head.port,
        args.encrypt,
        args.resume,
        true,
        plan.transfer_id,
        plan.is_chain());

    return sender.run();
}


int main(int argc, char* argv[]) {
    // Initialize networking (Winsock on Windows, no-op on Linux)
    if (!net_init()) {
        return 1;
    }

#ifndef _WIN32
    // Do not let sendfile() terminate sender on a disconnected receiver.
    // It must return EPIPE so Sender can retry with --resume.
    signal(SIGPIPE, SIG_IGN);
#endif

    Args args;
    if (!parse_args(argc, argv, args)) {
        return 1;
    }

    // Set log level
    if (args.verbose) {
        Logger::set_level(LogLevel::DEBUG);
    }

    if (!args.log_file.empty()) {
        Logger::set_log_file(args.log_file);
    }

    switch (args.command) {
        case Command::NONE:
        case Command::HELP:
        case Command::VERSION:
            // Already handled in parse_args
            return 0;

        case Command::SEND: {
            if (args.targets.empty()) {
                LOG_ERROR("No receiver addresses provided");
                return 1;
            }

            if (args.relay) {
                return run_relay_send(args);
            }

            if (args.targets.size() == 1) {
                Sender sender(
                    args.paths,
                    args.targets.front(),
                    args.port,
                    args.encrypt,
                    args.resume);

                return sender.run();
            }

            FanoutSender sender(
                args.paths,
                args.targets,
                args.port,
                args.encrypt,
                args.resume
            );

            return sender.run();
        }

        case Command::RECV: {
            Receiver receiver(args.port, args.out_dir, args.accept_all, args.daemon, args.log_file);
            return receiver.run();
        }

        case Command::RELAY: {
            // Relay mode is handled within the send command
            LOG_ERROR("Relay mode should be used with the send command");
            return 1;
        }
    }

    net_shutdown();
    return 0;
}
