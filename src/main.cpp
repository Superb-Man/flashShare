#include "cli/args.h"
#include "util/logger.h"
#include "transfer/sender.h"
#include "transfer/receiver.h"
#include "net/socket.h"
#include <signal.h>

#include <cstdio>

using namespace flashshare;

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
            Sender sender(args.paths, args.to, args.port, args.encrypt, args.resume);
            return sender.run();
        }

        case Command::RECV: {
            Receiver receiver(args.port, args.out_dir, args.accept_all, args.daemon, args.log_file);
            return receiver.run();
        }
    }

    net_shutdown();
    return 0;
}
