#include "cli/args.h"
#include "util/logger.h"
#include "transfer/sender.h"
#include "transfer/receiver.h"
#include "net/socket.h"

#include <cstdio>

using namespace flashshare;

int main(int argc, char* argv[]) {
    // Initialize networking (Winsock on Windows, no-op on Linux)
    if (!net_init()) {
        return 1;
    }

    Args args;
    if (!parse_args(argc, argv, args)) {
        return 1;
    }

    // Set log level
    if (args.verbose) {
        Logger::set_level(LogLevel::DEBUG);
    }

    switch (args.command) {
        case Command::NONE:
        case Command::HELP:
        case Command::VERSION:
            // Already handled in parse_args
            return 0;

        case Command::SEND: {
            Sender sender(args.filepath, args.to, args.port, args.encrypt, args.resume);
            return sender.run();
        }

        case Command::RECV: {
            Receiver receiver(args.port, args.out_dir, args.accept_all, args.daemon);
            return receiver.run();
        }
    }

    net_shutdown();
    return 0;
}
