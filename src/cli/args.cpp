#include "cli/args.h"

#include <cstdio>
#include <cstring>
#include <cstdlib>

namespace flashshare {

static const char* VERSION_STR = "flashshare 1.0.0";

static void print_help() {
    printf(
        "FlashShare — High-speed LAN file transfer\n"
        "\n"
        "USAGE:\n"
        "  flashshare send <path> [<path>...] --to <ip> [options]   Send files to a peer\n"
        "  flashshare recv [options]                      Receive files from a peer\n"
        "\n"
        "COMMANDS:\n"
        "  send <path> [<path>...]   Send one or more files and/or directories\n"
        "  recv           Listen for incoming transfers\n"
        "  help            Show this help message\n"
        "  version         Show version\n"
        "\n"
        "OPTIONS:\n"
        "  --to <ip>       Target peer IP address (for send)\n"
        "  --port <n>      Port number (default: 5117)\n"
        "  --out <dir>     Output directory (default: current dir)\n"
        "  --encrypt       Enable AES-256-GCM encryption\n"
        "  --resume        Resume an interrupted transfer\n"
        "  --accept-all    Auto-accept incoming transfers (no prompt)\n"
        "  --daemon        Run receiver in background (daemon mode)\n"
        "  --verbose       Enable debug logging\n"
        "  --log-file <f>  Mirror log output to file (default in --daemon mode:\n"
        "                  /tmp/flashshare_receiver.log)\n"
        "  -h, --help      Show help\n"
        "  -v, --version   Show version\n"
        "\n"
        "EXAMPLES:\n"
        "  flashshare send bigfile.iso --to 192.168.1.50\n"
        "  flashshare send ./project/ --to 192.168.1.50 --encrypt\n"
        "  flashshare send a.txt b.txt ./project/ --to 192.168.1.50\n"
        "  flashshare recv --port 5117 --out ~/downloads\n"
        "\n"
    );
}

void print_usage() {
    print_help();
}

void print_version() {
    printf("%s\n", VERSION_STR);
}

bool parse_args(int argc, char* argv[], Args& args) {
    if (argc < 2) {
        print_help();
        return false;
    }

    std::string cmd = argv[1];

    if (cmd == "-h" || cmd == "--help" || cmd == "help") {
        args.command = Command::HELP;
        print_help();
        return true;
    }

    if (cmd == "-v" || cmd == "--version" || cmd == "version") {
        args.command = Command::VERSION;
        print_version();
        return true;
    }

    if (cmd == "send") {
        args.command = Command::SEND;
    } else if (cmd == "recv") {
        args.command = Command::RECV;
    } else {
        fprintf(stderr, "Unknown command: %s\n\n", cmd.c_str());
        print_help();
        return false;
    }

    // Parse remaining args
    for (int i = 2; i < argc; ++i) {
        std::string arg = argv[i];

        if (arg == "--to" && i + 1 < argc) {
            args.to = argv[++i];
        } else if (arg == "--port" && i + 1 < argc) {
            args.port = static_cast<uint16_t>(atoi(argv[++i]));
        } else if (arg == "--out" && i + 1 < argc) {
            args.out_dir = argv[++i];
        } else if (arg == "--encrypt") {
            args.encrypt = true;
        } else if (arg == "--resume") {
            args.resume = true;
        } else if (arg == "--accept-all") {
            args.accept_all = true;
        } else if (arg == "--verbose") {
            args.verbose = true;
        } else if (arg == "--log-file" && i + 1 < argc) {
            args.log_file = argv[++i];
        } else if (arg == "--daemon") {
            args.daemon = true;
        } else if (arg == "-h" || arg == "--help") {
            print_help();
            return false;
        } else if (arg[0] == '-') {
            fprintf(stderr, "Unknown option: %s\n", arg.c_str());
            return false;
        } else {
            // Positional argument
            if (args.command == Command::SEND) {
                args.paths.push_back(arg);
            } else {
                args.extra.push_back(arg);
            }
        }
    }

    // Validate
    if (args.command == Command::SEND && args.paths.empty()) {
        fprintf(stderr, "Error: send requires at least one file or directory path\n");
        print_help();
        return false;
    }

    if (args.command == Command::SEND && args.to.empty()) {
        fprintf(stderr, "Error: send requires --to <ip>\n");
        print_help();
        return false;
    }

    return true;
}

} // namespace flashshare
