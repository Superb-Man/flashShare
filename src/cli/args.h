#pragma once

#include <string>
#include <vector>
#include <cstdint>

namespace flashshare {

enum class Command {
    NONE,
    SEND,
    RECV,
    HELP,
    VERSION
};

struct Args {
    Command command = Command::NONE;
    std::string filepath;          // file/dir to send
    std::string to;                // target peer IP or hostname
    uint16_t port = 5117;          // port
    std::string out_dir = ".";     // output directory for receive
    bool encrypt = false;          // enable encryption
    bool resume = false;           // resume interrupted transfer
    bool accept_all = false;       // auto-accept incoming transfers
    bool verbose = false;          // debug logging
    bool daemon = false;           // run receiver as background daemon
    std::vector<std::string> extra; // extra positional args
};

// Parse command-line arguments
bool parse_args(int argc, char* argv[], Args& args);

// Print usage
void print_usage();

// Print version
void print_version();

} // namespace flashshare
