#pragma once

#include <string>
#include <vector>
#include <cstdint>

namespace flashshare {

enum class Command {
    NONE,
    SEND,
    RECV,
    RELAY,
    HELP,
    VERSION
};

struct Args {
    Command command = Command::NONE;
    std::vector<std::string> paths; // files/dirs to send (one or more)
    std::string to;                // target peer IP or hostname
    std::vector<std::string> targets; // list of target peers IPs or hostnames
    uint16_t port = 5117;          // port
    std::string next_host;         // next relay host
    uint16_t next_port = 5117;     // next relay port
    std::string out_dir = ".";     // output directory for receive
    bool encrypt = false;          // enable encryption
    bool resume = false;           // resume interrupted transfer
    bool accept_all = false;       // auto-accept incoming transfers
    bool verbose = false;          // debug logging
    bool daemon = false;           // run receiver as background daemon
    std::string log_file;          // mirror logs to this file (for debugging daemon mode)
    std::vector<std::string> extra; // extra positional args
};

// Parse command-line arguments
bool parse_args(int argc, char* argv[], Args& args);

// Print usage
void print_usage();

// Print version
void print_version();

} // namespace flashshare
