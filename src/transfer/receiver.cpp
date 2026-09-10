#include "transfer/receiver.h"
#include "net/listener.h"
#include "util/logger.h"
#include "util/fs.h"
#include "util/sha256.h"

#include <sys/stat.h>
#include <fcntl.h>
#include <cstring>
#include <cstdio>
#include <algorithm>
#include <chrono>

#ifdef _WIN32
#include <io.h>
#define fsync _commit
typedef int mode_t;
#define S_ISREG(m) (((m) & 0170000) == 0100000)
#else
#include <unistd.h>
#include <signal.h>
#include <sys/wait.h>
#endif

namespace flashshare {

// Global receiver pointer for signal handler
static Receiver* g_receiver = nullptr;

#ifndef _WIN32
static void signal_handler(int sig) {
    if (g_receiver) {
        const char* msg = "\n[flashshare] Stopping receiver...\n";
        write(STDERR_FILENO, msg, strlen(msg));
        g_receiver->stop();
    }
}
#endif

Receiver::Receiver(uint16_t port, const std::string& out_dir, bool accept_all, bool daemon,
                    const std::string& log_file)
    : port_(port), out_dir_(out_dir), accept_all_(accept_all), daemon_(daemon), log_file_(log_file) {}

Receiver::~Receiver() {
    stop();
    // Join any remaining threads
    for (auto& t : threads_) {
        if (t.joinable()) t.join();
    }
    if (daemon_) {
        remove_pid_file();
    }
}

void Receiver::stop() {
    running_ = false;
}

std::string Receiver::pid_file_path() const {
    return "/tmp/flashshare_receiver.pid";
}

bool Receiver::write_pid_file() {
    FILE* f = fopen(pid_file_path().c_str(), "w");
    if (!f) return false;
    fprintf(f, "%d\n", static_cast<int>(getpid()));
    fclose(f);
    return true;
}

void Receiver::remove_pid_file() {
    unlink(pid_file_path().c_str());
}

bool Receiver::daemonize() {
#ifndef _WIN32
    // Fork once
    pid_t pid = fork();
    if (pid < 0) {
        LOG_ERROR("fork() failed: %s", strerror(errno));
        return false;
    }
    if (pid > 0) {
        // Parent exits
        exit(0);
    }

    // Child: become session leader
    if (setsid() < 0) {
        LOG_ERROR("setsid() failed: %s", strerror(errno));
        return false;
    }

    // Fork again (double fork to prevent reacquiring a terminal)
    pid = fork();
    if (pid < 0) {
        LOG_ERROR("second fork() failed: %s", strerror(errno));
        return false;
    }
    if (pid > 0) {
        exit(0);
    }

    // Set file permissions
    umask(0);

    // Change to root directory to avoid blocking filesystem
    chdir("/");

    // Close standard file descriptors (redirect to /dev/null)
    close(STDIN_FILENO);
    close(STDOUT_FILENO);
    close(STDERR_FILENO);
    int devnull = open("/dev/null", O_RDWR);
    if (devnull >= 0) {
        dup2(devnull, STDIN_FILENO);
        dup2(devnull, STDOUT_FILENO);
        dup2(devnull, STDERR_FILENO);
        if (devnull > 2) close(devnull);
    }

    // Write PID file
    write_pid_file();

    return true;
#else
    LOG_ERROR("Daemon mode not supported on Windows");
    return false;
#endif
}

bool Receiver::prompt_accept(const std::string& sender_ip, const TransferRequest& req) {
    if (accept_all_) {
        return true;
    }

    fprintf(stderr, "\n");
    fprintf(stderr, "Incoming transfer from %s\n", sender_ip.c_str());
    fprintf(stderr, "  Transfer ID: %s\n", req.transfer_id.c_str());
    fprintf(stderr, "  Files: %zu\n", req.files.size());
    fprintf(stderr, "  Total size: %llu bytes\n", (unsigned long long)req.total_size);
    fprintf(stderr, "  Encrypted: %s\n", req.encrypted ? "yes" : "no");

    for (const auto& f : req.files) {
        fprintf(stderr, "    %s (%llu bytes)\n", f.relpath.c_str(), (unsigned long long)f.size);
    }

    fprintf(stderr, "Accept? [y/N] ");
    fflush(stderr);

    char buf[16];
    if (!fgets(buf, sizeof(buf), stdin)) {
        return false;
    }
    return buf[0] == 'y' || buf[0] == 'Y';
}

bool Receiver::recv_file(Connection& conn, const std::string& out_path,
                          uint64_t file_size, bool encrypted, ProgressBar& progress) {
    int fd = ::open(out_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        LOG_ERROR("Cannot create output file: %s — %s", out_path.c_str(), strerror(errno));
        return false;
    }

    uint64_t received = 0;

    if (!encrypted) {
        constexpr size_t BUF_SIZE = 256 * 1024;
        std::vector<uint8_t> buffer(BUF_SIZE);

        while (received < file_size) {
            size_t to_read = BUF_SIZE;
            if (file_size - received < BUF_SIZE) {
                to_read = static_cast<size_t>(file_size - received);
            }

            ssize_t n = conn.socket().recv(buffer.data(), to_read, 0);
            if (n < 0) {
                if (errno == EINTR) continue;
                LOG_ERROR("recv failed: %s", strerror(errno));
                ::close(fd);
                return false;
            }
            if (n == 0) {
                LOG_ERROR("Connection closed during file transfer");
                ::close(fd);
                return false;
            }

            ssize_t written = 0;
            while (written < n) {
                ssize_t w = ::write(fd, buffer.data() + written, static_cast<size_t>(n) - written);
                if (w < 0) {
                    if (errno == EINTR) continue;
                    LOG_ERROR("write failed: %s", strerror(errno));
                    ::close(fd);
                    return false;
                }
                written += w;
            }

            received += static_cast<uint64_t>(n);
            progress.update(received);
        }
    } else {
        while (received < file_size) {
            std::vector<uint8_t> data;
            uint32_t seq = 0;

            if (!conn.recv_data_chunk(data, seq)) {
                LOG_ERROR("Failed to receive data chunk");
                ::close(fd);
                return false;
            }

            ssize_t n = ::write(fd, data.data(), data.size());
            if (n < 0 || static_cast<size_t>(n) != data.size()) {
                LOG_ERROR("write failed: %s", strerror(errno));
                ::close(fd);
                return false;
            }

            received += data.size();
            progress.update(received);
        }
    }

    if (fsync(fd) < 0) {
        LOG_WARN("fsync failed: %s", strerror(errno));
    }
    ::close(fd);

    MessageType type;
    if (!conn.recv_message(type) || type != MessageType::FILE_COMPLETE) {
        LOG_WARN("Expected FILE_COMPLETE, got %d", static_cast<int>(type));
    }

    return true;
}

int Receiver::handle_connection(Connection& conn) {
    TransferRequest req;
    if (!conn.recv_transfer_request(req)) {
        LOG_ERROR("Failed to receive transfer request");
        return 1;
    }

    std::string sender_ip = conn.socket().peer_address();

    // Manifest relpaths come from the remote peer — reject anything that
    // could escape out_dir_ (absolute paths or ".." segments) before
    // accepting the transfer.
    for (const auto& f : req.files) {
        if (!fs_is_safe_relpath(f.relpath)) {
            LOG_ERROR("Rejecting transfer %s — unsafe path in manifest: %s",
                      req.transfer_id.c_str(), f.relpath.c_str());
            TransferResponse resp;
            resp.accepted = false;
            conn.send_transfer_response(resp);
            return 1;
        }
    }

    if (!prompt_accept(sender_ip, req)) {
        LOG_INFO("Transfer rejected by user");
        TransferResponse resp;
        resp.accepted = false;
        conn.send_transfer_response(resp);
        return 1;
    }

    TransferResponse resp;
    resp.accepted = true;
    for (size_t i = 0; i < req.files.size(); ++i) {
        resp.resume_offsets.push_back(0);
    }

    if (!conn.send_transfer_response(resp)) {
        LOG_ERROR("Failed to send transfer response");
        return 1;
    }

    LOG_INFO("Receiving transfer %s — %zu files, %llu bytes",
             req.transfer_id.c_str(), req.files.size(), (unsigned long long)req.total_size);

    auto overall_start = std::chrono::steady_clock::now();
    uint64_t overall_received = 0;

    for (size_t i = 0; i < req.files.size(); ++i) {
        uint32_t file_index;
        uint64_t file_size;
        std::string filename;
        if (!conn.recv_file_header(file_index, file_size, filename)) {
            LOG_ERROR("Failed to receive file header");
            return 1;
        }

        if (!fs_is_safe_relpath(filename)) {
            LOG_ERROR("Rejecting unsafe file path in FILE_HEADER: %s", filename.c_str());
            return 1;
        }
        std::string out_path = fs_join(out_dir_, filename);

        if (!fs_ensure_parent_dirs(out_path)) {
            LOG_ERROR("Failed to create directories for: %s", out_path.c_str());
            return 1;
        }

        LOG_INFO("[%zu/%zu] Receiving: %s (%llu bytes) -> %s",
                 i + 1, req.files.size(), filename.c_str(),
                 (unsigned long long)file_size, out_path.c_str());

        ProgressBar progress(filename, file_size);

        if (!recv_file(conn, out_path, file_size, req.encrypted, progress)) {
            LOG_ERROR("Failed to receive file: %s", filename.c_str());
            return 1;
        }

        progress.finish();

        // Verify integrity against the sender's SHA-256 (sent in the manifest).
        const std::string& expected_hash = (i < req.files.size()) ? req.files[i].sha256 : std::string();
        if (!expected_hash.empty()) {
            std::string actual_hash = SHA256::file_hash(out_path);
            if (actual_hash == expected_hash) {
                LOG_DEBUG("Hash OK: %s (%s)", filename.c_str(), actual_hash.c_str());
            } else {
                LOG_ERROR("Hash MISMATCH for %s — expected %s, got %s",
                          filename.c_str(), expected_hash.c_str(), actual_hash.c_str());
                return 1;
            }
        } else {
            LOG_DEBUG("No hash in manifest for %s, skipping verification", filename.c_str());
        }

        overall_received += file_size;
        LOG_INFO("File received: %s", out_path.c_str());
    }

    double overall_elapsed = std::chrono::duration<double>(
        std::chrono::steady_clock::now() - overall_start).count();
    double avg_mbps = overall_elapsed > 0
        ? (static_cast<double>(overall_received) / overall_elapsed) / (1024.0 * 1024.0)
        : 0.0;
    LOG_INFO("Overall: %zu file(s), %llu bytes received in %.1fs (avg %.1f MB/s)",
             req.files.size(), (unsigned long long)overall_received, overall_elapsed, avg_mbps);

    MessageType type;
    if (conn.recv_message(type) && type == MessageType::TRANSFER_COMPLETE) {
        LOG_INFO("Transfer complete: %s", req.transfer_id.c_str());
    } else {
        LOG_WARN("Did not receive TRANSFER_COMPLETE");
    }

    return 0;
}

void Receiver::handle_connection_thread(Socket client) {
    Connection conn(std::move(client));
    conn.tune_socket();

    try {
        handle_connection(conn);
    } catch (const std::exception& e) {
        LOG_ERROR("Exception in transfer thread: %s", e.what());
    }
}

int Receiver::run() {
    LOG_INFO("FlashShare Receiver — listening on port %u", port_);
    LOG_INFO("Output directory: %s", out_dir_.c_str());

    // Ensure output directory exists
    struct stat st;
    if (stat(out_dir_.c_str(), &st) != 0) {
        LOG_ERROR("Output directory does not exist: %s", out_dir_.c_str());
        return 1;
    }

    // Daemonize if requested
    if (daemon_) {
        LOG_INFO("Starting in daemon mode...");

        // stderr gets redirected to /dev/null once daemonized, so mirror log
        // output to a file — otherwise all subsequent logging (including
        // errors) is silently lost. Preserve whatever level was already set
        // (e.g. --verbose) instead of resetting it.
        std::string effective_log_file = log_file_.empty() ? "/tmp/flashshare_receiver.log"
                                                             : log_file_;
        LogLevel level_before_daemonize = Logger::get_level();

        if (!daemonize()) {
            LOG_ERROR("Failed to daemonize");
            return 1;
        }

        // After daemonize, we're in the background child process with
        // stdin/stdout/stderr redirected to /dev/null.
        Logger::set_level(level_before_daemonize);
        if (Logger::set_log_file(effective_log_file)) {
            LOG_INFO("Daemon logging to %s", effective_log_file.c_str());
        }
    }

    // Set up signal handler for graceful shutdown
    g_receiver = this;
#ifndef _WIN32
    struct sigaction sa;
    sa.sa_handler = signal_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);
    // Ignore SIGPIPE — handle broken connections gracefully
    signal(SIGPIPE, SIG_IGN);
#endif

    Listener listener;
    if (!listener.start("", port_)) {
        LOG_ERROR("Failed to start listener");
        return 1;
    }

    running_ = true;
    LOG_INFO("Waiting for incoming connections... (Ctrl+C to stop)");

    // Main accept loop — handles concurrent transfers
    while (running_) {
        Socket client = listener.accept_one();
        if (!client.is_valid()) {
            if (running_) {
                LOG_WARN("Accept failed, continuing...");
            }
            continue;
        }

        LOG_INFO("Connection from %s:%u", client.peer_address().c_str(), client.peer_port());

        // Spawn a thread for this connection (concurrent transfers)
        threads_.emplace_back(&Receiver::handle_connection_thread, this, std::move(client));

        // Clean up finished threads periodically
        threads_.erase(
            std::remove_if(threads_.begin(), threads_.end(),
                [](std::thread& t) {
                    if (t.joinable()) {
                        // Try to join non-blocking — if thread is done, join it
                        // Otherwise skip
                        // We use a simple approach: detach threads instead
                        return false;
                    }
                    return true;
                }),
            threads_.end()
        );
    }

    // Detach all remaining threads on shutdown
    for (auto& t : threads_) {
        if (t.joinable()) t.detach();
    }
    threads_.clear();

    listener.stop();
    LOG_INFO("Receiver stopped.");

    if (daemon_) {
        remove_pid_file();
    }

    return 0;
}

} // namespace flashshare
