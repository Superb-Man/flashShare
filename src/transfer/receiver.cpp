#include "transfer/receiver.h"
#include "net/listener.h"
#include "util/logger.h"
#include "util/fs.h"
#include "util/sha256.h"
#include "transfer/transfer_store.h"

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
    if (thread_pool_) {
        thread_pool_->stop(false);
    }
    if (daemon_) {
        remove_pid_file();
    }
}

void Receiver::stop() {
    running_ = false;
    shutdown_active_connections();
}

uint64_t Receiver::register_connection(std::shared_ptr<Connection> connection) {
    std::lock_guard<std::mutex> lock(active_connections_mutex_);
    const uint64_t connection_id = next_connection_id_++;
    active_connections_.emplace(connection_id, std::move(connection));
    return connection_id;
}

void Receiver::unregister_connection(uint64_t connection_id) {
    std::lock_guard<std::mutex> lock(active_connections_mutex_);
    active_connections_.erase(connection_id);
}

void Receiver::shutdown_active_connections() {
    std::vector<std::shared_ptr<Connection>> connections;
    {
        std::lock_guard<std::mutex> lock(active_connections_mutex_);
        connections.reserve(active_connections_.size());
        for (const auto& entry : active_connections_) {
            connections.push_back(entry.second);
        }
    }

    for (const auto& connection : connections) {
        connection->socket().shutdown_both();
    }
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

bool Receiver::recv_file(Connection& conn, StoredTransfer& transfer,
                         StoredFileProgress& file,
                         bool encrypted,
                         ProgressBar& progress) {
    // A sender still sends FILE_COMPLETE for an already-complete file when it
    // reconnects. Consume it, but never recreate or truncate the final file.
    if (file.completed) {
        MessageType type;
        if (!conn.recv_message(type) || type != MessageType::FILE_COMPLETE) {
            LOG_ERROR("Expected FILE_COMPLETE for already completed file");
            return false;
        }
        return true;
    }

    // A new transfer has no partial-file directory yet.
    if (!fs_ensure_parent_dirs(file.partial_path)) {
        LOG_ERROR("Cannot create partial-file directory for %s", file.partial_path.c_str());
        return false;
    }

    int fd = ::open(file.partial_path.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
        LOG_ERROR("Cannot open partial file %s: %s",
                  file.partial_path.c_str(), strerror(errno));
        return false;
    }

    uint64_t received = file.durable_bytes;

    // Any bytes after the last journal checkpoint were not confirmed durable.
    // Remove them and let the sender safely resend that small range.
    if (ftruncate(fd, static_cast<off_t>(received)) < 0 ||
        lseek(fd, static_cast<off_t>(received), SEEK_SET) < 0) {
        LOG_ERROR("Cannot seek partial file %s: %s",
                  file.partial_path.c_str(), strerror(errno));
        ::close(fd);
        return false;
    }

    progress.update(received);

    auto checkpoint = [&]() -> bool {
        if (fsync(fd) < 0) {
            LOG_ERROR("Cannot sync partial file %s: %s",
                      file.partial_path.c_str(), strerror(errno));
            return false;
        }

        if (!transfer_store_->checkpoint(
                transfer.sender_public_ip,
                transfer.transfer_id,
                file.file_index,
                received)) {
            return false;
        }

        file.durable_bytes = received;
        return true;
    };

    if (!encrypted) {
        constexpr size_t BUFFER_SIZE = 256 * 1024;
        constexpr uint64_t CHECKPOINT_INTERVAL = 4 * 1024 * 1024;

        std::vector<uint8_t> buffer(BUFFER_SIZE);
        uint64_t last_checkpoint = received;

        while (received < file.expected_size) {
            const size_t wanted = static_cast<size_t>(
                std::min<uint64_t>(BUFFER_SIZE, file.expected_size - received)
            );

            ssize_t n = conn.socket().recv(buffer.data(), wanted, 0);
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
                ssize_t w = ::write(fd, buffer.data() + written, static_cast<size_t>(n - written));
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

            if (received - last_checkpoint >= CHECKPOINT_INTERVAL) {
                if (!checkpoint()) {
                    ::close(fd);
                    return false;
                }
                last_checkpoint = received;
            }
        }
    } else {

        uint32_t expected_seq = static_cast<uint32_t>(received / CHUNK_SIZE);

        while (received < file.expected_size) {
            std::vector<uint8_t> data;
            uint32_t sequence = 0;

            if (!conn.recv_data_chunk(data, sequence)) {
                LOG_ERROR("Failed to receive encrypted data chunk");
                ::close(fd);
                return false;
            }

            if (sequence != expected_seq ||
                data.empty() ||
                data.size() > file.expected_size - received) {
                LOG_ERROR("Invalid encrypted chunk for file %u", file.file_index);
                ::close(fd);
                return false;
            }

            size_t written = 0;
            while (written < data.size()) {
                ssize_t n = ::write(fd, data.data() + written, data.size() - written);
                if (n < 0) {
                    if (errno == EINTR) continue;

                    LOG_ERROR("write failed: %s", strerror(errno));
                    ::close(fd);
                    return false;
                }
                written += static_cast<size_t>(n);
            }

            received += data.size();
            ++expected_seq;
            progress.update(received);

            if (!checkpoint()) {
                ::close(fd);
                return false;
            }
        }
    }

    if (!checkpoint()) {
        ::close(fd);
        return false;
    }

    ::close(fd);

    MessageType type;
    if (!conn.recv_message(type) || type != MessageType::FILE_COMPLETE) {
        LOG_ERROR("Expected FILE_COMPLETE after file data");
        return false;
    }

    return true;
}


int Receiver::handle_relay_discovery(
    Connection& conn,
    RelayConnection& relay_connection,
    const RelayDiscoveryRequest& request
) {
    RelayDiscoveryResponse response;
    response.discovery_id = request.discovery_id;

    if (request.version != PROTOCOL_VERSION) {
        response.message = "protocol version mismatch";
    } else if (request.file_count == 0) {
        response.message = "relay transfer contains no files";
    } else {
        response.available = true;
        response.message = "available";
    }

    // Discovery is a small control exchange. Flush its response immediately
    // instead of retaining it behind the data-transfer TCP_CORK setting.
    conn.socket().set_cork(false);

    if (!relay_connection.send_discovery_response(response)) {
        LOG_ERROR(
            "Failed to send relay discovery response to %s",
            conn.socket().peer_address().c_str()
        );
        return 1;
    }

    if (response.available) {
        LOG_INFO(
            "Available for relay discovery %s from %s",
            request.discovery_id.c_str(),
            conn.socket().peer_address().c_str()
        );
    } else {
        LOG_WARN(
            "Rejected relay discovery %s from %s: %s",
            request.discovery_id.c_str(),
            conn.socket().peer_address().c_str(),
            response.message.c_str()
        );
    }

    return 0;
}

bool Receiver::store_relay_assignment(
    const std::string& coordinator_ip,
    const RelayAssignmentRequest& request,
    std::string& rejection_reason
) {
    std::lock_guard<std::mutex> lock(relay_assignments_mutex_);

    auto assigned = relay_assignments_.find(request.transfer_id);

    if (assigned != relay_assignments_.end()) {
        const RelayAssignment& existing = assigned->second;
        const bool same_assignment =
            existing.coordinator_ip == coordinator_ip &&
            existing.discovery_id == request.discovery_id &&
            existing.total_size == request.total_size &&
            existing.file_count == request.file_count &&
            existing.has_downstream == request.has_downstream &&
            existing.downstream_address == request.downstream_address &&
            existing.downstream_port == request.downstream_port;

        if (same_assignment) {
            return true;
        }

        rejection_reason = "transfer ID conflicts with an existing relay assignment";
        return false;
    }

    if (!request.has_downstream && !request.downstream_address.empty()) {
        rejection_reason = "final relay node cannot have a downstream address";
        return false;
    }

    RelayAssignment assignment;
    assignment.coordinator_ip = coordinator_ip;
    assignment.discovery_id = request.discovery_id;
    assignment.total_size = request.total_size;
    assignment.file_count = request.file_count;
    assignment.has_downstream = request.has_downstream;
    assignment.downstream_address = request.downstream_address;
    assignment.downstream_port = request.downstream_port;

    relay_assignments_.emplace(request.transfer_id, std::move(assignment));
    return true;
}

int Receiver::handle_relay_assignment(
    Connection& conn,
    RelayConnection& relay_connection,
    const RelayAssignmentRequest& request
) {
    RelayAssignmentResponse response;
    response.discovery_id = request.discovery_id;
    response.transfer_id = request.transfer_id;

    if (request.version != PROTOCOL_VERSION) {
        response.message = "protocol version mismatch";
    } else {
        response.accepted = store_relay_assignment(
            conn.socket().peer_address(),
            request,
            response.message
        );

        if (response.accepted) {
            response.message = "assigned";
        }
    }

    conn.socket().set_cork(false);

    if (!relay_connection.send_assignment_response(response)) {
        LOG_ERROR(
            "Failed to send relay assignment response to %s",
            conn.socket().peer_address().c_str()
        );
        return 1;
    }

    if (response.accepted) {
        if (request.has_downstream) {
            LOG_INFO(
                "Assigned relay transfer %s; downstream %s:%u",
                request.transfer_id.c_str(),
                request.downstream_address.c_str(),
                request.downstream_port
            );
        } else {
            LOG_INFO(
                "Assigned relay transfer %s as final node",
                request.transfer_id.c_str()
            );
        }
    } else {
        LOG_WARN(
            "Rejected relay assignment %s from %s: %s",
            request.transfer_id.c_str(),
            conn.socket().peer_address().c_str(),
            response.message.c_str()
        );
    }

    return response.accepted ? 0 : 1;
}

// Handles one accepted connection on a thread-pool worker.
//
// Initial-message routing:
// 1. Read the first framed message once.
// 2. RELAY_DISCOVERY_REQUEST reports whether this receiver is available.
// 3. RELAY_ASSIGNMENT_REQUEST stores this receiver's downstream role.
// 4. TRANSFER_REQUEST continues through the existing file-transfer flow.
//
// Transfer recovery identity:
// - peer_address() selects the receiver output directory:
//     <out-dir>/<immediate-upstream-ip>/
// - For a direct or fan-out transfer, the immediate upstream is the sender.
// - For a relay transfer, it may be the previous relay node.
// - transfer_id selects the persistent recovery journal in that directory.
// - The journal stores each file's allocated final name, .part path,
//   expected size/hash, and receiver-confirmed durable byte offset.
//
// TRANSFER_REQUEST flow:
// 1. Validate the sender's manifest.
// 2. Ask the user to accept unless --accept-all is enabled.
// 3. Load the existing recovery journal for the upstream IP and transfer ID,
//    or create one and reserve unique final filenames such as "file (1).txt".
// 4. Lock the transfer so two live connections cannot modify the same .part
//    files concurrently.
// 5. Return one durable resume offset per manifest file.
// 6. For each FILE_HEADER, verify its index, path, complete size, and sender
//    offset against the saved receiver state.
// 7. Receive the remaining bytes into .part files, verify SHA-256, and
//    atomically rename each verified file to its final destination.
// 8. Mark the journal complete only after TRANSFER_COMPLETE is received.
//
// If a data connection breaks, the function returns while retaining its
// journal and .part files. A later upstream connection using the same
// transfer_id receives the stored offsets and can continue safely.
int Receiver::handle_connection(Connection& conn) {
    MessageType initial_type{};
    TransferRequest req{};
    RelayDiscoveryRequest discovery_request;
    RelayAssignmentRequest assignment_request;
    RelayConnection relay_connection(conn);

    if (!relay_connection.recv_initial_request(
            initial_type,
            req,
            discovery_request,
            assignment_request)) {
        LOG_ERROR("Failed to receive initial request");
        return 1;
    }

    if (initial_type == MessageType::RELAY_DISCOVERY_REQUEST) {
        return handle_relay_discovery(
            conn,
            relay_connection,
            discovery_request
        );
    }

    if (initial_type == MessageType::RELAY_ASSIGNMENT_REQUEST) {
        return handle_relay_assignment(
            conn,
            relay_connection,
            assignment_request
        );
    }

    if (initial_type != MessageType::TRANSFER_REQUEST) {
        LOG_ERROR(
            "Unsupported initial request type: %d",
            static_cast<int>(initial_type)
        );
        return 1;
    }

    const std::string sender_public_ip = conn.socket().peer_address();

    auto reject = [&](const std::string& reason) {
        LOG_ERROR("Rejecting transfer %s: %s",
                  req.transfer_id.c_str(), reason.c_str());

        TransferResponse response;
        response.accepted = false;
        response.message = reason;
        conn.send_transfer_response(response);
        return 1;
    };

    if (req.version != PROTOCOL_VERSION) {
        return reject("protocol version mismatch");
    }

    if (req.transfer_id.empty()) {
        return reject("missing transfer ID");
    }

    for (const auto& file : req.files) {
        if (!fs_is_safe_relpath(file.relpath)) {
            return reject("unsafe path in transfer manifest");
        }
    }

    // A first connection still needs user approval. With --accept-all this
    // returns immediately; later refinement can skip this prompt for an
    // already-approved recovered journal.
    if (!prompt_accept(sender_public_ip, req)) {
        TransferResponse response;
        response.accepted = false;
        response.message = "transfer rejected by receiver user";
        conn.send_transfer_response(response);
        return 1;
    }

    auto stored_transfer = transfer_store_->open_or_create(sender_public_ip, req);

    if (!stored_transfer) {
        return reject("cannot create or validate transfer recovery state");
    }

    if (!transfer_store_->acquire_session(
            sender_public_ip, stored_transfer->transfer_id)) {
        return reject("this transfer is already active");
    }

    struct SessionRelease {
        TransferStore* store;
        std::string sender_ip;
        std::string transfer_id;

        ~SessionRelease() {
            store->release_session(sender_ip, transfer_id);
        }
    } release{
        transfer_store_.get(),
        sender_public_ip,
        stored_transfer->transfer_id
    };

    TransferResponse response;
    response.accepted = true;
    response.resume_offsets = transfer_store_->resume_offsets(*stored_transfer);

    if (!conn.send_transfer_response(response)) {
        LOG_ERROR("Failed to send transfer response");
        return 1;
    }

    LOG_INFO("Receiving transfer %s from %s — %zu files",
             stored_transfer->transfer_id.c_str(),
             sender_public_ip.c_str(),
             stored_transfer->files.size());

    const auto overall_start = std::chrono::steady_clock::now();

    for (size_t i = 0; i < stored_transfer->files.size(); ++i) {
        StoredFileProgress& stored_file = stored_transfer->files[i];

        uint32_t file_index = 0;
        uint64_t full_file_size = 0;
        uint64_t file_offset = 0;
        std::string filename;

        if (!conn.recv_file_header(file_index, full_file_size, file_offset, filename)) {
            LOG_ERROR("Failed to receive file header");
            return 1;
        }

        // Files must arrive in manifest order. This keeps raw-byte boundaries
        // unambiguous and matches the sender's one-connection batch format.
        if (file_index != i ||
            filename != stored_file.requested_relpath ||
            full_file_size != stored_file.expected_size ||
            file_offset != stored_file.durable_bytes) {
            LOG_ERROR("Invalid resume header for transfer %s, file %zu", stored_transfer->transfer_id.c_str(), i);
            return 1;
        }

        LOG_INFO("[%zu/%zu] Receiving %s at offset %llu/%llu",
                 i + 1,
                 stored_transfer->files.size(),
                 stored_file.final_relpath.c_str(),
                 static_cast<unsigned long long>(file_offset),
                 static_cast<unsigned long long>(full_file_size));

        ProgressBar progress(stored_file.final_relpath, stored_file.expected_size);

        if (!recv_file(conn, *stored_transfer, stored_file, req.encrypted, progress)) {
            LOG_ERROR("Failed to receive file: %s",
                      stored_file.requested_relpath.c_str());
            return 1;
        }

        // On a reconnect, a file already marked complete was only consumed
        // above; never hash or rename it again.
        if (!stored_file.completed) {
            if (!stored_file.expected_sha256.empty()) {
                const std::string actual_hash = SHA256::file_hash(stored_file.partial_path);

                if (actual_hash != stored_file.expected_sha256) {
                    LOG_ERROR("Hash mismatch for %s", stored_file.requested_relpath.c_str());
                    return 1;
                }
            }

            if (!transfer_store_->complete_file(
                    sender_public_ip,
                    stored_transfer->transfer_id,
                    stored_file.file_index)) {
                LOG_ERROR("Failed to finalize received file: %s",
                          stored_file.final_relpath.c_str());
                return 1;
            }

            stored_file.completed = true;
        }

        progress.finish();
    }

    MessageType type;
    if (!conn.recv_message(type) || type != MessageType::TRANSFER_COMPLETE) {
        LOG_ERROR("Expected TRANSFER_COMPLETE");
        return 1;
    }

    if (!transfer_store_->complete_transfer( sender_public_ip, stored_transfer->transfer_id)) {
        LOG_ERROR("Failed to mark transfer complete: %s",
                  stored_transfer->transfer_id.c_str());
        return 1;
    }

    const double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - overall_start).count();

    LOG_INFO("Transfer complete: %s (%.1fs)", stored_transfer->transfer_id.c_str(), elapsed);

    return 0;
}

void Receiver::handle_connection_thread(std::shared_ptr<Connection> connection,
                                        uint64_t connection_id) {
    struct ConnectionUnregister {
        Receiver* receiver;
        uint64_t id;
        ~ConnectionUnregister() { receiver->unregister_connection(id); }
    } unregister{this, connection_id};

    connection->tune_socket();

    try {
        handle_connection(*connection);
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

    transfer_store_ = std::make_unique<TransferStore>(out_dir_);

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

    // Create workers only after daemonization: forking with live threads is unsafe.
    thread_pool_ = std::make_unique<ThreadPool>();
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


        auto connection = std::make_shared<Connection>(std::move(client));
        const uint64_t connection_id = register_connection(connection);

        if (!thread_pool_->enqueue([this, connection, connection_id] {
                handle_connection_thread(connection, connection_id);
            })) {
            LOG_WARN("Connection queue is full; rejecting incoming connection");
            unregister_connection(connection_id);
        }
    }

    listener.stop();
    if (thread_pool_) {
        thread_pool_->stop(true);
    }
    LOG_INFO("Receiver stopped.");

    if (daemon_) {
        remove_pid_file();
    }

    return 0;
}

} // namespace flashshare
