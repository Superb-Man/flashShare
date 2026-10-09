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
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <utility>

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

// Accepts concurrent file transfers and relay control requests.
// TransferStore journals durable progress for resume; assigned relay nodes
// can forward data downstream and request rejoin after restart.
// Each accepted data connection runs on a worker; relay rejoin retries run
// separately so they do not occupy a file-transfer worker.

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
    rejoin_changed_.notify_all();
    if (rejoin_worker_.joinable()) rejoin_worker_.join();
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
                         ProgressBar& progress,
                         RelayForwarder* forwarder) {
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

            // write() already put these bytes in the page cache that
            // sendfile() reads from, so the forwarding thread can send them
            // onward now rather than waiting for the next fsync checkpoint.
            if (forwarder) {
                forwarder->publish(file.file_index, received);
            }

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
        RelayAssignment assignment;
        if (!request.transfer_id.empty() &&
            find_relay_assignment(request.transfer_id, assignment)) {
            if (assignment.coordinator_ip != conn.socket().peer_address() ||
                assignment.total_size != request.total_size ||
                assignment.file_count != request.file_count) {
                response.message = "relay transfer identity does not match";
            } else {
                response.available = true;
                response.offset = transfer_store_->durable_bytes(
                    assignment.coordinator_ip, request.transfer_id);
                response.message = "available";
            }
        } else {
            response.available = true;
            response.message = "available";
        }
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

std::string Receiver::relay_assignment_path(const std::string& transfer_id) const {
    const auto* bytes = reinterpret_cast<const uint8_t*>(transfer_id.data());
    const std::string key = SHA256::buffer_hash(bytes, transfer_id.size());
    return (std::filesystem::path(out_dir_) / ".flashshare" /
            "relay_assignments" / (key + ".state")).string();
}

bool Receiver::save_relay_assignment(
    const std::string& transfer_id,
    const RelayAssignment& assignment
) const {
    namespace fs = std::filesystem;
    const std::string path = relay_assignment_path(transfer_id);
    const std::string temporary = path + ".tmp";
    std::error_code error;
    fs::create_directories(fs::path(path).parent_path(), error);
    if (error) return false;

    {
        std::ofstream out(temporary, std::ios::trunc);
        if (!out) return false;
        out << "FLASHSHARE_RELAY_ASSIGNMENT_V2\n"
            << std::quoted(transfer_id) << '\n'
            << std::quoted(assignment.coordinator_ip) << '\n'
            << std::quoted(assignment.receiver_ip) << '\n'
            << std::quoted(assignment.discovery_id) << '\n'
            << assignment.epoch << ' ' << assignment.total_size << ' '
            << assignment.file_count << ' '
            << assignment.coordinator_rejoin_port << ' '
            << assignment.has_downstream << ' '
            << assignment.downstream_port << '\n'
            << std::quoted(assignment.downstream_address) << '\n';
        out.flush();
        if (!out) return false;
    }

#ifndef _WIN32
    const int fd = ::open(temporary.c_str(), O_RDONLY);
    if (fd < 0) return false;
    const bool synced = ::fsync(fd) == 0;
    ::close(fd);
    if (!synced) return false;
#endif

    fs::rename(temporary, path, error);
    return !error;
}

void Receiver::register_active_transfer(const std::string& transfer_id, Connection* connection) {
    std::lock_guard<std::mutex> lock(active_transfers_mutex_);
    active_transfers_[transfer_id] = connection;
}

void Receiver::unregister_active_transfer(const std::string& transfer_id) {
    std::lock_guard<std::mutex> lock(active_transfers_mutex_);
    active_transfers_.erase(transfer_id);
}

void Receiver::preempt_active_transfer(const std::string& transfer_id) {
    Connection* state = nullptr; 
    {
        std::lock_guard<std::mutex> lock(active_transfers_mutex_);
        auto it = active_transfers_.find(transfer_id);
        if (it != active_transfers_.end()) {
            state = it->second;
        }
    }

    if (state) {
        // shutdown_block is a blocking operation that will close both read and write channels
        // of the socket, effectively preempting the transfer.
        // exit and release the transfer session
        state->socket().shutdown_both();
    }
}

void Receiver::load_relay_assignments() {
    namespace fs = std::filesystem;
    const fs::path directory = fs::path(out_dir_) / ".flashshare" / "relay_assignments";
    std::error_code error;
    if (!fs::exists(directory, error)) return;
    for (const auto& entry : fs::directory_iterator(directory, error)) {
        if (error) break;
        if (!entry.is_regular_file() || entry.path().extension() != ".state") continue;

        std::ifstream in(entry.path());
        std::string format;
        std::string transfer_id;
        RelayAssignment assignment;
        unsigned int rejoin_port = 0;
        unsigned int downstream_port = 0;
        if (!std::getline(in, format) ||
            (format != "FLASHSHARE_RELAY_ASSIGNMENT_V1" &&
             format != "FLASHSHARE_RELAY_ASSIGNMENT_V2") ||
            !(in >> std::quoted(transfer_id)
                 >> std::quoted(assignment.coordinator_ip)) ||
            (format == "FLASHSHARE_RELAY_ASSIGNMENT_V2" &&
             !(in >> std::quoted(assignment.receiver_ip))) ||
            !(in >> std::quoted(assignment.discovery_id)
                 >> assignment.epoch >> assignment.total_size
                 >> assignment.file_count >> rejoin_port
                 >> assignment.has_downstream >> downstream_port
                 >> std::quoted(assignment.downstream_address)) ||
            transfer_id.empty() || assignment.coordinator_ip.empty() ||
            assignment.discovery_id.empty() || assignment.file_count == 0 ||
            rejoin_port == 0 || rejoin_port > UINT16_MAX ||
            downstream_port > UINT16_MAX ||
            entry.path() != relay_assignment_path(transfer_id)) {
            LOG_WARN("Ignoring invalid relay assignment record %s",
                     entry.path().c_str());
            continue;
        }
        assignment.coordinator_rejoin_port = static_cast<uint16_t>(rejoin_port);
        assignment.downstream_port = static_cast<uint16_t>(downstream_port);
        relay_assignments_.emplace(std::move(transfer_id), std::move(assignment));
    }
}

void Receiver::remove_relay_assignment(const std::string& transfer_id) const {
    std::error_code error;
    std::filesystem::remove(relay_assignment_path(transfer_id), error);
    if (error) LOG_WARN("Cannot remove completed relay assignment: %s", error.message().c_str());
}

bool Receiver::request_rejoin(
    const std::string& transfer_id,
    const RelayAssignment& assignment
) const {
    Socket socket;
    if (!socket.create()) return false;
    if (!assignment.receiver_ip.empty() &&
        !socket.bind(assignment.receiver_ip, 0)) {
        LOG_WARN("Cannot bind relay rejoin request to %s", assignment.receiver_ip.c_str());
        return false;
    }
    if (!socket.relay_connect(assignment.coordinator_ip,
                              assignment.coordinator_rejoin_port,
                              std::chrono::seconds(2))) {
        LOG_WARN("Cannot request relay rejoin for transfer %s", transfer_id.c_str());
        return false;
    }

    socket.relay_set_send_timeout(std::chrono::seconds(2));
    socket.relay_set_recv_timeout(std::chrono::seconds(2));

    Connection connection(std::move(socket));

    RelayConnection relay_connection(connection);
    RelayRejoinMessage request;

    request.transfer_id = transfer_id;
    request.epoch = assignment.epoch;

    RelayRejoinMessage acknowledgement;

    if (!relay_connection.send_rejoin_request(request) ||
        !relay_connection.recv_rejoin_ack(acknowledgement) ||
        acknowledgement.transfer_id != transfer_id ||
        acknowledgement.epoch != assignment.epoch) {
        LOG_WARN("Relay rejoin was not acknowledged for transfer %s", transfer_id.c_str());
        return false;
    }
    LOG_INFO("Relay rejoin accepted for transfer %s", transfer_id.c_str());
    return true;
}

void Receiver::queue_rejoin(const std::string& transfer_id,
                            const RelayAssignment& assignment) {
    if (assignment.coordinator_rejoin_port == 0) return;
    {
        std::lock_guard<std::mutex> lock(rejoin_mutex_);
        rejoin_pending_[transfer_id] = assignment;
    }
    rejoin_changed_.notify_one();
}

void Receiver::rejoin_loop() {
    while (running_) {
        std::unordered_map<std::string, RelayAssignment> pending;
        {
            std::unique_lock<std::mutex> lock(rejoin_mutex_);
            rejoin_changed_.wait(lock, [&] {
                return !running_ || !rejoin_pending_.empty();
            });
            if (!running_) break;
            pending = rejoin_pending_;
        }

        for (const auto& entry : pending) {
            if (!running_) break;
            {
                std::lock_guard<std::mutex> lock(rejoin_mutex_);
                const auto current = rejoin_pending_.find(entry.first);
                if (current == rejoin_pending_.end() ||
                    current->second.epoch != entry.second.epoch) continue;
            }
            if (request_rejoin(entry.first, entry.second)) {
                std::lock_guard<std::mutex> lock(rejoin_mutex_);
                const auto current = rejoin_pending_.find(entry.first);
                if (current != rejoin_pending_.end() &&
                    current->second.epoch == entry.second.epoch) {
                    rejoin_pending_.erase(current);
                }
            }
        }

        std::unique_lock<std::mutex> lock(rejoin_mutex_);
        rejoin_changed_.wait_for(lock, std::chrono::seconds(2), [&] {
            return !running_ || rejoin_pending_.size() > pending.size();
        });
    }
}

bool Receiver::store_relay_assignment(
    const std::string& coordinator_ip,
    const std::string& receiver_ip,
    const RelayAssignmentRequest& request,
    std::string& rejection_reason
) {
    std::lock_guard<std::mutex> lock(relay_assignments_mutex_);

    auto assigned = relay_assignments_.find(request.transfer_id);

    if (assigned != relay_assignments_.end()) {
        const RelayAssignment& existing = assigned->second;
        const bool same_assignment =
            existing.coordinator_ip == coordinator_ip &&
            existing.receiver_ip == receiver_ip &&
            existing.discovery_id == request.discovery_id &&
            existing.epoch == request.epoch &&
            existing.total_size == request.total_size &&
            existing.file_count == request.file_count &&
            existing.coordinator_rejoin_port == request.coordinator_rejoin_port &&
            existing.has_downstream == request.has_downstream &&
            existing.downstream_address == request.downstream_address &&
            existing.downstream_port == request.downstream_port;

        if (same_assignment) {
            return true;
        }

        if (existing.coordinator_ip != coordinator_ip ||
            existing.total_size != request.total_size ||
            existing.file_count != request.file_count ||
            existing.coordinator_rejoin_port != request.coordinator_rejoin_port ||
            request.epoch <= existing.epoch) {
            rejection_reason = "transfer ID conflicts with an existing relay assignment";
            return false;
        }
    }

    if (!request.has_downstream && !request.downstream_address.empty()) {
        rejection_reason = "final relay node cannot have a downstream address";
        return false;
    }

    RelayAssignment assignment;
    assignment.coordinator_ip = coordinator_ip;
    assignment.receiver_ip = receiver_ip;
    assignment.discovery_id = request.discovery_id;
    assignment.epoch = request.epoch;
    assignment.total_size = request.total_size;
    assignment.file_count = request.file_count;
    assignment.coordinator_rejoin_port = request.coordinator_rejoin_port;
    assignment.has_downstream = request.has_downstream;
    assignment.downstream_address = request.downstream_address;
    assignment.downstream_port = request.downstream_port;

    if (assignment.coordinator_rejoin_port != 0 &&
        !save_relay_assignment(request.transfer_id, assignment)) {
        rejection_reason = "cannot persist relay rejoin assignment";
        return false;
    }

    // If relay assignment mismatches
    // preempt the active transfer 
    if (assigned != relay_assignments_.end()) {
        preempt_active_transfer(request.transfer_id);
    }


    relay_assignments_[request.transfer_id] = std::move(assignment);
    {
        std::lock_guard<std::mutex> rejoin_lock(rejoin_mutex_);
        rejoin_pending_.erase(request.transfer_id);
    }
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
    response.epoch = request.epoch;

    if (request.version != PROTOCOL_VERSION) {
        response.message = "protocol version mismatch";
    } else {
        response.accepted = store_relay_assignment(
            conn.socket().peer_address(),
            conn.socket().local_address(),
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

int Receiver::handle_relay_heartbeat(
    Connection& conn,
    RelayConnection& relay_connection,
    const RelayHeartbeatRequest& request
) {
    RelayHeartbeatResponse response;
    response.transfer_id = request.transfer_id;
    response.epoch = request.epoch;

    RelayAssignment assignment;
    response.alive = request.version == PROTOCOL_VERSION &&
                     find_relay_assignment(request.transfer_id, assignment) &&
                     assignment.epoch == request.epoch &&
                     assignment.coordinator_ip == conn.socket().peer_address();

    conn.socket().set_cork(false);

    if (!relay_connection.send_heartbeat_response(response)) {
        LOG_ERROR("Failed to send relay heartbeat response for %s", request.transfer_id.c_str());
        return 1;
    }
    return 0;
}

bool Receiver::find_relay_assignment(const std::string& transfer_id, RelayAssignment& assignment) {
    std::lock_guard<std::mutex> lock(relay_assignments_mutex_);

    auto found = relay_assignments_.find(transfer_id);
    if (found == relay_assignments_.end()) {
        return false;
    }

    assignment = found->second;
    return true;
}

std::unique_ptr<RelayForwarder> Receiver::start_relay_forwarder(
    const TransferRequest& request,
    const StoredTransfer& transfer,
    const std::string& upstream_ip,
    std::string& rejection_reason
) {
    RelayAssignment assignment;

    if (!find_relay_assignment(request.transfer_id, assignment) || !assignment.has_downstream) {
        return nullptr;
    }

    // no need for now
    if (request.encrypted) {
        rejection_reason = "relay forwarding does not support encrypted transfers";
        return nullptr;
    }

    std::vector<RelayFileFlow> flows;
    flows.reserve(transfer.files.size());

    for (const StoredFileProgress& stored : transfer.files) {
        RelayFileFlow flow;
        flow.file_index = stored.file_index;

        // Forward the sender's original relpath, not this node's possibly
        // deduplicated final name, so the downstream manifest still matches.
        flow.relpath = stored.requested_relpath;
        flow.partial_path = stored.partial_path;
        flow.final_path = transfer_store_->final_path(upstream_ip, stored);
        flow.expected_size = stored.expected_size;
        flow.initial_available = stored.durable_bytes;
        flow.completed = stored.completed;

        flows.push_back(std::move(flow));
    }

    auto forwarder = std::make_unique<RelayForwarder>(
        assignment.downstream_address,
        assignment.downstream_port,
        request,
        std::move(flows)
    );

    // Connect downstream before accepting upstream: if the chain cannot be
    // formed, the sender should learn now rather than after sending a file.
    // for now it will always forward
    // Fault model will be handled later
    if (!forwarder->start()) {
        rejection_reason = "cannot start relay forwarding: " + forwarder->last_error();
        return nullptr;
    }

    return forwarder;
}

// Handles one accepted connection on a thread-pool worker.
//
// Initial-message routing:
// 1. Read the first framed message once.
// 2. RELAY_DISCOVERY_REQUEST reports whether this receiver is available.
// 3. RELAY_ASSIGNMENT_REQUEST stores this receiver's downstream role.
// 4. RELAY_HEARTBEAT_REQUEST checks the stored relay assignment.
// 5. TRANSFER_REQUEST continues through the existing file-transfer flow.
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
    RelayHeartbeatRequest heartbeat_request;
    RelayRejoinMessage rejoin_hint;
    RelayConnection relay_connection(conn);

    if (!relay_connection.recv_initial_request(
            initial_type,
            req,
            discovery_request,
            assignment_request,
            heartbeat_request,
            rejoin_hint)) {
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

    if (initial_type == MessageType::RELAY_HEARTBEAT_REQUEST) {
        return handle_relay_heartbeat(conn, relay_connection, heartbeat_request);
    }

    if (initial_type == MessageType::PLEASE_REQUEST_REJOIN) {
        RelayAssignment assignment;
        if (!find_relay_assignment(rejoin_hint.transfer_id, assignment) ||
            assignment.coordinator_ip != conn.socket().peer_address() ||
            assignment.coordinator_rejoin_port == 0) return 1;

        queue_rejoin(rejoin_hint.transfer_id, assignment);
        return 0;
    }

    if (initial_type != MessageType::TRANSFER_REQUEST) {
        LOG_ERROR(
            "Unsupported initial request type: %d",
            static_cast<int>(initial_type)
        );
        return 1;
    }

    std::string sender_public_ip = conn.socket().peer_address();

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

    if (req.completion_ack) {
        RelayAssignment assignment;
        if (!find_relay_assignment(req.transfer_id, assignment) ||
            assignment.file_count != req.files.size() ||
            assignment.total_size != req.total_size) {
            return reject("missing or mismatched relay-chain assignment");
        }
        // The upstream relay can change after a rebuild. Keep the recovery
        // journal under the original coordinator's directory on every hop.
        sender_public_ip = assignment.coordinator_ip;
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

    if (!transfer_store_->acquire_session(sender_public_ip, stored_transfer->transfer_id)) {

        // A previous data connection for this transfer may still be alive
        // Force close it so that
        // old worker exits and releases the session and retry
        preempt_active_transfer(stored_transfer->transfer_id);

        bool acquired = false;

        for (int i = 0; i < 50; i++) {
            if (transfer_store_->acquire_session(sender_public_ip, stored_transfer->transfer_id)) {
                acquired = true;
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        if (!acquired) {
            return reject("this transfer is already active");
        }
    }

    register_active_transfer(stored_transfer->transfer_id, &conn);

    struct SessionRelease {
        TransferStore* store;
        Receiver* receiver;
        std::string sender_ip;
        std::string transfer_id;

        ~SessionRelease() {
            receiver->unregister_active_transfer(transfer_id);
            store->release_session(sender_ip, transfer_id);
        }
    } release{
        transfer_store_.get(),
        this,
        sender_public_ip,
        stored_transfer->transfer_id
    };

    // When a relay connection breaks through network error
    // The reciver should restart the process and send rejoin_request
    // while trying to join the relay chain it was in.
    struct RejoinedGuard {
        Receiver* receiver;
        std::string transfer_id;
        bool succeeded = false;

        ~RejoinedGuard() {
            if (succeeded) return;
            
            RelayAssignment assignment;
            if (receiver->find_relay_assignment(transfer_id, assignment)) {
                LOG_INFO("Rejoining relay for transfer %s", transfer_id.c_str());
                receiver->queue_rejoin(transfer_id, assignment);
            }
        }
    } rejoin_guard{this, stored_transfer->transfer_id};

    // A relay node must have its downstream node running before it accepts
    // upstream, so a chain that cannot be formed fails before any bytes move.
    std::string relay_error;
    std::unique_ptr<RelayForwarder> forwarder = start_relay_forwarder(
        req, *stored_transfer, sender_public_ip, relay_error);

    if (!relay_error.empty()) {
        return reject(relay_error);
    }

    TransferResponse response;
    response.accepted = true;
    response.completion_ack = req.completion_ack;
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

        if (!recv_file(conn, *stored_transfer, stored_file, req.encrypted, progress,
                       forwarder.get())) {
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

        if (forwarder) {
            forwarder->publish_verified(stored_file.file_index);
        }

        progress.finish();
    }

    MessageType type;
    if (!conn.recv_message(type) || type != MessageType::TRANSFER_COMPLETE) {
        LOG_ERROR("Expected TRANSFER_COMPLETE");
        return 1;
    }


    if (forwarder && !forwarder->finish()) {
        LOG_ERROR("Received transfer %s but could not relay it downstream",
                  stored_transfer->transfer_id.c_str());
        return 1;
    }

    if (!transfer_store_->complete_transfer( sender_public_ip, stored_transfer->transfer_id)) {
        LOG_ERROR("Failed to mark transfer complete: %s",
                  stored_transfer->transfer_id.c_str());
        return 1;
    }

    remove_relay_assignment(stored_transfer->transfer_id);

    if (req.completion_ack) {
        if (!conn.send_message(MessageType::TRANSFER_ACK)) {
            LOG_ERROR("Could not acknowledge relay-chain completion upstream");
            return 1;
        }
        conn.socket().set_cork(false);
        remove_relay_assignment(stored_transfer->transfer_id);
    }

    const double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - overall_start).count();

    LOG_INFO("Transfer complete: %s (%.1fs)", stored_transfer->transfer_id.c_str(), elapsed);

    rejoin_guard.succeeded = true;

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
    load_relay_assignments();

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
    {
        std::lock_guard<std::mutex> lock(rejoin_mutex_);
        rejoin_pending_ = relay_assignments_;
    }
    rejoin_worker_ = std::thread(&Receiver::rejoin_loop, this);
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
    rejoin_changed_.notify_all();
    if (rejoin_worker_.joinable()) rejoin_worker_.join();
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
