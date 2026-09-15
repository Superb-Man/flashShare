#include "transfer/sender.h"
#include "util/logger.h"
#include "util/fs.h"
#include "util/sha256.h"

#include <sys/stat.h>
#include <fcntl.h>
#include <cstring>
#include <cstdio>
#include <random>
#include <sstream>
#include <iomanip>
#include <chrono>
#include <unordered_set>
#include <cstdlib>
#include <thread>

#ifdef _WIN32
#include <io.h>
#define S_ISREG(m) (((m) & 0170000) == 0100000)
#define S_ISDIR(m) (((m) & 0170000) == 0040000)
#else
#include <unistd.h>
#endif

namespace flashshare {

namespace {

// Test-only hook. It has no effect unless the test explicitly sets:
//   FLASHSHARE_TEST_SEND_DELAY_US=<microseconds>
//
// It makes receiver-restart tests deterministic on fast localhost links.
void maybe_test_send_delay() {
    const char* value = std::getenv("FLASHSHARE_TEST_SEND_DELAY_US");
    if (!value || *value == '\0') {
        return;
    }

    char* end = nullptr;
    const unsigned long microseconds = std::strtoul(value, &end, 10);

    if (*end != '\0' || microseconds == 0) {
        return;
    }

    std::this_thread::sleep_for(
        std::chrono::microseconds(microseconds));
}

} // namespace

Sender::Sender(const std::vector<std::string>& paths, const std::string& target_ip,
               uint16_t port, bool encrypt, bool resume)
    : paths_(paths), target_ip_(target_ip), port_(port),
      encrypt_(encrypt), resume_(resume) {}

std::string Sender::generate_transfer_id() {
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<uint64_t> dis;

    std::ostringstream ss;
    ss << std::hex << std::setfill('0');
    ss << std::setw(16) << dis(gen);
    ss << std::setw(16) << dis(gen);
    return ss.str();
}

uint64_t Sender::get_file_size(const std::string& path) {
    struct stat st;
    if (stat(path.c_str(), &st) != 0) {
        return 0;
    }
    return static_cast<uint64_t>(st.st_size);
}

std::string Sender::basename(const std::string& path) {
    size_t pos = path.find_last_of('/');
    if (pos == std::string::npos) return path;
    return path.substr(pos + 1);
}

// Sends one file from the exact durable byte offset confirmed by the receiver.
//
// The sender never assumes that a successful sendfile()/send() call means the
// receiver stored those bytes. After a broken connection, it reconnects and
// uses a new receiver-confirmed offset before sending again.
bool Sender::send_file(Connection& conn, const FileEntry& file, uint32_t file_index, uint64_t receiver_confirmed_offset, ProgressBar& progress) {
    if (receiver_confirmed_offset > file.size) {
        LOG_ERROR("Receiver returned invalid offset for %s: %llu > %llu",
                  file.relpath.c_str(),
                  static_cast<unsigned long long>(receiver_confirmed_offset),
                  static_cast<unsigned long long>(file.size));
        return false;
    }

    if (!conn.send_file_header(file_index, file.size, receiver_confirmed_offset, file.relpath)) {
        LOG_ERROR("Failed to send file header for %s", file.relpath.c_str());
        return false;
    }

    uint64_t sent = receiver_confirmed_offset;
    progress.update(sent);

    // The receiver already finalized this file during an earlier connection.
    // It still expects FILE_COMPLETE so it can keep the connection protocol
    // aligned for the next file.
    if (sent == file.size) {
        return conn.send_message(MessageType::FILE_COMPLETE);
    }

    int file_fd = ::open(file.abspath.c_str(), O_RDONLY);
    if (file_fd < 0) {
        LOG_ERROR("Cannot open source file %s: %s",
                  file.abspath.c_str(), strerror(errno));
        return false;
    }

    if (!encrypt_) {
        LOG_INFO("Resuming %s at %llu/%llu via zero-copy sendfile",
                 file.relpath.c_str(),
                 static_cast<unsigned long long>(sent),
                 static_cast<unsigned long long>(file.size));

        // Ensure the FILE_HEADER is on the wire before unframed raw bytes.
        conn.socket().set_cork(false);
        conn.socket().set_cork(true);

        while (sent < file.size) {
            size_t to_send = CHUNK_SIZE;
            if (file.size - sent < CHUNK_SIZE) {
                to_send = static_cast<size_t>(file.size - sent);
            }

            off_t source_offset = static_cast<off_t>(sent);
            ssize_t n = conn.socket().sendfile(file_fd, &source_offset, to_send);

            if (n < 0) {
                if (errno == EINTR || errno == EAGAIN) continue;

                LOG_ERROR("sendfile failed for %s: %s", file.relpath.c_str(), strerror(errno));
                ::close(file_fd);
                return false;
            }

            if (n == 0) {
                LOG_ERROR("Source file changed or ended early: %s",
                          file.abspath.c_str());
                ::close(file_fd);
                return false;
            }

            sent += static_cast<uint64_t>(n);
            progress.update(sent);
            maybe_test_send_delay(); // For test purpose

        }
    } else {
        // Encrypted chunks must resume at a chunk boundary. The receiver
        // checkpoints encrypted transfers after each complete chunk.
        if (receiver_confirmed_offset % CHUNK_SIZE != 0) {
            LOG_ERROR("Encrypted resume offset is not chunk-aligned");
            ::close(file_fd);
            return false;
        }

        if (lseek(file_fd, static_cast<off_t>(receiver_confirmed_offset), SEEK_SET) < 0) {
            LOG_ERROR("Cannot seek source file %s: %s", file.abspath.c_str(), strerror(errno));
            ::close(file_fd);
            return false;
        }

        uint32_t sequence = static_cast<uint32_t>(receiver_confirmed_offset / CHUNK_SIZE);

        std::vector<uint8_t> buffer(CHUNK_SIZE);

        while (sent < file.size) {
            size_t to_read = CHUNK_SIZE;
            if (file.size - sent < CHUNK_SIZE) {
                to_read = static_cast<size_t>(file.size - sent);
            }

            ssize_t n = ::read(file_fd, buffer.data(), to_read);
            if (n < 0) {
                if (errno == EINTR) continue;

                LOG_ERROR("read failed for %s: %s", file.abspath.c_str(), strerror(errno));
                ::close(file_fd);
                return false;
            }

            if (n == 0) {
                LOG_ERROR("Source file changed or ended early: %s",
                          file.abspath.c_str());
                ::close(file_fd);
                return false;
            }

            if (!conn.send_data_chunk( buffer.data(), static_cast<size_t>(n), sequence++)) {
                LOG_ERROR("Failed to send data chunk for %s", file.relpath.c_str());
                ::close(file_fd);
                return false;
            }

            sent += static_cast<uint64_t>(n);
            progress.update(sent);
        }
    }

    ::close(file_fd);

    conn.socket().set_cork(false);
    conn.socket().set_cork(true);

    if (!conn.send_message(MessageType::FILE_COMPLETE)) {
        LOG_ERROR("Failed to send FILE_COMPLETE for %s", file.relpath.c_str());
        return false;
    }

    return true;
}

std::vector<FileEntry> Sender::build_manifest() {
    std::vector<FileEntry> files;

    for (const auto& path : paths_) {
        if (fs_is_directory(path)) {
            LOG_DEBUG("Walking directory: %s", path.c_str());
            auto dir_files = fs_walk_directory(path);
            files.insert(files.end(), dir_files.begin(), dir_files.end());
        } else {
            FileEntry entry;
            entry.relpath = basename(path);
            entry.abspath = path;
            entry.size = get_file_size(path);
            files.push_back(std::move(entry));
        }
    }

    // Multiple source paths can legitimately produce the same relpath (e.g.
    // two single files with the same basename) — warn since the receiver
    // will silently overwrite one with the other.
    std::unordered_set<std::string> seen;
    for (const auto& entry : files) {
        if (!seen.insert(entry.relpath).second) {
            LOG_WARN("Duplicate path in manifest: %s (later file will overwrite it on the receiver)",
                     entry.relpath.c_str());
        }
    }

    for (auto& entry : files) {
        entry.sha256 = SHA256::file_hash(entry.abspath);
        LOG_DEBUG("Hashed %s -> %s", entry.relpath.c_str(), entry.sha256.c_str());
    }

    return files;
}

// Opens a new TCP connection and asks the receiver for its durable offsets.
// The same TransferSession is used for every retry, including its transfer ID
// and original manifest, so the receiver can find the correct recovery journal.
bool Sender::connect_and_negotiate(const TransferSession& session, std::unique_ptr<Connection>& conn, std::vector<uint64_t>& resume_offsets) {
    conn.reset();
    resume_offsets.clear();

    Socket socket;
    if (!socket.create()) {
        return false;
    }

    socket.set_buffer_size(4 * 1024 * 1024, 4 * 1024 * 1024);

    if (!socket.connect(target_ip_, port_, 10)) {
        LOG_ERROR("Cannot connect to receiver %s:%u",
                  target_ip_.c_str(), port_);
        return false;
    }

    socket.set_nodelay(true);
    socket.set_cork(true);

    auto candidate = std::make_unique<Connection>(std::move(socket));

    TransferRequest request;
    request.version = PROTOCOL_VERSION;
    request.transfer_id = session.transfer_id;
    request.files = session.files;
    request.total_size = session.total_size;
    request.encrypted = encrypt_;
    request.resume = resume_;

    if (!candidate->send_transfer_request(request)) {
        LOG_ERROR("Failed to send transfer request");
        return false;
    }

    TransferResponse response;
    if (!candidate->recv_transfer_response(response)) {
        LOG_ERROR("Failed to receive transfer response");
        return false;
    }

    if (!response.accepted) {
        LOG_ERROR("Transfer rejected by receiver: %s",response.message.empty() ? "no reason supplied" : response.message.c_str());
        return false;
    }

    if (response.resume_offsets.size() != session.files.size()) {
        LOG_ERROR("Receiver returned %zu offsets for %zu files", response.resume_offsets.size(), session.files.size());
        return false;
    }

    for (size_t i = 0; i < session.files.size(); ++i) {
        if (response.resume_offsets[i] > session.files[i].size) {
            LOG_ERROR("Receiver returned invalid offset for %s", session.files[i].relpath.c_str());
            return false;
        }
    }

    resume_offsets = std::move(response.resume_offsets);
    conn = std::move(candidate);

    LOG_INFO("Connected to receiver %s:%u for transfer %s", target_ip_.c_str(), port_, session.transfer_id.c_str());

    return true;
}

bool Sender::wait_before_retry(unsigned int failed_attempt) const {
    unsigned int seconds = 1;

    for (unsigned int i = 1; i < failed_attempt && seconds < 10; ++i) {
        seconds *= 2;
    }

    if (seconds > 10) {
        seconds = 10;
    }

    LOG_WARN("Connection interrupted; retrying in %u second(s)", seconds);
    std::this_thread::sleep_for(std::chrono::seconds(seconds));
    return true;
}

// Sends one complete protocol attempt on one TCP connection.
//
// Even when the receiver reports a file as already complete, send_file()
// sends its FILE_HEADER and FILE_COMPLETE markers. This keeps the receiver's
// multi-file connection stream aligned before moving to the next file.
bool Sender::send_attempt(const TransferSession& session,
                          Connection& conn,
                          const std::vector<uint64_t>& resume_offsets) {
    if (resume_offsets.size() != session.files.size()) {
        LOG_ERROR("Invalid resume-offset count");
        return false;
    }

    const auto overall_start = std::chrono::steady_clock::now();
    uint64_t bytes_sent_this_attempt = 0;

    for (size_t i = 0; i < session.files.size(); ++i) {
        const FileEntry& file = session.files[i];
        const uint64_t offset = resume_offsets[i];

        LOG_INFO("[%zu/%zu] Sending %s from %llu/%llu",
                 i + 1,
                 session.files.size(),
                 file.relpath.c_str(),
                 static_cast<unsigned long long>(offset),
                 static_cast<unsigned long long>(file.size));

        ProgressBar progress(file.relpath, file.size);

        if (!send_file(conn,
                       file,
                       static_cast<uint32_t>(i),
                       offset,
                       progress)) {
            LOG_ERROR("Transfer attempt failed while sending %s",
                      file.relpath.c_str());
            return false;
        }

        progress.finish();
        bytes_sent_this_attempt += file.size - offset;
    }

    if (!conn.send_message(MessageType::TRANSFER_COMPLETE)) {
        LOG_ERROR("Failed to send TRANSFER_COMPLETE");
        return false;
    }

    const double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - overall_start).count();

    LOG_INFO("Connection attempt sent %llu remaining byte(s) in %.1fs",
             static_cast<unsigned long long>(bytes_sent_this_attempt),
             elapsed);

    return true;
}

int Sender::run() {
    if (paths_.empty()) {
        LOG_ERROR("No paths to send");
        return 1;
    }

    // Check every path exists and is a file or directory before doing
    // anything else, so a bad path fails fast instead of mid-transfer.
    for (const auto& path : paths_) {
        struct stat st;
        if (stat(path.c_str(), &st) != 0) {
            LOG_ERROR("Cannot access path: %s — %s", path.c_str(), strerror(errno));
            return 1;
        }
        if (!S_ISDIR(st.st_mode) && !S_ISREG(st.st_mode)) {
            LOG_ERROR("Not a regular file or directory: %s", path.c_str());
            return 1;
        }
    }

    TransferSession session;
    session.transfer_id = generate_transfer_id();
    session.files = build_manifest();

    if (session.files.empty()) {
        LOG_ERROR("No files found in given paths");
        return 1;
    }

    uint64_t total_size = 0;
    for (const auto& f : session.files) total_size += f.size;

    LOG_INFO("Prepared transfer %s: %zu file(s), %llu byte(s)",
             session.transfer_id.c_str(),
             session.files.size(),
             static_cast<unsigned long long>(session.total_size));

    // Without --resume, preserve the existing one-attempt behavior.
    // With --resume, every reconnect uses this same session and transfer ID.

    constexpr unsigned int MAX_RESUME_RETRIES = 10;
    unsigned int failed_attempts = 0;

    for (;;) {
        std::unique_ptr<Connection> conn;
        std::vector<uint64_t> resume_offsets;

        const bool connected = connect_and_negotiate(session, conn, resume_offsets);

        const bool transferred = connected && send_attempt(session, *conn, resume_offsets);

        if (transferred) {
            LOG_INFO("Transfer complete: %s", session.transfer_id.c_str());
            return 0;
        }

        if (!resume_) {
            LOG_ERROR("Transfer failed. Re-run with --resume to enable recovery.");
            return 1;
        }

        ++failed_attempts;
        if (failed_attempts >= MAX_RESUME_RETRIES) {
            LOG_ERROR("Transfer %s could not recover after %u attempts", session.transfer_id.c_str(), failed_attempts);
            return 1;
        }

        if (!wait_before_retry(failed_attempts)) {
            return 1;
        }
    }
}

}
