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

#ifdef _WIN32
#include <io.h>
#define S_ISREG(m) (((m) & 0170000) == 0100000)
#define S_ISDIR(m) (((m) & 0170000) == 0040000)
#else
#include <unistd.h>
#endif

namespace flashshare {

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

bool Sender::send_file(Connection& conn, const std::string& filepath,
                       uint32_t file_index, uint64_t file_size,
                       const std::string& filename, ProgressBar& progress) {
    // Send file header (framed)
    if (!conn.send_file_header(file_index, file_size, filename)) {
        LOG_ERROR("Failed to send file header for %s", filename.c_str());
        return false;
    }

    // Open file
    int file_fd = ::open(filepath.c_str(), O_RDONLY);
    if (file_fd < 0) {
        LOG_ERROR("Cannot open file: %s — %s", filepath.c_str(), strerror(errno));
        return false;
    }

    uint64_t sent = 0;

    if (!encrypt_) {
        // Zero-copy path: send raw bytes via sendfile() — NO framing
        // Receiver knows file_size from FILE_HEADER and reads exactly that many bytes
        LOG_INFO("Sending %s (%llu bytes) via zero-copy sendfile",
                  filename.c_str(), (unsigned long long)file_size);

        // Flush cork to ensure FILE_HEADER frame is sent before raw data
        conn.socket().set_cork(false);
        conn.socket().set_cork(true);

        while (sent < file_size) {
            size_t to_send = CHUNK_SIZE;
            if (file_size - sent < CHUNK_SIZE) {
                to_send = static_cast<size_t>(file_size - sent);
            }

            off_t offset = static_cast<off_t>(sent);
            ssize_t n = conn.socket().sendfile(file_fd, &offset, to_send);

            if (n < 0) {
                if (errno == EINTR || errno == EAGAIN) continue;
                LOG_ERROR("sendfile failed: %s", strerror(errno));
                ::close(file_fd);
                return false;
            }
            if (n == 0) {
                LOG_ERROR("sendfile returned 0 — unexpected EOF");
                ::close(file_fd);
                return false;
            }

            sent += static_cast<uint64_t>(n);
            progress.update(sent);
        }
    } else {
        // Encrypted path: read → encrypt → send (framed chunks)
        LOG_INFO("Sending %s (%llu bytes) via encrypted path",
                  filename.c_str(), (unsigned long long)file_size);

        uint32_t seq = 0;
        std::vector<uint8_t> buffer(CHUNK_SIZE);
        while (sent < file_size) {
            size_t to_read = CHUNK_SIZE;
            if (file_size - sent < CHUNK_SIZE) {
                to_read = static_cast<size_t>(file_size - sent);
            }

            ssize_t n = ::read(file_fd, buffer.data(), to_read);
            if (n < 0) {
                if (errno == EINTR) continue;
                LOG_ERROR("read failed: %s", strerror(errno));
                ::close(file_fd);
                return false;
            }
            if (n == 0) break;

            if (!conn.send_data_chunk(buffer.data(), static_cast<size_t>(n), seq++)) {
                LOG_ERROR("Failed to send data chunk");
                ::close(file_fd);
                return false;
            }

            sent += static_cast<uint64_t>(n);
            progress.update(sent);
        }
    }

    ::close(file_fd);

    // Flush cork before sending FILE_COMPLETE frame
    conn.socket().set_cork(false);
    conn.socket().set_cork(true);

    if (!conn.send_message(MessageType::FILE_COMPLETE)) {
        LOG_ERROR("Failed to send FILE_COMPLETE");
        return false;
    }

    LOG_INFO("File sent: %s (%llu bytes)", filename.c_str(), (unsigned long long)sent);
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

    std::string transfer_id = generate_transfer_id();

    std::vector<FileEntry> files = build_manifest();
    if (files.empty()) {
        LOG_ERROR("No files found in given paths");
        return 1;
    }

    uint64_t total_size = 0;
    for (const auto& f : files) total_size += f.size;

    LOG_INFO("Prepared manifest: %zu file(s), %llu bytes total",
             files.size(), (unsigned long long)total_size);

    LOG_INFO("FlashShare Sender — connecting to %s:%u", target_ip_.c_str(), port_);

    // Create socket and connect
    Socket sock;
    if (!sock.create()) {
        return 1;
    }

    // Tune socket before connect
    sock.set_buffer_size(4 * 1024 * 1024, 4 * 1024 * 1024);

    if (!sock.connect(target_ip_, port_, 10)) {
        LOG_ERROR("Failed to connect to %s:%u", target_ip_.c_str(), port_);
        return 1;
    }

    // Post-connect tuning
    sock.set_nodelay(true);
    sock.set_cork(true);

    LOG_INFO("Connected to %s:%u", target_ip_.c_str(), port_);

    Connection conn(std::move(sock));

    // Build transfer request
    TransferRequest req;
    req.version = PROTOCOL_VERSION;
    req.transfer_id = transfer_id;
    req.encrypted = encrypt_;
    req.resume = resume_;
    req.total_size = total_size;
    req.files = files;

    // Send transfer request
    if (!conn.send_transfer_request(req)) {
        LOG_ERROR("Failed to send transfer request");
        return 1;
    }

    // Wait for response
    TransferResponse resp;
    if (!conn.recv_transfer_response(resp)) {
        LOG_ERROR("Failed to receive transfer response");
        return 1;
    }

    if (!resp.accepted) {
        LOG_ERROR("Transfer rejected by receiver");
        return 1;
    }

    LOG_INFO("Transfer accepted — sending %zu file(s), %llu bytes",
             files.size(), (unsigned long long)total_size);

    // Send each file. Per-file progress is shown live via ProgressBar;
    // overall progress is reported as a running "[i/n]" counter plus a
    // final aggregate summary.
    auto overall_start = std::chrono::steady_clock::now();
    uint64_t overall_sent = 0;

    for (size_t i = 0; i < files.size(); ++i) {
        const FileEntry& entry = files[i];
        LOG_INFO("[%zu/%zu] Sending %s (%llu bytes)",
                 i + 1, files.size(), entry.relpath.c_str(), (unsigned long long)entry.size);

        ProgressBar progress(entry.relpath, entry.size);

        if (!send_file(conn, entry.abspath, static_cast<uint32_t>(i), entry.size,
                       entry.relpath, progress)) {
            LOG_ERROR("File transfer failed: %s", entry.relpath.c_str());
            return 1;
        }

        progress.finish();
        overall_sent += entry.size;
    }

    double overall_elapsed = std::chrono::duration<double>(
        std::chrono::steady_clock::now() - overall_start).count();
    double avg_mbps = overall_elapsed > 0
        ? (static_cast<double>(overall_sent) / overall_elapsed) / (1024.0 * 1024.0)
        : 0.0;
    LOG_INFO("Overall: %zu file(s), %llu bytes sent in %.1fs (avg %.1f MB/s)",
             files.size(), (unsigned long long)overall_sent, overall_elapsed, avg_mbps);

    // Send TRANSFER_COMPLETE
    if (!conn.send_message(MessageType::TRANSFER_COMPLETE)) {
        LOG_ERROR("Failed to send TRANSFER_COMPLETE");
        return 1;
    }

    LOG_INFO("Transfer complete: %s", transfer_id.c_str());
    return 0;
}

}
