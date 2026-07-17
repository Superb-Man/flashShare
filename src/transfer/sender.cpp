#include "transfer/sender.h"
#include "util/logger.h"

#include <sys/stat.h>
#include <fcntl.h>
#include <cstring>
#include <cstdio>
#include <random>
#include <sstream>
#include <iomanip>

#ifdef _WIN32
#include <io.h>
#define S_ISREG(m) (((m) & 0170000) == 0100000)
#else
#include <unistd.h>
#endif

namespace flashshare {

Sender::Sender(const std::string& filepath, const std::string& target_ip,
               uint16_t port, bool encrypt, bool resume)
    : filepath_(filepath), target_ip_(target_ip), port_(port),
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

int Sender::run() {
    // Check file exists
    struct stat st;
    if (stat(filepath_.c_str(), &st) != 0) {
        LOG_ERROR("Cannot access file: %s — %s", filepath_.c_str(), strerror(errno));
        return 1;
    }

    if (!S_ISREG(st.st_mode)) {
        LOG_ERROR("Not a regular file: %s (directory support coming in Phase 3)", filepath_.c_str());
        return 1;
    }

    uint64_t file_size = static_cast<uint64_t>(st.st_size);
    std::string filename = basename(filepath_);
    std::string transfer_id = generate_transfer_id();

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
    req.total_size = file_size;

    FileEntry entry;
    entry.relpath = filename;
    entry.abspath = filepath_;
    entry.size = file_size;
    entry.sha256 = ""; // Phase 3 will add hashing
    req.files.push_back(entry);

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

    LOG_INFO("Transfer accepted — sending %s (%llu bytes)",
             filename.c_str(), (unsigned long long)file_size);

    // Send the file
    ProgressBar progress(filename, file_size);

    if (!send_file(conn, filepath_, 0, file_size, filename, progress)) {
        LOG_ERROR("File transfer failed");
        return 1;
    }

    progress.finish();

    // Send TRANSFER_COMPLETE
    if (!conn.send_message(MessageType::TRANSFER_COMPLETE)) {
        LOG_ERROR("Failed to send TRANSFER_COMPLETE");
        return 1;
    }

    LOG_INFO("Transfer complete: %s", transfer_id.c_str());
    return 0;
}

}
