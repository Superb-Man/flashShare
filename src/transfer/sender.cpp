#include "transfer/sender.h"
#include "transfer/scoped_file.h"
#include "util/logger.h"
#include <fcntl.h>
#include <cerrno>
#include <cstring>
#include <cstdio>
#include <random>
#include <sstream>
#include <iomanip>
#include <chrono>
#include <cstdlib>
#include <exception>
#include <memory>
#include <stdexcept>
#include <thread>
#include <utility>

#ifdef _WIN32
#include <io.h>
#else
#include <unistd.h>
#endif

namespace flashshare {

namespace {

std::string make_transfer_id() {
    std::random_device rd;
    std::mt19937_64 generator(rd());
    std::uniform_int_distribution<uint64_t> distribution;

    std::ostringstream result;
    result << std::hex << std::setfill('0');
    result << std::setw(16) << distribution(generator);
    result << std::setw(16) << distribution(generator);
    return result.str();
}

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

Sender::Sender(std::shared_ptr<const PreparedManifest> manifest,
               const std::string& target_ip,
               uint16_t port,
               bool encrypt,
               bool resume,
               bool show_progress)
    : target_ip_(target_ip), port_(port),
      encrypt_(encrypt), resume_(resume), show_progress_(show_progress) {
    session_.manifest = std::move(manifest);
}

// Sends one file from the exact durable byte offset confirmed by the receiver.
//
// The sender never assumes that a successful sendfile()/send() call means the
// receiver stored those bytes. After a broken connection, it reconnects and
// uses a new receiver-confirmed offset before sending again.
bool Sender::send_file(Connection& conn, const FileEntry& file, uint32_t file_index, uint64_t receiver_confirmed_offset, ProgressBar* progress) {
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
    if (progress) {
        progress->update(sent);
    }

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
    ScopedFile source_guard(file_fd);

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
                if (errno == EINTR) continue;

                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    LOG_ERROR("Socket send time out while sending %s", file.relpath.c_str());
                    return false;
                }

                LOG_ERROR("sendfile failed for %s: %s", file.relpath.c_str(), strerror(errno));
                return false;
            }

            if (n == 0) {
                LOG_ERROR("Source file changed or ended early: %s",
                          file.abspath.c_str());
                return false;
            }

            uint64_t new_sent = static_cast<uint64_t>(n);
            sent += new_sent;
            bytes_sent_ += new_sent;
            if (progress) {
                progress->update(sent);
            }
            maybe_test_send_delay(); // For test purpose

        }
    } else {
        // Encrypted chunks must resume at a chunk boundary. The receiver
        // checkpoints encrypted transfers after each complete chunk.
        if (receiver_confirmed_offset % CHUNK_SIZE != 0) {
            LOG_ERROR("Encrypted resume offset is not chunk-aligned");
            return false;
        }

        if (lseek(file_fd, static_cast<off_t>(receiver_confirmed_offset), SEEK_SET) < 0) {
            LOG_ERROR("Cannot seek source file %s: %s", file.abspath.c_str(), strerror(errno));
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
                return false;
            }

            if (n == 0) {
                LOG_ERROR("Source file changed or ended early: %s",
                          file.abspath.c_str());
                return false;
            }

            if (!conn.send_data_chunk( buffer.data(), static_cast<size_t>(n), sequence++)) {
                LOG_ERROR("Failed to send data chunk for %s", file.relpath.c_str());
                return false;
            }

            uint64_t new_sent = static_cast<uint64_t>(n);
            sent += new_sent;
            bytes_sent_ += new_sent;
            if (progress) {
                progress->update(sent);
            }
        }
    }

    conn.socket().set_cork(false);
    conn.socket().set_cork(true);

    if (!conn.send_message(MessageType::FILE_COMPLETE)) {
        LOG_ERROR("Failed to send FILE_COMPLETE for %s", file.relpath.c_str());
        return false;
    }

    return true;
}

// Opens a new TCP connection and asks the receiver for its durable offsets.
// The same TransferSession is used for every retry, including its transfer ID
// and original manifest, so the receiver can find the correct recovery journal.
bool Sender::connect_and_negotiate(const TransferSession& session, std::unique_ptr<Connection>& conn, std::vector<uint64_t>& resume_offsets) {
    conn.reset();
    resume_offsets.clear();

    const PreparedManifest& manifest = *session.manifest;
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

    constexpr int SEND_TIMEOUT_SECONDS = 15;
    if (!socket.set_send_timeout(SEND_TIMEOUT_SECONDS)) {
            LOG_ERROR("Cannot set send timeout for %s:%u: %s", target_ip_.c_str(), port_, strerror(errno));
        return false;
    }
    socket.set_nodelay(true);
    socket.set_cork(true);

    auto candidate = std::make_unique<Connection>(std::move(socket));

    TransferRequest request;
    request.version = PROTOCOL_VERSION;
    request.transfer_id = session.transfer_id;
    request.files = manifest.files;
    request.total_size = manifest.total_size;
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
        last_error_ = response.message.empty()
            ? "Transfer rejected by receiver"
            : "Transfer rejected: " + response.message;
        LOG_ERROR("Transfer rejected by receiver: %s",response.message.empty() ? "no reason supplied" : response.message.c_str());
        return false;
    }

    if (response.resume_offsets.size() != manifest.files.size()) {
        LOG_ERROR("Receiver returned %zu offsets for %zu files", response.resume_offsets.size(), manifest.files.size());
        return false;
    }

    for (size_t i = 0; i < manifest.files.size(); ++i) {
        if (response.resume_offsets[i] > manifest.files[i].size) {
            LOG_ERROR("Receiver returned invalid offset for %s", manifest.files[i].relpath.c_str());
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

    LOG_WARN("[%s:%u] Connection interrupted; retrying in %u second(s)",
             target_ip_.c_str(), port_, seconds);
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
    const auto& files = session.manifest->files;
    if (resume_offsets.size() != files.size()) {
        LOG_ERROR("Invalid resume-offset count");
        return false;
    }

    const auto overall_start = std::chrono::steady_clock::now();
    uint64_t bytes_sent_this_attempt = 0;

    for (size_t i = 0; i < files.size(); ++i) {
        const FileEntry& file = files[i];
        const uint64_t offset = resume_offsets[i];

        LOG_INFO("[%s:%u] [%zu/%zu] Sending %s from %llu/%llu",
                 target_ip_.c_str(), port_,
                 i + 1,
                 files.size(),
                 file.relpath.c_str(),
                 static_cast<unsigned long long>(offset),
                 static_cast<unsigned long long>(file.size));

        std::unique_ptr<ProgressBar> progress;
        if (show_progress_) {
            progress = std::make_unique<ProgressBar>(file.relpath, file.size);
        }

        if (!send_file(conn,
                       file,
                       static_cast<uint32_t>(i),
                       offset,
                       progress.get())) {
            LOG_ERROR("Transfer attempt failed while sending %s",
                      file.relpath.c_str());
            return false;
        }

        if (progress) {
            progress->finish();
        }
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
    const auto started = std::chrono::steady_clock::now();
    last_error_.clear();
    retry_count_ = 0;
    bytes_sent_ = 0;
    elapsed_time_ = 0.0;
    session_.transfer_id.clear();

    auto finish = [&](int exit_code) {
        elapsed_time_ = std::chrono::duration<double>(
            std::chrono::steady_clock::now() - started).count();
        return exit_code;
    };

    try {
        // Direct transfers prepare their metadata here. Fan-out workers
        // reuse the immutable manifest supplied by their coordinator.
        if (!paths_.empty() || !session_.manifest) {
            session_.manifest = flashshare::prepare_manifest(paths_);
        }

        if (session_.manifest->files.empty()) {
            throw std::runtime_error("No files found in source paths");
        }

        // One destination's identity stays stable throughout all retries.
        session_.transfer_id = make_transfer_id();
        LOG_INFO("[%s:%u] Prepared transfer %s: %zu file(s), %llu bytes",
                 target_ip_.c_str(), port_, session_.transfer_id.c_str(),
                 session_.manifest->files.size(),
                 static_cast<unsigned long long>(session_.manifest->total_size));

        // Preserve the original limit of ten total attempts with --resume.
        constexpr unsigned int MAX_ATTEMPTS = 10;
        unsigned int failed_attempts = 0;

        for (;;) {
            last_error_.clear();
            std::unique_ptr<Connection> conn;
            std::vector<uint64_t> resume_offsets;

            const bool connected = connect_and_negotiate(session_, conn, resume_offsets);
            const bool transferred = connected && send_attempt(session_, *conn, resume_offsets);

            // Close the failed connection before waiting to reconnect.
            conn.reset();

            if (transferred) {
                LOG_INFO("[%s:%u] Finished sending transfer %s",
                         target_ip_.c_str(), port_, session_.transfer_id.c_str());
                return finish(0);
            }

            if (last_error_.empty()) {
                last_error_ = connected
                    ? "File transmission failed"
                    : "Connection or transfer negotiation failed";
            }

            ++failed_attempts;
            if (!resume_) {
                LOG_ERROR("[%s:%u] %s; automatic resume is disabled",
                          target_ip_.c_str(), port_, last_error_.c_str());
                return finish(1);
            }

            if (failed_attempts >= MAX_ATTEMPTS) {
                LOG_ERROR("[%s:%u] Failed after %u attempts: %s",
                          target_ip_.c_str(), port_, failed_attempts, last_error_.c_str());
                return finish(1);
            }

            if (!wait_before_retry(failed_attempts)) {
                last_error_ = "Retry interrupted";
                return finish(1);
            }
            ++retry_count_;
        }
    } catch (const std::exception& error) {
        last_error_ = error.what();
    } catch (...) {
        last_error_ = "Unexpected sender failure";
    }

    LOG_ERROR("[%s:%u] %s", target_ip_.c_str(), port_, last_error_.c_str());
    return finish(1);
}

}
