#include "transfer/relay_forwarder.h"

#include "transfer/receive_pipe.h"
#include "transfer/scoped_file.h"
#include "util/logger.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <utility>

#ifdef _WIN32
#include <io.h>
#else
#include <unistd.h>
#endif

// Forwards a relay transfer over one downstream connection. Fresh unencrypted
// Linux transfers splice duplicated incoming pipe buffers on a forwarding
// worker; resumed transfers retain the existing .part-file forwarding path.

namespace flashshare {

RelayForwarder::RelayForwarder(std::string downstream_address,
                               uint16_t downstream_port,
                               TransferRequest upstream_request,
                               std::vector<RelayFileFlow> files)
    : downstream_address_(std::move(downstream_address)),
      downstream_port_(downstream_port),
      upstream_request_(std::move(upstream_request)),
      files_(std::move(files)) {
    available_.reserve(files_.size());
    verified_.reserve(files_.size());
    live_pipes_.resize(files_.size());

    // A file finished during an earlier connection has all of its bytes
    // available immediately; a partially received one starts from whatever
    // this node had already stored.
    for (const auto& file : files_) {
        available_.push_back(file.completed ? file.expected_size
                                            : file.initial_available);
        verified_.push_back(file.completed);
    }
}

RelayForwarder::~RelayForwarder() {
    abort();
    if (thread_.joinable()) {
        thread_.join();
    }
}

void RelayForwarder::fail(const std::string& reason) {
    {
        std::lock_guard<std::mutex> lock(error_mutex_);
        if (last_error_.empty()) {
            last_error_ = reason;
        }
    }
    failed_ = true;
}

std::string RelayForwarder::last_error() const {
    std::lock_guard<std::mutex> lock(error_mutex_);
    return last_error_;
}

bool RelayForwarder::start() {
    Socket socket;
    if (!socket.create()) {
        fail("Cannot create downstream relay socket");
        return false;
    }

    socket.set_buffer_size(4 * 1024 * 1024, 4 * 1024 * 1024);

    if (!socket.relay_connect(downstream_address_, downstream_port_,
                              std::chrono::seconds(2))) {
        fail("Cannot connect to downstream relay node " + downstream_address_);
        return false;
    }

    socket.set_nodelay(true);
    socket.set_cork(true);
    socket.relay_set_send_timeout(std::chrono::seconds(2));
    socket.relay_set_recv_timeout(std::chrono::seconds(2));

    auto downstream = std::make_unique<Connection>(std::move(socket));

    if (!downstream->send_transfer_request(upstream_request_)) {
        fail("Cannot send downstream relay transfer request");
        return false;
    }

    TransferResponse response;
    
    if (!downstream->recv_transfer_response(response)) {
        fail("No downstream relay transfer response");
        return false;
    }

    if (!response.accepted) {
        fail("Downstream relay node rejected the transfer: " +
             (response.message.empty() ? "no reason supplied" : response.message));
        return false;
    }

    if (response.resume_offsets.size() != files_.size()) {
        fail("Downstream relay node returned the wrong number of resume offsets");
        return false;
    }

    for (size_t i = 0; i < files_.size(); ++i) {
        if (response.resume_offsets[i] > files_[i].expected_size) {
            fail("Downstream relay node returned an invalid resume offset");
            return false;
        }
    }

    if (upstream_request_.completion_ack && !response.completion_ack) {
        fail("Downstream relay node does not support chain completion acknowledgements");
        return false;
    }

    downstream_offsets_ = std::move(response.resume_offsets);
    downstream->socket().relay_set_send_timeout(std::chrono::milliseconds(0));
    downstream->socket().relay_set_recv_timeout(std::chrono::milliseconds(0));
    downstream_ = std::move(downstream);

#ifdef __linux__
    /**
     * Determine if live pipe mode can be enabled based on the upstream request
     * and the state of the files.
     */
    live_pipe_mode_ = upstream_request_.pipe && !upstream_request_.encrypted;
    for (size_t i = 0; i < files_.size(); ++i) {
        if (files_[i].completed || files_[i].initial_available != 0 || downstream_offsets_[i] != 0) {
            live_pipe_mode_ = false;
            break;
        }
    }
#endif

    LOG_INFO("Relaying transfer %s downstream to %s:%u",
             upstream_request_.transfer_id.c_str(),
             downstream_address_.c_str(),
             downstream_port_);

    thread_ = std::thread(&RelayForwarder::forward_loop, this);
    return true;
}

#ifdef __linux__

/**
 * Attach a live pipe for the specified file index.
 * Returns true if the pipe was successfully attached, false otherwise.
 */
bool RelayForwarder::attach_live_pipe(uint32_t file_index, std::shared_ptr<ReceivePipe> pipe) {
    if (!live_pipe_mode_ || !pipe || !pipe->valid()) return false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (aborted_ || file_index >= live_pipes_.size() || live_pipes_[file_index]) {
            return false;
        }
        live_pipes_[file_index] = std::move(pipe);
    }

    data_available_.notify_all();
    return true;
}
#endif

void RelayForwarder::publish(uint32_t file_index, uint64_t available) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (file_index >= available_.size()) {
            return;
        }

        // The receive thread only ever moves a file forward, but a resume
        // that rewinds must never make the forwarder resend from behind.
        if (available <= available_[file_index]) {
            return;
        }

        available_[file_index] = available;
    }

    data_available_.notify_all();
}

void RelayForwarder::publish_verified(uint32_t file_index) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (file_index >= verified_.size()) return;
        verified_[file_index] = true;
    }
    data_available_.notify_all();
}

bool RelayForwarder::wait_for_bytes(uint32_t file_index,
                                    uint64_t sent,
                                    uint64_t& available) {
    std::unique_lock<std::mutex> lock(mutex_);

    data_available_.wait(lock, [&] {
        return aborted_ || available_[file_index] > sent;
    });

    if (aborted_) {
        return false;
    }

    available = available_[file_index];
    return true;
}

bool RelayForwarder::wait_for_verified(uint32_t file_index) {
    std::unique_lock<std::mutex> lock(mutex_);
    data_available_.wait(lock, [&] {
        return aborted_ || verified_[file_index];
    });
    return !aborted_;
}

bool RelayForwarder::wait_for_upstream_complete() {
    std::unique_lock<std::mutex> lock(mutex_);
    data_available_.wait(lock, [&] {
        return aborted_ || upstream_complete_;
    });
    return !aborted_;
}

void RelayForwarder::abort() {
#ifdef __linux__
    std::vector<std::shared_ptr<ReceivePipe>> pipes;
#endif
    {
        std::lock_guard<std::mutex> lock(mutex_);
        aborted_ = true;
#ifdef __linux__
        pipes = live_pipes_;
#endif
    }
    data_available_.notify_all();
#ifdef __linux__
    /**
     * Abort all live pipes to ensure that any blocked operations are interrupted.
     */
    for (const auto& pipe : pipes) {
        if (pipe) pipe->abort();
    }
#endif
    // A rebuilding upstream must also wake a forwarding worker blocked in
    // downstream sendfile() or waiting for its completion acknowledgement.
    if (downstream_) downstream_->socket().shutdown_both();
}

// Forwards one file from its live pipe or its growing partial file.
bool RelayForwarder::forward_file(const RelayFileFlow& file, uint64_t start_offset) {
    if (!downstream_->send_file_header(file.file_index,
                                       file.expected_size,
                                       start_offset,
                                       file.relpath)) {
        fail("Cannot send downstream file header for " + file.relpath);
        return false;
    }

    uint64_t sent = start_offset;

#ifdef __linux__

    /**
     * If live pipe mode is enabled, we need to wait for the live pipe
     * corresponding to this file to become available before forwarding.
     */
    if (live_pipe_mode_) {
        std::shared_ptr<ReceivePipe> pipe;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            
            // Wait until the live pipe for this file becomes available or the forwarder is aborted.
            // If the forwarder is aborted while waiting, the wait will exit and the function will return false.
            data_available_.wait(lock, [&] {
                return aborted_ || live_pipes_[file.file_index] != nullptr;
            });
            if (aborted_) return false;
            pipe = live_pipes_[file.file_index];
        }

        // Header and bytes share one socket, but the receive thread writes
        // Pipe A to disk while this worker drains Pipe B downstream.
        downstream_->socket().set_cork(false);
        downstream_->socket().set_cork(true);

        /*
         * Send the remaining live-pipe bytes first. If that pipe fills,
         * continue on this connection from the matching offset in .part.
         */
        bool file_fallback = false;

        while (sent < file.expected_size) {
            const size_t wanted = static_cast<size_t>(std::min<uint64_t>(CHUNK_SIZE, file.expected_size - sent));
            const ssize_t n = pipe->forward_to_socket(downstream_->socket().fd(), wanted);
            
            if (n == 0) {
                uint64_t switch_offset = 0;
                if (!pipe->fallback_offset(switch_offset) || switch_offset != sent) {
                    fail("Relay pipe fallback offset mismatch for " + file.relpath);
                    return false;
                }
                LOG_INFO("Relay pipe filled for %s; continuing from .part at %llu",
                         file.relpath.c_str(),
                         static_cast<unsigned long long>(switch_offset));
                file_fallback = true;
                break;
            }

            if (n < 0) {
                fail("Cannot splice live relay bytes downstream for " + file.relpath + strerror(errno));
                return false;
            }

            sent += static_cast<uint64_t>(n);
            bytes_forwarded_ += static_cast<uint64_t>(n);
        }

        if (!file_fallback) {

            if (!wait_for_verified(file.file_index)) 
                return false;

            if (!downstream_->send_message(MessageType::FILE_COMPLETE)) {
                fail("Cannot send downstream FILE_COMPLETE for " + file.relpath);
                return false;
            }
            downstream_->socket().set_cork(false);
            downstream_->socket().set_cork(true);


            return true;
        }
        // Pipe B has been drained. The common sendfile path below starts at
        // the first byte that was never queued into B on the same socket.
    }
#endif

    // The downstream node already holds this whole file. It still expects
    // FILE_COMPLETE so its multi-file stream stays aligned.
    if (sent >= file.expected_size) {
        if (!wait_for_verified(file.file_index)) {
            return false;
        }

        const bool sent_complete = downstream_->send_message(MessageType::FILE_COMPLETE);
        downstream_->socket().set_cork(false);
        downstream_->socket().set_cork(true);
        return sent_complete;
    }

    // recv_file() creates .part before it publishes the first written bytes.
    // Wait for that publication before opening the source file.
    uint64_t available = 0;
    if (!wait_for_bytes(file.file_index, sent, available)) {
        fail("Relay flow aborted while waiting for " + file.relpath);
        return false;
    }

    // complete_file() renames .part to the final name, so a file this node
    // finished earlier has to be read from its destination instead.
    std::string source_path =
        file.completed ? file.final_path : file.partial_path;

    int source_fd = ::open(source_path.c_str(), O_RDONLY);

    // A small file can be received and renamed before the forwarding thread
    // reaches it. Renaming does not disturb an fd this thread already holds,
    // but it does invalidate a .part path opened a moment too late.
    if (source_fd < 0 && !file.completed && errno == ENOENT) {
        source_path = file.final_path;
        source_fd = ::open(source_path.c_str(), O_RDONLY);
    }

    if (source_fd < 0) {
        fail("Cannot open relay source " + source_path + ": " + strerror(errno));
        return false;
    }
    ScopedFile source_guard(source_fd);

    // Flush the header before the unframed raw bytes that follow it.
    downstream_->socket().set_cork(false);
    downstream_->socket().set_cork(true);

    while (sent < file.expected_size) {
        available = std::min(available, file.expected_size);

        while (sent < available) {
            const size_t to_send = static_cast<size_t>(
                std::min<uint64_t>(CHUNK_SIZE, available - sent));

            off_t source_offset = static_cast<off_t>(sent);
            ssize_t n = downstream_->socket().sendfile(source_fd, &source_offset, to_send);

            if (n < 0) {
                if (errno == EINTR) continue;

                fail("Downstream sendfile failed for " + file.relpath + ": " +
                     strerror(errno));
                return false;
            }

            // The receive thread published these bytes, so a short file here
            // means the .part was truncated underneath the forwarder.
            if (n == 0) {
                fail("Relay source ended early: " + source_path);
                return false;
            }

            sent += static_cast<uint64_t>(n);
            bytes_forwarded_ += static_cast<uint64_t>(n);
        }

        if (sent < file.expected_size &&
            !wait_for_bytes(file.file_index, sent, available)) {
            fail("Relay flow aborted while waiting for " + file.relpath);
            return false;
        }
    }

    downstream_->socket().set_cork(false);
    downstream_->socket().set_cork(true);

    // The bytes may have streamed earlier; completion waits for this node
    // to finish its own hash check and atomic rename.
    if (!wait_for_verified(file.file_index)) return false;

    if (!downstream_->send_message(MessageType::FILE_COMPLETE)) {
        fail("Cannot send downstream FILE_COMPLETE for " + file.relpath);
        return false;
    }
    downstream_->socket().set_cork(false);
    downstream_->socket().set_cork(true);

    return true;
}

// Walks the manifest in order, but sends TRANSFER_COMPLETE only after
// this node has received and verified the entire upstream transfer.
void RelayForwarder::forward_loop() {
    for (size_t i = 0; i < files_.size(); ++i) {
        if (!forward_file(files_[i], downstream_offsets_[i])) {
            abort();
            return;
        }
    }

    if (!wait_for_upstream_complete()) return;

    if (!complete_downstream()) abort();
}

bool RelayForwarder::complete_downstream() {
    if (!downstream_->send_message(MessageType::TRANSFER_COMPLETE)) {
        fail("Cannot send downstream TRANSFER_COMPLETE");
        return false;
    }

    if (upstream_request_.completion_ack) {
        downstream_->socket().set_cork(false);
        MessageType reply{};
        if (!downstream_->recv_message(reply) || reply != MessageType::TRANSFER_ACK) {
            fail("Downstream relay chain did not confirm completion");
            return false;
        }
    }

    LOG_INFO("Relayed transfer %s downstream to %s:%u — %llu byte(s)",
             upstream_request_.transfer_id.c_str(),
             downstream_address_.c_str(),
             downstream_port_,
             static_cast<unsigned long long>(bytes_forwarded_.load()));
    return true;
}

bool RelayForwarder::finish() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        upstream_complete_ = true;
    }
    data_available_.notify_all();

    if (thread_.joinable()) {
        thread_.join();
    }

    if (failed_) {
        LOG_ERROR("Relay forwarding to %s:%u failed: %s",
                  downstream_address_.c_str(),
                  downstream_port_,
                  last_error().c_str());
        return false;
    }

    return true;
}

} // namespace flashshare
