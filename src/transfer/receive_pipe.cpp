#include "transfer/receive_pipe.h"

#ifdef __linux__

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <fcntl.h>
#include <thread>
#include <unistd.h>
#include <system_error>
#include <utility>

// Receives unencrypted bytes through kernel pipes. In relay mode tee() gives
// the forwarding worker one branch while a separate writer saves the other.

namespace flashshare {

ReceivePipe::ReceivePipe(bool relay) : relay_(relay) {

    // Test-only delay for partial file writes.
    if (const char* value = std::getenv("FLASHSHARE_TEST_PART_WRITE_DELAY_US")) {
        char* end = nullptr;
        errno = 0;
        const unsigned long delay = std::strtoul(value, &end, 10);

        if (end != value && *end == '\0' && errno == 0 && delay <= 1000000) {
            test_write_delay_us_ = static_cast<uint32_t>(delay);
        }
    }

    if (::pipe(receive_fds_) != 0) 
        return;
    if (relay_ && ::pipe(forward_fds_) != 0) {
        ::close(receive_fds_[0]);
        ::close(receive_fds_[1]);

        receive_fds_[0] = receive_fds_[1] = -1;
        return;
    }

    /*
    * Give the local file writer its own pipe. The forwarding pipe can then be
    * drained independently while incoming bytes wait to be saved to .part.
    */
    if (relay_ && ::pipe(file_fds_) != 0) {
        for (int fd : receive_fds_) ::close(fd);
        for (int fd : forward_fds_) ::close(fd);
        receive_fds_[0] = receive_fds_[1] = -1;
        forward_fds_[0] = forward_fds_[1] = -1;
        return;
    }
    if (relay_) {
        const int flags = ::fcntl(file_fds_[1], F_GETFL);
        if (flags < 0 || ::fcntl(file_fds_[1], F_SETFL, flags | O_NONBLOCK) < 0) {
            for (int fd : receive_fds_) ::close(fd);
            for (int fd : forward_fds_) ::close(fd);
            for (int fd : file_fds_) ::close(fd);
            receive_fds_[0] = receive_fds_[1] = -1;
            forward_fds_[0] = forward_fds_[1] = -1;
            file_fds_[0] = file_fds_[1] = -1;
            return;
        }
        // Best effort: a larger kernel queue absorbs brief disk stalls.
        // User or container pipe limits may leave the default capacity.
        ::fcntl(file_fds_[1], F_SETPIPE_SZ, 1024 * 1024);
        ::fcntl(forward_fds_[1], F_SETPIPE_SZ, 1024 * 1024);
    }

    valid_ = true;
}

ReceivePipe::~ReceivePipe() {
    finish_writer();
    for (int fd : receive_fds_) if (fd >= 0) ::close(fd);
    for (int fd : forward_fds_) if (fd >= 0) ::close(fd);
    for (int fd : file_fds_) if (fd >= 0) ::close(fd);
}

/*
 * Start one writer for this file so receiving and saving can run separately.
 */
bool ReceivePipe::start_writer(int file_fd, WrittenCallback on_written) {
    if (!valid_ || !relay_ || file_fd < 0 || writer_.joinable()) return false;
    file_fd_ = file_fd;
    on_written_ = std::move(on_written);
    try {
        writer_ = std::thread(&ReceivePipe::write_file, this);
    } catch (const std::system_error&) {
        return false;
    }
    return true;
}

/*
 * Wait for all queued bytes to reach .part before finishing the file.
 * Report an error if the writer could not save everything.
 */
bool ReceivePipe::finish_writer() {
    if (!relay_) return true;
    if (writer_.joinable()) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            writer_closed_ = true;
        }
        changed_.notify_all();
        writer_.join();
    }
    std::lock_guard<std::mutex> lock(mutex_);
    if (writer_error_ || written_ != queued_) {
        errno = writer_error_ ? writer_error_ : EIO;
        return false;
    }
    return true;
}

/*
 * Drain the local pipe into .part. Publish an offset only after those bytes
 * have been written, so file-based forwarding cannot get ahead of the writer.
 */
void ReceivePipe::write_file() {
    for (;;) {
        size_t count;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            changed_.wait(lock, [&] {
                return queued_ > written_ || writer_closed_;
            });
            if (queued_ == written_) return;
            count = static_cast<size_t>(std::min<uint64_t>(
                queued_ - written_, 256 * 1024));
        }

        if (test_write_delay_us_ > 0) {
            std::this_thread::sleep_for(std::chrono::microseconds(test_write_delay_us_));
        }

        ssize_t n;
        do {
            n = ::splice(file_fds_[0], nullptr, file_fd_, nullptr, count, 0);
        } while (n < 0 && errno == EINTR);
        if (n <= 0) {
            std::lock_guard<std::mutex> lock(mutex_);
            writer_error_ = n == 0 ? EIO : errno;
            aborted_ = true;
            changed_.notify_all();
            return;
        }

        uint64_t written;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            written_ += static_cast<uint64_t>(n);
            written = written_;
        }
        changed_.notify_all();
        if (on_written_ && !on_written_(written)) {
            std::lock_guard<std::mutex> lock(mutex_);
            writer_error_ = EIO;
            aborted_ = true;
            changed_.notify_all();
            return;
        }
    }
}

void ReceivePipe::abort() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        aborted_ = true;
    }
    changed_.notify_all();
}

/**
 * @brief Receive data from the socket and store it into the specified file descriptor.
 *
 * This function pulls up to `count` bytes from the given socket file descriptor
 * into the receive pipe and then writes it to the specified file descriptor.
 * In live relay mode, it also duplicates the data to the forward pipe for forwarding.
 * 
 * @param socket_fd 
 * @param file_fd 
 * @param count 
 * @return ssize_t 
 */
ssize_t ReceivePipe::receive_to_file(int socket_fd, int file_fd, size_t count) {
    if (!valid_ || (relay_ && !writer_.joinable())) {
        errno = EINVAL;
        return -1;
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (aborted_ || writer_error_) {
            errno = writer_error_ ? writer_error_ : ECANCELED;
            return -1;
        }
    }

    ssize_t pulled;
    do {
        // Pull data from the socket into the receive pipe. This may block until
        // some data is available or an error occurs.
        pulled = ::splice(socket_fd, nullptr, receive_fds_[1], nullptr, count, 0);
    } while (pulled < 0 && errno == EINTR);
    if (pulled <= 0) return pulled;

    size_t remaining = static_cast<size_t>(pulled);

    while (remaining > 0) {
        size_t to_store = remaining;
        if (relay_) {
            // uint64_t forwarded_before;
            bool fallback;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                if (aborted_ || writer_error_) {
                    errno = writer_error_ ? writer_error_ : ECANCELED;
                    return -1;
                }
                // forwarded_before = forwarded_;
                fallback = fallback_;
            }

            /*
             * Copy pipe references for downstream forwarding. If that pipe
             * fills, remember where forwarding must continue from .part.
             */
            if (!fallback) {
                ssize_t duplicated;
                do {
                    duplicated = ::tee(
                                        receive_fds_[0], 
                                        forward_fds_[1], 
                                        remaining,
                                        SPLICE_F_NONBLOCK
                                    );
                } while (duplicated < 0 && errno == EINTR);

                if (duplicated < 0 && errno == EAGAIN) {
                    {
                        std::lock_guard<std::mutex> lock(mutex_);
                        fallback_ = true;
                        fallback_offset_ = published_;
                    }
                    changed_.notify_all();

                    continue;
                }

                if (duplicated <= 0) {
                    if (duplicated == 0) errno = EIO;
                    return -1;
                }
                
                to_store = static_cast<size_t>(duplicated);

                // The forwarder may drain B while the file writer drains its
                // own queued pipe into .part.
                
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    published_ += to_store;
                }
                changed_.notify_all();


            }
        }

        /*
         *The non-relay path retains its direct write. In relay mode the
         * writer thread applies this delay without holding up socket reads.
         */
        if (!relay_ && test_write_delay_us_ > 0) {
            std::this_thread::sleep_for(std::chrono::microseconds(test_write_delay_us_));
        }

        size_t stored = 0;
        while (stored < to_store) {

            uint64_t before = 0;
            if (relay_) {
                std::lock_guard<std::mutex> lock(mutex_);
                if (aborted_ || writer_error_) {
                    errno = writer_error_ ? writer_error_ : ECANCELED;
                    return -1;
                }
                before = written_;
            }

            /*
             * Move the local branch into the writer's pipe. Wait if it fills
             * until the writer makes room.
             */
            ssize_t n = ::splice(receive_fds_[0], nullptr,
                                 relay_ ? file_fds_[1] : file_fd, nullptr,
                                 to_store - stored,
                                 relay_ ? SPLICE_F_NONBLOCK : 0);
            
            if (n < 0 && errno == EINTR) 
                continue;

            if (n < 0 && relay_ && errno == EAGAIN) {
                std::unique_lock<std::mutex> lock(mutex_);
                changed_.wait(lock, [&] {
                    return writer_error_ || aborted_ || written_ != before;
                });
                if (writer_error_ || aborted_) {
                    errno = writer_error_ ? writer_error_ : ECANCELED;
                    return -1;
                }
                continue;
            }

            if (n <= 0) {
                if (n == 0) errno = EIO;
                return -1;
            }
            stored += static_cast<size_t>(n);

            if (relay_) {
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    queued_ += static_cast<uint64_t>(n);
                }
                changed_.notify_all();
            }
        }

        remaining -= to_store;
    }

    return pulled;
}

/**
 * @brief Forward data from the receive pipe to the specified socket.
 * 
 * This function attempts to forward up to `count` bytes from the receive pipe
 * to the given socket file descriptor. It blocks until the socket is ready for writing
 * or the operation is aborted.
 * 
 * @param socket_fd 
 * @param count 
 * @return ssize_t 
 */
ssize_t ReceivePipe::forward_to_socket(int socket_fd, size_t count) {
    if (!valid_ || !relay_) {
        errno = EINVAL;
        return -1;
    }

    size_t ready;
    {
        std::unique_lock<std::mutex> lock(mutex_);
        /*
         * Wait for live bytes or a fallback request. Once the live pipe is
         * drained, return zero so the forwarder can switch to .part.
         */
        changed_.wait(lock, [&] {
            return aborted_ || published_ > forwarded_ || fallback_;
        });
        if (aborted_) {
            errno = ECANCELED;
            return -1;
        }
        if (published_ == forwarded_) return 0;
        ready = static_cast<size_t>(std::min<uint64_t>(count, published_ - forwarded_));
    }

    ssize_t n;
    do {
        // Attempt to forward data from the pipe to the socket. This may block until the socket is ready for writing.
        n = ::splice(forward_fds_[0], nullptr, socket_fd, nullptr, ready, 0);
    } while (n < 0 && errno == EINTR);

    if (n > 0) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            forwarded_ += static_cast<uint64_t>(n);
        }
        changed_.notify_all();
    }
    return n;
}

/*
 * Give the forwarder the exact offset where pipe forwarding stopped.
 */
bool ReceivePipe::fallback_offset(uint64_t& offset) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!fallback_) {
        return false;
    }
    offset = fallback_offset_;
    return true;
}

} // namespace flashshare
#endif
