#include "transfer/receive_pipe.h"

#ifdef __linux__

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <fcntl.h>
#include <thread>
#include <unistd.h>

// Receives unencrypted bytes through a kernel pipe. In relay mode tee()
// duplicates pipe-buffer references before either branch is consumed.

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
    valid_ = true;
}

ReceivePipe::~ReceivePipe() {
    for (int fd : receive_fds_) if (fd >= 0) ::close(fd);
    for (int fd : forward_fds_) if (fd >= 0) ::close(fd);
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
    if (!valid_) {
        errno = EINVAL;
        return -1;
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (aborted_) {
            errno = ECANCELED;
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
            uint64_t forwarded_before;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                if (aborted_) {
                    errno = ECANCELED;
                    return -1;
                }
                forwarded_before = forwarded_;
            }

            ssize_t duplicated;
            do {
                duplicated = ::tee(receive_fds_[0], forward_fds_[1], remaining,
                                   SPLICE_F_NONBLOCK);
            } while (duplicated < 0 && errno == EINTR);

            if (duplicated < 0 && errno == EAGAIN) {
                std::unique_lock<std::mutex> lock(mutex_);
                // Wait until either the pipe has been forwarded further or the operation is aborted.
                // This ensures that we do not busy-wait when the pipe is full and need to wait for forwarding progress.
                changed_.wait(lock, [&] {
                    return aborted_ || forwarded_ > forwarded_before;
                });

                if (aborted_) {
                    errno = ECANCELED;
                    return -1;
                }
                continue;
            }
            // If we reach here, the tee operation succeeded and we can proceed to store the duplicated data.
            if (duplicated <= 0) {
                if (duplicated == 0) errno = EIO;
                return -1;
            }
            to_store = static_cast<size_t>(duplicated);

            // The forwarder may start splicing this branch while the receive
            // thread writes the other branch to the partial file.
            {
                std::lock_guard<std::mutex> lock(mutex_);
                published_ += to_store;
            }
            changed_.notify_all();
        }

        // Test-only delay on the .part write. Pipe B was already published,
        // so live forwarding can proceed while this receive thread waits.
        if (test_write_delay_us_ > 0) {
            std::this_thread::sleep_for(std::chrono::microseconds(test_write_delay_us_));
        }

        size_t stored = 0;
        while (stored < to_store) {
            ssize_t n = ::splice(receive_fds_[0], nullptr, file_fd, nullptr, to_store - stored, 0);

            if (n < 0 && errno == EINTR) 
                continue;

            if (n <= 0) {
                if (n == 0) errno = EIO;
                return -1;
            }
            stored += static_cast<size_t>(n);
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
        changed_.wait(lock, [&] {
            return aborted_ || published_ > forwarded_;
        });
        if (aborted_) {
            errno = ECANCELED;
            return -1;
        }
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

} // namespace flashshare
#endif
