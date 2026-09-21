#pragma once

#ifdef _WIN32
#include <io.h>
#else
#include <unistd.h>
#endif

namespace flashshare {

// RAII wrapper that closes a raw fd when it goes out of scope.
class ScopedFile {
public:
    explicit ScopedFile(int fd) : fd_(fd) {}
    ~ScopedFile() {
        if (fd_ >= 0) {
            ::close(fd_);
        }
    }

    ScopedFile(const ScopedFile&) = delete;
    ScopedFile& operator=(const ScopedFile&) = delete;

    int get() const { return fd_; }

private:
    int fd_ = -1;
};

} // namespace flashshare
