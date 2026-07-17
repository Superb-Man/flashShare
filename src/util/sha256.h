#pragma once

#include <string>
#include <cstdint>
#include <vector>

namespace flashshare {

class SHA256 {
public:
    // Compute SHA-256 of a file (returns hex string)
    static std::string file_hash(const std::string& filepath);

    // Compute SHA-256 of a buffer
    static std::string buffer_hash(const uint8_t* data, size_t len);

    // Compute SHA-256 of a file, but only from offset to offset+length (for resume verification)
    static std::string file_hash_range(const std::string& filepath, uint64_t offset, uint64_t length);

private:
    static std::string to_hex(const unsigned char* hash, size_t len);
};

} // namespace flashshare
