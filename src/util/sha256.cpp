#include "util/sha256.h"
#include "util/logger.h"

#include <openssl/sha.h>
#include <openssl/evp.h>

#include <cstdio>
#include <cstring>
#include <fstream>

namespace flashshare {

std::string SHA256::to_hex(const unsigned char* hash, size_t len) {
    static const char hex[] = "0123456789abcdef";
    std::string result;
    result.reserve(len * 2);
    for (size_t i = 0; i < len; ++i) {
        result.push_back(hex[hash[i] >> 4]);
        result.push_back(hex[hash[i] & 0x0F]);
    }
    return result;
}

std::string SHA256::file_hash(const std::string& filepath) {
    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    if (!ctx) {
        LOG_ERROR("Failed to create EVP_MD_CTX");
        return "";
    }

    if (EVP_DigestInit_ex(ctx, EVP_sha256(), nullptr) != 1) {
        EVP_MD_CTX_free(ctx);
        return "";
    }

    std::ifstream file(filepath, std::ios::binary);
    if (!file.is_open()) {
        LOG_ERROR("Cannot open file for hashing: %s", filepath.c_str());
        EVP_MD_CTX_free(ctx);
        return "";
    }

    constexpr size_t BUF_SIZE = 256 * 1024; // 256 KB
    std::vector<char> buffer(BUF_SIZE);

    while (file.good()) {
        file.read(buffer.data(), BUF_SIZE);
        std::streamsize bytes = file.gcount();
        if (bytes > 0) {
            EVP_DigestUpdate(ctx, buffer.data(), static_cast<size_t>(bytes));
        }
    }

    unsigned char hash[EVP_MAX_MD_SIZE];
    unsigned int hash_len = 0;
    EVP_DigestFinal_ex(ctx, hash, &hash_len);
    EVP_MD_CTX_free(ctx);

    return to_hex(hash, hash_len);
}

std::string SHA256::buffer_hash(const uint8_t* data, size_t len) {
    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    if (!ctx) return "";

    EVP_DigestInit_ex(ctx, EVP_sha256(), nullptr);
    EVP_DigestUpdate(ctx, data, len);

    unsigned char hash[EVP_MAX_MD_SIZE];
    unsigned int hash_len = 0;
    EVP_DigestFinal_ex(ctx, hash, &hash_len);
    EVP_MD_CTX_free(ctx);

    return to_hex(hash, hash_len);
}

std::string SHA256::file_hash_range(const std::string& filepath, uint64_t offset, uint64_t length) {
    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    if (!ctx) return "";

    EVP_DigestInit_ex(ctx, EVP_sha256(), nullptr);

    std::ifstream file(filepath, std::ios::binary);
    if (!file.is_open()) {
        EVP_MD_CTX_free(ctx);
        return "";
    }

    file.seekg(static_cast<std::streamoff>(offset));
    if (!file.good()) {
        EVP_MD_CTX_free(ctx);
        return "";
    }

    constexpr size_t BUF_SIZE = 256 * 1024;
    std::vector<char> buffer(BUF_SIZE);
    uint64_t remaining = length;

    while (remaining > 0 && file.good()) {
        size_t to_read = (remaining < BUF_SIZE) ? static_cast<size_t>(remaining) : BUF_SIZE;
        file.read(buffer.data(), to_read);
        std::streamsize bytes = file.gcount();
        if (bytes <= 0) break;
        EVP_DigestUpdate(ctx, buffer.data(), static_cast<size_t>(bytes));
        remaining -= static_cast<uint64_t>(bytes);
    }

    unsigned char hash[EVP_MAX_MD_SIZE];
    unsigned int hash_len = 0;
    EVP_DigestFinal_ex(ctx, hash, &hash_len);
    EVP_MD_CTX_free(ctx);

    return to_hex(hash, hash_len);
}

} // namespace flashshare
