#pragma once

#include <cstdint>
#include <vector>
#include <string>

namespace flashshare {

/**
 * AES-256-GCM authenticated encryption using OpenSSL EVP.
 * Provides confidentiality + integrity for file data chunks.
 */
class Cipher {
public:
    Cipher();
    ~Cipher();

    // Initialize with a 256-bit key and 96-bit IV
    bool init(const uint8_t key[32], const uint8_t iv[12]);

    // Encrypt plaintext -> ciphertext (includes 16-byte GCM tag appended)
    bool encrypt(const uint8_t* plaintext, size_t len, std::vector<uint8_t>& ciphertext);

    // Decrypt ciphertext (with appended 16-byte GCM tag) -> plaintext
    bool decrypt(const uint8_t* ciphertext, size_t len, std::vector<uint8_t>& plaintext);

    // Generate a random key
    static void generate_key(uint8_t key[32]);

    // Generate a random IV
    static void generate_iv(uint8_t iv[12]);

    bool is_initialized() const { return initialized_; }

private:
    bool initialized_;
    uint8_t key_[32];
    uint8_t iv_[12];
    uint64_t counter_; // Incremented per encryption for nonce uniqueness
};

} // namespace flashshare
