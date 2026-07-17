#pragma once

#include "crypto/cipher.h"
#include <cstdint>
#include <vector>
#include <string>

namespace flashshare {

/**
 * X25519 ECDH key exchange for establishing a shared secret.
 * The shared secret is then used to derive an AES-256 key and IV.
 */
class KeyExchange {
public:
    KeyExchange();
    ~KeyExchange();

    // Generate a new X25519 keypair
    bool generate_keypair();

    // Get our public key (32 bytes) to send to peer
    const std::vector<uint8_t>& public_key() const { return public_key_; }

    // Set peer's public key and compute shared secret
    bool set_peer_public_key(const uint8_t peer_pub[32]);

    // Derive AES-256 key and 12-byte IV from shared secret using HKDF
    bool derive_session_key(uint8_t key[32], uint8_t iv[12]);

    // Create a Cipher from the derived key
    std::unique_ptr<Cipher> create_cipher();

    // Get our public key as hex string (for display)
    std::string public_key_hex() const;

    // Set peer public key from hex string
    bool set_peer_public_key_hex(const std::string& hex);

private:
    std::vector<uint8_t> private_key_;  // 32 bytes
    std::vector<uint8_t> public_key_;   // 32 bytes
    std::vector<uint8_t> shared_secret_; // 32 bytes
    bool has_shared_secret_;
};

} // namespace flashshare
