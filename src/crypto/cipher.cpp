#include "crypto/cipher.h"
#include "util/logger.h"

#include <openssl/evp.h>
#include <openssl/rand.h>
#include <openssl/err.h>

#include <cstring>

namespace flashshare {

Cipher::Cipher() : initialized_(false), counter_(0) {
    memset(key_, 0, sizeof(key_));
    memset(iv_, 0, sizeof(iv_));
}

Cipher::~Cipher() {
    // Securely zero key material
    memset(key_, 0, sizeof(key_));
    memset(iv_, 0, sizeof(iv_));
}

bool Cipher::init(const uint8_t key[32], const uint8_t iv[12]) {
    memcpy(key_, key, 32);
    memcpy(iv_, iv, 12);
    counter_ = 0;
    initialized_ = true;
    return true;
}

bool Cipher::encrypt(const uint8_t* plaintext, size_t len, std::vector<uint8_t>& ciphertext) {
    if (!initialized_) {
        LOG_ERROR("Cipher not initialized");
        return false;
    }

    EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
    if (!ctx) {
        LOG_ERROR("Failed to create EVP_CIPHER_CTX");
        return false;
    }

    bool ok = false;
    int outlen = 0;
    int finallen = 0;

    // Construct unique nonce: IV (12 bytes) with counter embedded in last 8 bytes
    uint8_t nonce[12];
    memcpy(nonce, iv_, 4); // First 4 bytes of IV
    uint64_t c = counter_++;
    memcpy(nonce + 4, &c, 8); // Last 8 bytes = counter

    if (EVP_EncryptInit_ex(ctx, EVP_aes_256_gcm(), nullptr, key_, nonce) != 1) {
        LOG_ERROR("EVP_EncryptInit_ex failed");
        goto cleanup;
    }

    ciphertext.resize(len + 16); // +16 for GCM tag

    if (EVP_EncryptUpdate(ctx, ciphertext.data(), &outlen, plaintext, static_cast<int>(len)) != 1) {
        LOG_ERROR("EVP_EncryptUpdate failed");
        goto cleanup;
    }

    if (EVP_EncryptFinal_ex(ctx, ciphertext.data() + outlen, &finallen) != 1) {
        LOG_ERROR("EVP_EncryptFinal_ex failed");
        goto cleanup;
    }

    outlen += finallen;

    // Get GCM tag (16 bytes) and append
    if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_GET_TAG, 16, ciphertext.data() + outlen) != 1) {
        LOG_ERROR("EVP_CTRL_GCM_GET_TAG failed");
        goto cleanup;
    }

    ciphertext.resize(outlen + 16);
    ok = true;

cleanup:
    EVP_CIPHER_CTX_free(ctx);
    return ok;
}

bool Cipher::decrypt(const uint8_t* ciphertext, size_t len, std::vector<uint8_t>& plaintext) {
    if (!initialized_) {
        LOG_ERROR("Cipher not initialized");
        return false;
    }

    if (len < 16) {
        LOG_ERROR("Ciphertext too short for GCM tag");
        return false;
    }

    EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
    if (!ctx) {
        LOG_ERROR("Failed to create EVP_CIPHER_CTX");
        return false;
    }

    bool ok = false;
    int outlen = 0;
    int finallen = 0;

    // Reconstruct nonce (we use the same counter sequence — both sides must be in sync)
    uint8_t nonce[12];
    memcpy(nonce, iv_, 4);
    uint64_t c = counter_++;
    memcpy(nonce + 4, &c, 8);

    size_t ct_len = len - 16;
    const uint8_t* tag = ciphertext + ct_len;

    if (EVP_DecryptInit_ex(ctx, EVP_aes_256_gcm(), nullptr, key_, nonce) != 1) {
        LOG_ERROR("EVP_DecryptInit_ex failed");
        goto cleanup;
    }

    plaintext.resize(ct_len);

    if (EVP_DecryptUpdate(ctx, plaintext.data(), &outlen, ciphertext, static_cast<int>(ct_len)) != 1) {
        LOG_ERROR("EVP_DecryptUpdate failed");
        goto cleanup;
    }

    // Set expected tag
    if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_TAG, 16, const_cast<uint8_t*>(tag)) != 1) {
        LOG_ERROR("EVP_CTRL_GCM_SET_TAG failed");
        goto cleanup;
    }

    if (EVP_DecryptFinal_ex(ctx, plaintext.data() + outlen, &finallen) != 1) {
        LOG_ERROR("Authentication failed — data may be tampered");
        goto cleanup;
    }

    outlen += finallen;
    plaintext.resize(outlen);
    ok = true;

cleanup:
    EVP_CIPHER_CTX_free(ctx);
    return ok;
}

void Cipher::generate_key(uint8_t key[32]) {
    RAND_bytes(key, 32);
}

void Cipher::generate_iv(uint8_t iv[12]) {
    RAND_bytes(iv, 12);
}

} // namespace flashshare
