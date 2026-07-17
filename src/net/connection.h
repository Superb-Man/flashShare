#pragma once

#include "net/socket.h"
#include "crypto/cipher.h"
#include <string>
#include <cstdint>
#include <vector>
#include <functional>
#include <memory>

namespace flashshare {

// Protocol constants
constexpr uint32_t PROTOCOL_VERSION = 1;
constexpr uint16_t DEFAULT_PORT = 5117;
constexpr size_t CHUNK_SIZE = 256 * 1024; // 256 KB chunks
constexpr size_t MAX_MANIFEST_SIZE = 16 * 1024 * 1024; // 16 MB max manifest

enum class MessageType : uint8_t {
    TRANSFER_REQUEST = 1,
    TRANSFER_ACCEPT  = 2,
    TRANSFER_REJECT  = 3,
    FILE_HEADER      = 4,
    FILE_DATA        = 5,
    FILE_COMPLETE     = 6,
    TRANSFER_COMPLETE = 7,
    TRANSFER_ERROR    = 8,
    RESUME_REQUEST   = 9,
    RESUME_RESPONSE  = 10,
};

struct FileEntry {
    std::string relpath;    // Relative path within transfer
    std::string abspath;    // Absolute path on sender
    uint64_t size;          // File size in bytes
    std::string sha256;     // SHA-256 hash (empty if not computed yet)
};

struct TransferRequest {
    uint32_t version;
    std::string transfer_id;
    std::vector<FileEntry> files;
    uint64_t total_size;
    bool encrypted;
    bool resume;
};

struct TransferResponse {
    bool accepted;
    std::vector<uint64_t> resume_offsets; // Per-file offset to resume from
};

/**
 * Connection manages the protocol over a Socket.
 * Handles framing, handshake, and data transfer.
 */
class Connection {
public:
    explicit Connection(Socket socket);
    ~Connection();

    bool send_transfer_request(const TransferRequest& req);
    bool recv_transfer_request(TransferRequest& req);
    bool send_transfer_response(const TransferResponse& resp);
    bool recv_transfer_response(TransferResponse& resp);
    bool send_file_header(uint32_t file_index, uint64_t file_size, const std::string& filename);
    bool recv_file_header(uint32_t& file_index, uint64_t& file_size, std::string& filename);
    bool send_data_chunk(const void* data, size_t len, uint32_t seq);
    bool recv_data_chunk(std::vector<uint8_t>& data, uint32_t& seq);
    bool send_message(MessageType type);
    bool recv_message(MessageType& type);
    void set_cipher(std::unique_ptr<Cipher> cipher);

    bool is_encrypted() const { return cipher_ != nullptr; }

    Socket& socket() { return socket_; }

    void tune_socket();

private:
    Socket socket_;
    std::unique_ptr<Cipher> cipher_;

    // Framing: [4 bytes: total_len][1 byte: msg_type][payload]
    bool send_frame(MessageType type, const void* payload, size_t len);
    bool recv_frame(MessageType& type, std::vector<uint8_t>& payload);

    std::string serialize_request(const TransferRequest& req);
    bool deserialize_request(const std::string& json, TransferRequest& req);
    std::string serialize_response(const TransferResponse& resp);
    bool deserialize_response(const std::string& json, TransferResponse& resp);
};

}
