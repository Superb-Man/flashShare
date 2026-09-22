#pragma once

#include "net/socket.h"
#include "crypto/cipher.h"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace flashshare {

class RelayConnection;

constexpr uint32_t PROTOCOL_VERSION = 2;
constexpr uint16_t DEFAULT_PORT = 5117;
constexpr size_t CHUNK_SIZE = 256 * 1024;
constexpr size_t MAX_MANIFEST_SIZE = 16 * 1024 * 1024;

enum class MessageType : uint8_t {
    TRANSFER_REQUEST  = 1,
    TRANSFER_ACCEPT   = 2,
    TRANSFER_REJECT   = 3,
    FILE_HEADER       = 4,
    FILE_DATA         = 5,
    FILE_COMPLETE     = 6,
    TRANSFER_COMPLETE = 7,
    TRANSFER_ERROR    = 8,
    RESUME_REQUEST    = 9,
    RESUME_RESPONSE   = 10,
    RELAY_DISCOVERY_REQUEST  = 11,
    RELAY_DISCOVERY_RESPONSE = 12,
    RELAY_ASSIGNMENT_REQUEST = 13,
    RELAY_ASSIGNMENT_RESPONSE = 14
};

struct FileEntry {
    std::string relpath;
    std::string abspath;
    uint64_t size;
    std::string sha256;
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

    // A completed file returns its complete size.
    std::vector<uint64_t> resume_offsets;

    std::string message;
};

/**
 * Connection manages the application protocol on a TCP socket.
 */
class Connection {
public:
    explicit Connection(Socket socket);
    ~Connection();

    bool send_transfer_request(const TransferRequest& req);

    // RelayConnection::recv_initial_request() to dispatch transfer and relay
    // control messages arriving on the same listening port.
    // Obsolete
    bool recv_transfer_request(TransferRequest& req);

    bool send_transfer_response(const TransferResponse& resp);
    bool recv_transfer_response(TransferResponse& resp);

    /*
     * full_file_size is always the complete original size.
     * file_offset says where this connection starts sending.
     *
     * Receiver verifies file_offset equals its journal's durable_bytes before
     * accepting raw bytes. This prevents an accidental overwrite or append at
     * the wrong position after reconnecting.
     */
    bool send_file_header(uint32_t file_index, uint64_t full_file_size, uint64_t file_offset, const std::string& filename);

    bool recv_file_header(uint32_t& file_index, uint64_t& full_file_size, uint64_t& file_offset, std::string& filename);

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

    bool send_frame(MessageType type, const void* payload, size_t len);
    bool recv_frame(MessageType& type, std::vector<uint8_t>& payload);

    std::string serialize_request(const TransferRequest& req);
    bool deserialize_request(const std::string& json, TransferRequest& req);
    std::string serialize_response(const TransferResponse& resp);
    bool deserialize_response(const std::string& json, TransferResponse& resp);

    // RelayConnection adds relay-specific protocol messages without making
    // Connection a polymorphic base class.
    friend class RelayConnection;
};

} // namespace flashshare
