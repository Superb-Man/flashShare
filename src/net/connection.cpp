#include "net/connection.h"
#include "util/logger.h"

#include <cstring>
#include <sstream>
#include <cstdlib>

namespace flashshare {

static std::string json_escape(const std::string& s) {
    std::string out;
    out.reserve(s.size() + 8);
    for (char c : s) {
        switch (c) {
            case '"':  out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default:
                if (static_cast<unsigned char>(c) < 0x20) {
                    char buf[8];
                    snprintf(buf, sizeof(buf), "\\u%04x", c);
                    out += buf;
                } else {
                    out += c;
                }
        }
    }
    return out;
}

static std::string json_unescape(const std::string& s) {
    std::string out;
    out.reserve(s.size());
    for (size_t i = 0; i < s.size(); ++i) {
        if (s[i] == '\\' && i + 1 < s.size()) {
            switch (s[i + 1]) {
                case '"':  out += '"';  ++i; break;
                case '\\': out += '\\'; ++i; break;
                case 'n':  out += '\n'; ++i; break;
                case 'r':  out += '\r'; ++i; break;
                case 't':  out += '\t'; ++i; break;
                default:   out += s[i]; break;
            }
        } else {
            out += s[i];
        }
    }
    return out;
}

// Extract string value for a key from simple JSON
static std::string json_get_string(const std::string& json, const std::string& key) {
    std::string search = "\"" + key + "\":\"";
    size_t pos = json.find(search);
    if (pos == std::string::npos) return "";
    pos += search.size();
    size_t end = pos;
    while (end < json.size() && json[end] != '"') {
        if (json[end] == '\\' && end + 1 < json.size()) end += 2;
        else ++end;
    }
    return json_unescape(json.substr(pos, end - pos));
}

// Extract uint64 value for a key
static uint64_t json_get_u64(const std::string& json, const std::string& key) {
    std::string search = "\"" + key + "\":";
    size_t pos = json.find(search);
    if (pos == std::string::npos) return 0;
    pos += search.size();
    // Skip whitespace
    while (pos < json.size() && (json[pos] == ' ' || json[pos] == '\t')) ++pos;
    return strtoull(json.c_str() + pos, nullptr, 10);
}

// Extract bool value
static bool json_get_bool(const std::string& json, const std::string& key) {
    std::string search = "\"" + key + "\":";
    size_t pos = json.find(search);
    if (pos == std::string::npos) return false;
    pos += search.size();
    while (pos < json.size() && (json[pos] == ' ' || json[pos] == '\t')) ++pos;
    return (pos < json.size() && json[pos] == 't');
}

// Extract array of objects with relpath, size, sha256
static std::vector<FileEntry> json_get_files(const std::string& json) {
    std::vector<FileEntry> files;
    size_t pos = json.find("\"files\"");
    if (pos == std::string::npos) return files;
    pos = json.find('[', pos);
    if (pos == std::string::npos) return files;

    while (true) {
        pos = json.find('{', pos);
        if (pos == std::string::npos) break;

        size_t obj_end = json.find('}', pos);
        if (obj_end == std::string::npos) break;

        std::string obj = json.substr(pos, obj_end - pos + 1);
        FileEntry entry;
        entry.relpath = json_get_string(obj, "relpath");
        entry.abspath = json_get_string(obj, "abspath");
        entry.size = json_get_u64(obj, "size");
        entry.sha256 = json_get_string(obj, "sha256");
        files.push_back(entry);

        pos = obj_end + 1;
        // Check if there's another element
        size_t next = json.find_first_of(",]", pos);
        if (next == std::string::npos || json[next] == ']') break;
        pos = next + 1;
    }
    return files;
}

// Extract array of uint64 (resume offsets)
static std::vector<uint64_t> json_get_offsets(const std::string& json) {
    std::vector<uint64_t> offsets;
    size_t pos = json.find("\"resume_offsets\"");
    if (pos == std::string::npos) return offsets;
    pos = json.find('[', pos);
    if (pos == std::string::npos) return offsets;

    while (true) {
        ++pos;
        while (pos < json.size() && (json[pos] == ' ' || json[pos] == '\t')) ++pos;
        if (pos >= json.size() || json[pos] == ']') break;
        uint64_t val = strtoull(json.c_str() + pos, nullptr, 10);
        offsets.push_back(val);
        size_t next = json.find_first_of(",]", pos);
        if (next == std::string::npos || json[next] == ']') break;
        pos = next;
    }
    return offsets;
}


Connection::Connection(Socket socket) : socket_(std::move(socket)) {}

Connection::~Connection() {}

void Connection::tune_socket() {
    socket_.set_buffer_size(4 * 1024 * 1024, 4 * 1024 * 1024);
    socket_.set_nodelay(true);
    socket_.set_cork(true);
}

bool Connection::send_frame(MessageType type, const void* payload, size_t len) {
    // [4 bytes: total_len (including msg_type byte)][1 byte: msg_type][payload]
    uint32_t total_len = static_cast<uint32_t>(1 + len);
    if (!socket_.send_all(&total_len, 4)) {
        return false;
    }
    uint8_t t = static_cast<uint8_t>(type);
    if (!socket_.send_all(&t, 1)) {
        return false;
    }
    if (len > 0 && payload) {
        if (!socket_.send_all(payload, len)) {
            return false;
        }
    }
    return true;
}

bool Connection::recv_frame(MessageType& type, std::vector<uint8_t>& payload) {
    uint32_t total_len = 0;
    if (!socket_.recv_all(&total_len, 4)) {
        return false;
    }
    if (total_len == 0 || total_len > MAX_MANIFEST_SIZE + CHUNK_SIZE + 1024) {
        LOG_ERROR("Invalid frame length: %u", total_len);
        return false;
    }

    uint8_t t = 0;
    if (!socket_.recv_all(&t, 1)) {
        return false;
    }
    type = static_cast<MessageType>(t);

    size_t payload_len = total_len - 1;
    if (payload_len > 0) {
        payload.resize(payload_len);
        if (!socket_.recv_all(payload.data(), payload_len)) {
            return false;
        }
    } else {
        payload.clear();
    }
    return true;
}

bool Connection::send_message(MessageType type) {
    return send_frame(type, nullptr, 0);
}

bool Connection::recv_message(MessageType& type) {
    std::vector<uint8_t> payload;
    return recv_frame(type, payload);
}

std::string Connection::serialize_request(const TransferRequest& req) {
    std::ostringstream ss;
    ss << "{";
    ss << "\"version\":" << req.version << ",";
    ss << "\"transfer_id\":\"" << json_escape(req.transfer_id) << "\",";
    ss << "\"total_size\":" << req.total_size << ",";
    ss << "\"encrypted\":" << (req.encrypted ? "true" : "false") << ",";
    ss << "\"resume\":" << (req.resume ? "true" : "false") << ",";
    ss << "\"files\":[";
    for (size_t i = 0; i < req.files.size(); ++i) {
        const auto& f = req.files[i];
        ss << "{";
        ss << "\"relpath\":\"" << json_escape(f.relpath) << "\",";
        ss << "\"abspath\":\"" << json_escape(f.abspath) << "\",";
        ss << "\"size\":" << f.size << ",";
        ss << "\"sha256\":\"" << json_escape(f.sha256) << "\"";
        ss << "}";
        if (i + 1 < req.files.size()) ss << ",";
    }
    ss << "]}";
    return ss.str();
}

bool Connection::deserialize_request(const std::string& json, TransferRequest& req) {
    req.version = static_cast<uint32_t>(json_get_u64(json, "version"));
    req.transfer_id = json_get_string(json, "transfer_id");
    req.total_size = json_get_u64(json, "total_size");
    req.encrypted = json_get_bool(json, "encrypted");
    req.resume = json_get_bool(json, "resume");
    req.files = json_get_files(json);
    return true;
}

std::string Connection::serialize_response(const TransferResponse& resp) {
    std::ostringstream ss;
    ss << "{";
    ss << "\"accepted\":" << (resp.accepted ? "true" : "false") << ",";
    ss << "\"message\":\"" << json_escape(resp.message) << "\",";
    ss << "\"resume_offsets\":[";
    for (size_t i = 0; i < resp.resume_offsets.size(); ++i) {
        ss << resp.resume_offsets[i];
        if (i + 1 < resp.resume_offsets.size()) ss << ",";
    }
    ss << "]}";
    return ss.str();
}

bool Connection::deserialize_response(const std::string& json, TransferResponse& resp) {
    resp.accepted = json_get_bool(json, "accepted");
    resp.message = json_get_string(json, "message");
    resp.resume_offsets = json_get_offsets(json);
    return true;
}

bool Connection::send_transfer_response(const TransferResponse& resp) {
    std::string json = serialize_response(resp);
    MessageType type = resp.accepted ? MessageType::TRANSFER_ACCEPT : MessageType::TRANSFER_REJECT;

    return send_frame(type, json.data(), json.size());
}


bool Connection::send_transfer_request(const TransferRequest& req) {
    std::string json = serialize_request(req);

    LOG_DEBUG("Sending transfer request: %zu files, %llu bytes", req.files.size(), static_cast<unsigned long long>(req.total_size));

    return send_frame(
        MessageType::TRANSFER_REQUEST,
        json.data(),
        json.size()
    );
}

bool Connection::recv_transfer_request(TransferRequest& req) {
    MessageType type;
    std::vector<uint8_t> payload;

    if (!recv_frame(type, payload)) {
        return false;
    }

    if (type != MessageType::TRANSFER_REQUEST) {
        LOG_ERROR("Expected TRANSFER_REQUEST, got %d", static_cast<int>(type));
        return false;
    }

    std::string json(payload.begin(), payload.end());
    return deserialize_request(json, req);
}

bool Connection::recv_transfer_response(TransferResponse& resp) {
    MessageType type;
    std::vector<uint8_t> payload;

    if (!recv_frame(type, payload)) return false;

    if (type != MessageType::TRANSFER_ACCEPT && type != MessageType::TRANSFER_REJECT) {
        LOG_ERROR("Expected TRANSFER_ACCEPT/REJECT, got %d", static_cast<int>(type));
        return false;
    }

    std::string json(payload.begin(), payload.end());
    if (!deserialize_response(json, resp)) {
        return false;
    }

    // The message type is authoritative even if a malformed peer says
    // `"accepted": true` in a rejection payload.
    if (type == MessageType::TRANSFER_REJECT) {
        resp.accepted = false;
    }

    return true;
}

bool Connection::send_file_header(uint32_t file_index, uint64_t full_file_size, uint64_t file_offset, const std::string& filename) {
    // Payload:
    // [4 bytes: file_index]
    // [8 bytes: complete original file size]
    // [8 bytes: resume/send offset]
    // [2 bytes: filename length]
    // [n bytes: filename]
    size_t name_len = filename.size();
    if (name_len > 4096) name_len = 4096;

    std::vector<uint8_t> payload(4 + 8 + 8 + 2 + name_len);

    memcpy(payload.data(), &file_index, 4);
    memcpy(payload.data() + 4, &full_file_size, 8);
    memcpy(payload.data() + 12, &file_offset, 8);

    uint16_t nlen = static_cast<uint16_t>(name_len);
    memcpy(payload.data() + 20, &nlen, 2);
    memcpy(payload.data() + 22, filename.data(), name_len);

    return send_frame(MessageType::FILE_HEADER, payload.data(), payload.size());
}

bool Connection::recv_file_header(uint32_t& file_index, uint64_t& full_file_size, uint64_t& file_offset, std::string& filename) {
    MessageType type;
    std::vector<uint8_t> payload;

    if (!recv_frame(type, payload)) return false;

    if (type != MessageType::FILE_HEADER) {
        LOG_ERROR("Expected FILE_HEADER, got %d", static_cast<int>(type));
        return false;
    }

    constexpr size_t FIXED_HEADER_SIZE = 4 + 8 + 8 + 2;

    if (payload.size() < FIXED_HEADER_SIZE) {
        LOG_ERROR("FILE_HEADER payload too small");
        return false;
    }

    memcpy(&file_index, payload.data(), 4);
    memcpy(&full_file_size, payload.data() + 4, 8);
    memcpy(&file_offset, payload.data() + 12, 8);

    uint16_t nlen = 0;
    memcpy(&nlen, payload.data() + 20, 2);

    if (payload.size() != FIXED_HEADER_SIZE + nlen) {
        LOG_ERROR("Invalid FILE_HEADER filename length");
        return false;
    }

    if (file_offset > full_file_size) {
        LOG_ERROR("Invalid FILE_HEADER offset: %llu > %llu",
                  static_cast<unsigned long long>(file_offset),
                  static_cast<unsigned long long>(full_file_size));
        return false;
    }

    filename.assign(reinterpret_cast<const char*>(payload.data() + 22), nlen);
    return true;
}

bool Connection::send_data_chunk(const void* data, size_t len, uint32_t seq) {
    if (cipher_) {
        // Encrypt: [4 bytes: seq][2 bytes: ciphertext_len][ciphertext + 16-byte GCM tag]
        std::vector<uint8_t> ciphertext;
        if (!cipher_->encrypt(static_cast<const uint8_t*>(data), len, ciphertext)) {
            LOG_ERROR("Encryption failed for chunk %u", seq);
            return false;
        }
        // Payload: [4 bytes: seq][2 bytes: len][ciphertext]
        size_t payload_size = 4 + 2 + ciphertext.size();
        std::vector<uint8_t> payload(payload_size);
        memcpy(payload.data(), &seq, 4);
        uint16_t clen = static_cast<uint16_t>(ciphertext.size());
        memcpy(payload.data() + 4, &clen, 2);
        memcpy(payload.data() + 6, ciphertext.data(), ciphertext.size());

        return send_frame(MessageType::FILE_DATA, payload.data(), payload.size());
    } else {
        // Plaintext: [4 bytes: seq][2 bytes: len][data]
        size_t payload_size = 4 + 2 + len;
        std::vector<uint8_t> payload(payload_size);
        memcpy(payload.data(), &seq, 4);
        uint16_t dlen = static_cast<uint16_t>(len);
        memcpy(payload.data() + 4, &dlen, 2);
        memcpy(payload.data() + 6, data, len);

        return send_frame(MessageType::FILE_DATA, payload.data(), payload.size());
    }
}

bool Connection::recv_data_chunk(std::vector<uint8_t>& data, uint32_t& seq) {
    MessageType type;
    std::vector<uint8_t> payload;
    if (!recv_frame(type, payload)) return false;
    if (type != MessageType::FILE_DATA) {
        LOG_ERROR("Expected FILE_DATA, got %d", static_cast<int>(type));
        return false;
    }
    if (payload.size() < 6) {
        LOG_ERROR("FILE_DATA payload too small");
        return false;
    }
    memcpy(&seq, payload.data(), 4);
    uint16_t dlen = 0;
    memcpy(&dlen, payload.data() + 4, 2);

    if (cipher_) {
        // Decrypt
        std::vector<uint8_t> ciphertext(payload.begin() + 6, payload.end());
        if (!cipher_->decrypt(ciphertext.data(), ciphertext.size(), data)) {
            LOG_ERROR("Decryption failed for chunk %u", seq);
            return false;
        }
    } else {
        data.assign(payload.begin() + 6, payload.begin() + 6 + dlen);
    }
    return true;
}

void Connection::set_cipher(std::unique_ptr<Cipher> cipher) {
    cipher_ = std::move(cipher);
}

} // namespace flashshare
