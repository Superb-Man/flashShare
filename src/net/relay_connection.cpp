#include "net/relay_connection.h"

#include "util/logger.h"

#include <cstdlib>
#include <sstream>
#include <vector>

namespace flashshare {

namespace {

std::string json_escape(const std::string& value) {
    std::string escaped;
    escaped.reserve(value.size() + 8);

    for (char ch : value) {
        switch (ch) {
            case '"': escaped += "\\\""; break;
            case '\\': escaped += "\\\\"; break;
            case '\n': escaped += "\\n"; break;
            case '\r': escaped += "\\r"; break;
            case '\t': escaped += "\\t"; break;
            default: escaped += ch; break;
        }
    }

    return escaped;
}

std::string json_unescape(const std::string& value) {
    std::string unescaped;
    unescaped.reserve(value.size());

    for (size_t i = 0; i < value.size(); ++i) {
        if (value[i] != '\\' || i + 1 >= value.size()) {
            unescaped += value[i];
            continue;
        }

        switch (value[++i]) {
            case '"': unescaped += '"'; break;
            case '\\': unescaped += '\\'; break;
            case 'n': unescaped += '\n'; break;
            case 'r': unescaped += '\r'; break;
            case 't': unescaped += '\t'; break;
            default:
                unescaped += '\\';
                unescaped += value[i];
                break;
        }
    }

    return unescaped;
}

std::string json_get_string(const std::string& json, const std::string& key) {
    const std::string prefix = "\"" + key + "\":\"";
    size_t position = json.find(prefix);
    if (position == std::string::npos) {
        return "";
    }

    position += prefix.size();
    size_t end = position;

    while (end < json.size() && json[end] != '"') {
        end += json[end] == '\\' && end + 1 < json.size() ? 2 : 1;
    }

    return json_unescape(json.substr(position, end - position));
}

uint64_t json_get_u64(const std::string& json, const std::string& key) {
    const std::string prefix = "\"" + key + "\":";
    size_t position = json.find(prefix);
    if (position == std::string::npos) {
        return 0;
    }

    position += prefix.size();
    while (position < json.size() &&
           (json[position] == ' ' || json[position] == '\t')) {
        ++position;
    }

    return std::strtoull(json.c_str() + position, nullptr, 10);
}

bool json_get_bool(const std::string& json, const std::string& key) {
    const std::string prefix = "\"" + key + "\":";
    size_t position = json.find(prefix);
    if (position == std::string::npos) {
        return false;
    }

    position += prefix.size();
    while (position < json.size() &&
           (json[position] == ' ' || json[position] == '\t')) {
        ++position;
    }

    return position < json.size() && json[position] == 't';
}

} // namespace

RelayConnection::RelayConnection(Connection& connection) : connection_(connection) {}

std::string RelayConnection::serialize_request(const RelayDiscoveryRequest& request) const {
    std::ostringstream json;
    json << "{"
         << "\"version\":" << request.version << ","
         << "\"discovery_id\":\"" << json_escape(request.discovery_id) << "\","
         << "\"total_size\":" << request.total_size << ","
         << "\"file_count\":" << request.file_count << ","
         << "\"reservation_ms\":" << request.reservation_ms
         << "}";
    return json.str();
}

bool RelayConnection::deserialize_request(const std::string& json, RelayDiscoveryRequest& request) const {
    request.version = static_cast<uint32_t>(json_get_u64(json, "version"));
    request.discovery_id = json_get_string(json, "discovery_id");
    request.total_size = json_get_u64(json, "total_size");
    request.file_count = static_cast<uint32_t>(json_get_u64(json, "file_count"));
    request.reservation_ms = static_cast<uint32_t>(
        json_get_u64(json, "reservation_ms")
    );

    return !request.discovery_id.empty();
}

std::string RelayConnection::serialize_response(const RelayDiscoveryResponse& response) const {
    std::ostringstream json;
    json << "{"
         << "\"discovery_id\":\"" << json_escape(response.discovery_id) << "\","
         << "\"available\":" << (response.available ? "true" : "false") << ","
         << "\"message\":\"" << json_escape(response.message) << "\""
         << "}";
    return json.str();
}

bool RelayConnection::deserialize_response(const std::string& json, RelayDiscoveryResponse& response) const {
    response.discovery_id = json_get_string(json, "discovery_id");
    response.available = json_get_bool(json, "available");
    response.message = json_get_string(json, "message");

    return !response.discovery_id.empty();
}

bool RelayConnection::send_discovery_request(const RelayDiscoveryRequest& request) {
    const std::string json = serialize_request(request);
    return connection_.send_frame(
        MessageType::RELAY_DISCOVERY_REQUEST,
        json.data(),
        json.size()
    );
}

bool RelayConnection::recv_discovery_response(RelayDiscoveryResponse& response) {
    MessageType type;
    std::vector<uint8_t> payload;

    if (!connection_.recv_frame(type, payload)) {
        return false;
    }

    if (type != MessageType::RELAY_DISCOVERY_RESPONSE) {
        LOG_ERROR(
            "Expected RELAY_DISCOVERY_RESPONSE, got %d",
            static_cast<int>(type)
        );
        return false;
    }

    const std::string json(payload.begin(), payload.end());
    return deserialize_response(json, response);
}

bool RelayConnection::recv_discovery_request(RelayDiscoveryRequest& request) {
    MessageType type;
    std::vector<uint8_t> payload;

    if (!connection_.recv_frame(type, payload)) {
        return false;
    }

    if (type != MessageType::RELAY_DISCOVERY_REQUEST) {
        LOG_ERROR(
            "Expected RELAY_DISCOVERY_REQUEST, got %d",
            static_cast<int>(type)
        );
        return false;
    }

    const std::string json(payload.begin(), payload.end());
    return deserialize_request(json, request);
}

bool RelayConnection::send_discovery_response(const RelayDiscoveryResponse& response) {
    const std::string json = serialize_response(response);
    return connection_.send_frame(
        MessageType::RELAY_DISCOVERY_RESPONSE,
        json.data(),
        json.size()
    );
}

bool RelayConnection::recv_initial_request(
    MessageType& type,
    TransferRequest& transfer_request,
    RelayDiscoveryRequest& discovery_request
) {
    std::vector<uint8_t> payload;
    if (!connection_.recv_frame(type, payload)) {
        return false;
    }

    const std::string json(payload.begin(), payload.end());

    switch (type) {
        case MessageType::TRANSFER_REQUEST:
            return connection_.deserialize_request(json, transfer_request);

        case MessageType::RELAY_DISCOVERY_REQUEST:
            return deserialize_request(json, discovery_request);

        default:
            LOG_ERROR(
                "Unexpected initial message type: %d",
                static_cast<int>(type)
            );
            return false;
    }
}

} // namespace flashshare
