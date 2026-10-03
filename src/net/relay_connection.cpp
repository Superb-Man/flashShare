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

std::string RelayConnection::serialize_discovery_request(const RelayDiscoveryRequest& request) const {
    std::ostringstream json;
    json << "{"
         << "\"version\":" << request.version << ","
         << "\"discovery_id\":\"" << json_escape(request.discovery_id) << "\","
         << "\"transfer_id\":\"" << json_escape(request.transfer_id) << "\","
         << "\"total_size\":" << request.total_size << ","
         << "\"file_count\":" << request.file_count
         << "}";
    return json.str();
}

bool RelayConnection::deserialize_discovery_request(const std::string& json, RelayDiscoveryRequest& request) const {
    request.version = static_cast<uint32_t>(json_get_u64(json, "version"));
    request.discovery_id = json_get_string(json, "discovery_id");
    request.transfer_id = json_get_string(json, "transfer_id");
    request.total_size = json_get_u64(json, "total_size");
    request.file_count = static_cast<uint32_t>(json_get_u64(json, "file_count"));

    return !request.discovery_id.empty();
}

std::string RelayConnection::serialize_discovery_response(const RelayDiscoveryResponse& response) const {
    std::ostringstream json;
    json << "{"
         << "\"discovery_id\":\"" << json_escape(response.discovery_id) << "\","
         << "\"available\":" << (response.available ? "true" : "false") << ","
         << "\"offset\":" << response.offset << ","
         << "\"message\":\"" << json_escape(response.message) << "\""
         << "}";
    return json.str();
}

bool RelayConnection::deserialize_discovery_response(const std::string& json, RelayDiscoveryResponse& response) const {
    response.discovery_id = json_get_string(json, "discovery_id");
    response.available = json_get_bool(json, "available");
    response.offset = json_get_u64(json, "offset");
    response.message = json_get_string(json, "message");

    return !response.discovery_id.empty();
}

bool RelayConnection::send_discovery_request(const RelayDiscoveryRequest& request) {
    const std::string json = serialize_discovery_request(request);
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
    return deserialize_discovery_response(json, response);
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
    return deserialize_discovery_request(json, request);
}

bool RelayConnection::send_discovery_response(const RelayDiscoveryResponse& response) {
    const std::string json = serialize_discovery_response(response);
    return connection_.send_frame(
        MessageType::RELAY_DISCOVERY_RESPONSE,
        json.data(),
        json.size()
    );
}

std::string RelayConnection::serialize_assignment_request(
    const RelayAssignmentRequest& request) const {
    std::ostringstream json;
    json << "{"
         << "\"version\":" << request.version << ","
         << "\"discovery_id\":\"" << json_escape(request.discovery_id) << "\","
         << "\"transfer_id\":\"" << json_escape(request.transfer_id) << "\","
         << "\"epoch\":" << request.epoch << ","
         << "\"total_size\":" << request.total_size << ","
         << "\"file_count\":" << request.file_count << ","
         << "\"coordinator_rejoin_port\":" << request.coordinator_rejoin_port << ","
         << "\"has_downstream\":" << (request.has_downstream ? "true" : "false") << ","
         << "\"downstream_address\":\"" << json_escape(request.downstream_address) << "\","
         << "\"downstream_port\":" << request.downstream_port
         << "}";
    return json.str();
}

bool RelayConnection::deserialize_assignment_request(
    const std::string& json,
    RelayAssignmentRequest& request) const {
    request.version = static_cast<uint32_t>(json_get_u64(json, "version"));
    request.discovery_id = json_get_string(json, "discovery_id");
    request.transfer_id = json_get_string(json, "transfer_id");
    request.epoch = json_get_u64(json, "epoch");
    request.total_size = json_get_u64(json, "total_size");
    request.file_count = static_cast<uint32_t>(json_get_u64(json, "file_count"));
    const uint64_t rejoin_port = json_get_u64(json, "coordinator_rejoin_port");
    if (rejoin_port > UINT16_MAX) return false;
    request.coordinator_rejoin_port = static_cast<uint16_t>(rejoin_port);
    request.has_downstream = json_get_bool(json, "has_downstream");
    request.downstream_address = json_get_string(json, "downstream_address");
    request.downstream_port = static_cast<uint16_t>(
        json_get_u64(json, "downstream_port")
    );

    if (request.discovery_id.empty() || request.transfer_id.empty() ||
        request.file_count == 0) {
        return false;
    }

    if (request.has_downstream &&
        (request.downstream_address.empty() || request.downstream_port == 0)) {
        return false;
    }

    return true;
}

std::string RelayConnection::serialize_assignment_response(
    const RelayAssignmentResponse& response) const {
    std::ostringstream json;
    json << "{"
         << "\"discovery_id\":\"" << json_escape(response.discovery_id) << "\","
         << "\"transfer_id\":\"" << json_escape(response.transfer_id) << "\","
         << "\"epoch\":" << response.epoch << ","
         << "\"accepted\":" << (response.accepted ? "true" : "false") << ","
         << "\"message\":\"" << json_escape(response.message) << "\""
         << "}";
    return json.str();
}

bool RelayConnection::deserialize_assignment_response(
    const std::string& json,
    RelayAssignmentResponse& response) const {
    response.discovery_id = json_get_string(json, "discovery_id");
    response.transfer_id = json_get_string(json, "transfer_id");
    response.epoch = json_get_u64(json, "epoch");
    response.accepted = json_get_bool(json, "accepted");
    response.message = json_get_string(json, "message");

    return !response.discovery_id.empty() && !response.transfer_id.empty();
}

bool RelayConnection::send_assignment_request(
    const RelayAssignmentRequest& request) {
    const std::string json = serialize_assignment_request(request);
    return connection_.send_frame(
        MessageType::RELAY_ASSIGNMENT_REQUEST,
        json.data(),
        json.size()
    );
}

bool RelayConnection::recv_assignment_response(
    RelayAssignmentResponse& response) {
    MessageType type;
    std::vector<uint8_t> payload;

    if (!connection_.recv_frame(type, payload)) {
        return false;
    }

    if (type != MessageType::RELAY_ASSIGNMENT_RESPONSE) {
        LOG_ERROR("Expected RELAY_ASSIGNMENT_RESPONSE, got %d", static_cast<int>(type));
        return false;
    }

    const std::string json(payload.begin(), payload.end());
    return deserialize_assignment_response(json, response);
}

bool RelayConnection::recv_assignment_request(
    RelayAssignmentRequest& request) {
    MessageType type;
    std::vector<uint8_t> payload;

    if (!connection_.recv_frame(type, payload)) {
        return false;
    }

    if (type != MessageType::RELAY_ASSIGNMENT_REQUEST) {
        LOG_ERROR("Expected RELAY_ASSIGNMENT_REQUEST, got %d", static_cast<int>(type));
        return false;
    }

    const std::string json(payload.begin(), payload.end());
    return deserialize_assignment_request(json, request);
}

bool RelayConnection::send_assignment_response(
    const RelayAssignmentResponse& response) {
    const std::string json = serialize_assignment_response(response);
    return connection_.send_frame(
        MessageType::RELAY_ASSIGNMENT_RESPONSE,
        json.data(),
        json.size()
    );
}

std::string RelayConnection::serialize_heartbeat_request(const RelayHeartbeatRequest& request) const {
    std::ostringstream json;
    json << "{"
         << "\"version\":" << request.version << ","
         << "\"transfer_id\":\"" << json_escape(request.transfer_id) << "\","
         << "\"epoch\":" << request.epoch
         << "}";
    return json.str();
}

bool RelayConnection::deserialize_heartbeat_request(const std::string& json,
                                                    RelayHeartbeatRequest& request) const {
    request.version = static_cast<uint32_t>(json_get_u64(json, "version"));
    request.transfer_id = json_get_string(json, "transfer_id");
    request.epoch = json_get_u64(json, "epoch");
    return !request.transfer_id.empty();
}

std::string RelayConnection::serialize_heartbeat_response(const RelayHeartbeatResponse& response) const {
    std::ostringstream json;
    json << "{"
         << "\"transfer_id\":\"" << json_escape(response.transfer_id) << "\","
         << "\"epoch\":" << response.epoch << ","
         << "\"alive\":" << (response.alive ? "true" : "false")
         << "}";
    return json.str();
}

bool RelayConnection::deserialize_heartbeat_response(const std::string& json,
                                                     RelayHeartbeatResponse& response) const {
    response.transfer_id = json_get_string(json, "transfer_id");
    response.epoch = json_get_u64(json, "epoch");
    response.alive = json_get_bool(json, "alive");
    return !response.transfer_id.empty();
}

bool RelayConnection::send_heartbeat_request(const RelayHeartbeatRequest& request) {
    const std::string json = serialize_heartbeat_request(request);
    return connection_.send_frame(MessageType::RELAY_HEARTBEAT_REQUEST, json.data(), json.size());
}

bool RelayConnection::recv_heartbeat_response(RelayHeartbeatResponse& response) {
    MessageType type;
    std::vector<uint8_t> payload;
    if (!connection_.recv_frame(type, payload)) return false;
    if (type != MessageType::RELAY_HEARTBEAT_RESPONSE) {
        LOG_ERROR("Expected RELAY_HEARTBEAT_RESPONSE, got %d", static_cast<int>(type));
        return false;
    }
    return deserialize_heartbeat_response(std::string(payload.begin(), payload.end()), response);
}

bool RelayConnection::send_heartbeat_response(const RelayHeartbeatResponse& response) {
    const std::string json = serialize_heartbeat_response(response);
    return connection_.send_frame(MessageType::RELAY_HEARTBEAT_RESPONSE, json.data(), json.size());
}

bool RelayConnection::send_rejoin_message(MessageType type,
                                          const RelayRejoinMessage& message) {
    if (message.transfer_id.empty()) return false;
    std::ostringstream json;
    json << "{"
         << "\"version\":" << message.version << ","
         << "\"transfer_id\":\"" << json_escape(message.transfer_id) << "\","
         << "\"epoch\":" << message.epoch
         << "}";
    const std::string payload = json.str();
    return connection_.send_frame(type, payload.data(), payload.size());
}

bool RelayConnection::recv_rejoin_message(MessageType expected,
                                          RelayRejoinMessage& message) {
    MessageType type;
    std::vector<uint8_t> payload;
    if (!connection_.recv_frame(type, payload) || type != expected) return false;
    return deserialize_rejoin_message(std::string(payload.begin(), payload.end()), message);
}

bool RelayConnection::deserialize_rejoin_message(const std::string& json,
                                                 RelayRejoinMessage& message) const {
    message.version = static_cast<uint32_t>(json_get_u64(json, "version"));
    message.transfer_id = json_get_string(json, "transfer_id");
    message.epoch = json_get_u64(json, "epoch");
    return message.version == PROTOCOL_VERSION && !message.transfer_id.empty();
}

bool RelayConnection::send_rejoin_hint(const RelayRejoinMessage& message) {
    return send_rejoin_message(MessageType::PLEASE_REQUEST_REJOIN, message);
}

bool RelayConnection::recv_rejoin_hint(RelayRejoinMessage& message) {
    return recv_rejoin_message(MessageType::PLEASE_REQUEST_REJOIN, message);
}

bool RelayConnection::send_rejoin_request(const RelayRejoinMessage& message) {
    return send_rejoin_message(MessageType::REJOIN_REQ, message);
}

bool RelayConnection::recv_rejoin_request(RelayRejoinMessage& message) {
    return recv_rejoin_message(MessageType::REJOIN_REQ, message);
}

bool RelayConnection::send_rejoin_ack(const RelayRejoinMessage& message) {
    return send_rejoin_message(MessageType::REJOIN_ACK, message);
}

bool RelayConnection::recv_rejoin_ack(RelayRejoinMessage& message) {
    return recv_rejoin_message(MessageType::REJOIN_ACK, message);
}

bool RelayConnection::recv_initial_request(
    MessageType& type,
    TransferRequest& transfer_request,
    RelayDiscoveryRequest& discovery_request,
    RelayAssignmentRequest& assignment_request,
    RelayHeartbeatRequest& heartbeat_request,
    RelayRejoinMessage& rejoin_hint
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
            return deserialize_discovery_request(json, discovery_request);

        case MessageType::RELAY_ASSIGNMENT_REQUEST:
            return deserialize_assignment_request(json, assignment_request);

        case MessageType::RELAY_HEARTBEAT_REQUEST:
            return deserialize_heartbeat_request(json, heartbeat_request);

        case MessageType::PLEASE_REQUEST_REJOIN:
            return deserialize_rejoin_message(json, rejoin_hint);

        default:
            LOG_ERROR(
                "Unexpected initial message type: %d",
                static_cast<int>(type)
            );
            return false;
    }
}

} // namespace flashshare
