#pragma once

#include "net/connection.h"

#include <cstdint>
#include <string>

// Defines relay control requests and responses and their wire helpers.
// The coordinator and receivers use these messages to discover nodes,
// assign a chain, check health, and request rejoin.
// Discovery responses include saved progress, while heartbeat and rejoin
// messages carry the transfer ID and assignment epoch.

namespace flashshare {

struct RelayDiscoveryRequest {
    uint32_t version = PROTOCOL_VERSION;
    std::string discovery_id;
    std::string transfer_id;
    uint64_t total_size = 0;
    uint32_t file_count = 0;
};

struct RelayDiscoveryResponse {
    std::string discovery_id;
    bool available = false;
    uint64_t offset = 0; // Initial build starts at zero; recovery can report progress later.
    std::string message;
};

struct RelayHeartbeatRequest {
    uint32_t version = PROTOCOL_VERSION;
    std::string transfer_id;
    uint64_t epoch = 0;
};

struct RelayHeartbeatResponse {
    std::string transfer_id;
    uint64_t epoch = 0;
    bool alive = false;
};

// Both messages carry the same transfer identity. The coordinator sends the
// hint to a missed node; that node sends REJOIN_REQ back when available.
struct RelayRejoinMessage {
    uint32_t version = PROTOCOL_VERSION;
    std::string transfer_id;
    uint64_t epoch = 0;
};

struct RelayAssignmentRequest {
    uint32_t version = PROTOCOL_VERSION;
    std::string discovery_id;
    std::string transfer_id;
    uint64_t epoch = 0;
    uint64_t total_size = 0;
    uint32_t file_count = 0;
    uint16_t coordinator_rejoin_port = 0;
    bool has_downstream = false;
    std::string downstream_address;
    uint16_t downstream_port = DEFAULT_PORT;
};

struct RelayAssignmentResponse {
    std::string discovery_id;
    std::string transfer_id;
    uint64_t epoch = 0;
    bool accepted = false;
    std::string message;
};

class RelayConnection {
public:
    explicit RelayConnection(Connection& connection);

    bool recv_initial_request(
        MessageType& type,
        TransferRequest& transfer_request,
        RelayDiscoveryRequest& discovery_request,
        RelayAssignmentRequest& assignment_request,
        RelayHeartbeatRequest& heartbeat_request,
        RelayRejoinMessage& rejoin_hint
    );

    bool send_discovery_request(const RelayDiscoveryRequest& request);
    bool recv_discovery_response(RelayDiscoveryResponse& response);

    bool recv_discovery_request(RelayDiscoveryRequest& request);
    bool send_discovery_response(const RelayDiscoveryResponse& response);

    // This is for handling relay assignment requests and responses.
    // After peer discovery multicasting, peers will exchange relay assignment requests and responses.
    // Sender ----R2 address----> R1 -----R3 address ----> R2 -------->R3
    bool send_assignment_request(const RelayAssignmentRequest& request);
    bool recv_assignment_response(RelayAssignmentResponse& response);

    bool recv_assignment_request(RelayAssignmentRequest& request);
    bool send_assignment_response(const RelayAssignmentResponse& response);

    bool send_heartbeat_request(const RelayHeartbeatRequest& request);
    bool recv_heartbeat_response(RelayHeartbeatResponse& response);
    bool send_heartbeat_response(const RelayHeartbeatResponse& response);

    bool send_rejoin_hint(const RelayRejoinMessage& message);
    bool recv_rejoin_hint(RelayRejoinMessage& message);
    bool send_rejoin_request(const RelayRejoinMessage& message);
    bool recv_rejoin_request(RelayRejoinMessage& message);
    bool send_rejoin_ack(const RelayRejoinMessage& message);
    bool recv_rejoin_ack(RelayRejoinMessage& message);

private:
    Connection& connection_;

    bool send_rejoin_message(MessageType type, const RelayRejoinMessage& message);
    bool recv_rejoin_message(MessageType type, RelayRejoinMessage& message);
    bool deserialize_rejoin_message(const std::string& json,
                                    RelayRejoinMessage& message) const;

    std::string serialize_discovery_request(const RelayDiscoveryRequest& request) const;
    bool deserialize_discovery_request(const std::string& json, RelayDiscoveryRequest& request) const;

    std::string serialize_discovery_response(const RelayDiscoveryResponse& response) const;
    bool deserialize_discovery_response(const std::string& json, RelayDiscoveryResponse& response) const;

    std::string serialize_heartbeat_request(const RelayHeartbeatRequest& request) const;
    bool deserialize_heartbeat_request(const std::string& json, RelayHeartbeatRequest& request) const;
    std::string serialize_heartbeat_response(const RelayHeartbeatResponse& response) const;
    bool deserialize_heartbeat_response(const std::string& json, RelayHeartbeatResponse& response) const;

    std::string serialize_assignment_request(const RelayAssignmentRequest& request) const;
    bool deserialize_assignment_request(const std::string& json, RelayAssignmentRequest& request) const;

    std::string serialize_assignment_response(const RelayAssignmentResponse& response) const;
    bool deserialize_assignment_response(const std::string& json, RelayAssignmentResponse& response) const;
};

} // namespace flashshare
