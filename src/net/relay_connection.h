#pragma once

#include "net/connection.h"

#include <cstdint>
#include <string>

namespace flashshare {

struct RelayDiscoveryRequest {
    uint32_t version = PROTOCOL_VERSION;
    std::string discovery_id;
    uint64_t total_size = 0;
    uint32_t file_count = 0;
};

struct RelayDiscoveryResponse {
    std::string discovery_id;
    bool available = false;
    std::string message;
};

struct RelayAssignmentRequest {
    uint32_t version = PROTOCOL_VERSION;
    std::string discovery_id;
    std::string transfer_id;
    uint64_t total_size = 0;
    uint32_t file_count = 0;
    bool has_downstream = false;
    std::string downstream_address;
    uint16_t downstream_port = DEFAULT_PORT;
};

struct RelayAssignmentResponse {
    std::string discovery_id;
    std::string transfer_id;
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
        RelayAssignmentRequest& assignment_request
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

private:
    Connection& connection_;

    std::string serialize_discovery_request(const RelayDiscoveryRequest& request) const;
    bool deserialize_discovery_request(const std::string& json, RelayDiscoveryRequest& request) const;

    std::string serialize_discovery_response(const RelayDiscoveryResponse& response) const;
    bool deserialize_discovery_response(const std::string& json, RelayDiscoveryResponse& response) const;

    std::string serialize_assignment_request(const RelayAssignmentRequest& request) const;
    bool deserialize_assignment_request(const std::string& json, RelayAssignmentRequest& request) const;

    std::string serialize_assignment_response(const RelayAssignmentResponse& response) const;
    bool deserialize_assignment_response(const std::string& json, RelayAssignmentResponse& response) const;
};

} // namespace flashshare
