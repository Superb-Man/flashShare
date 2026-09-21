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
    uint32_t reservation_ms = 0;
};

struct RelayDiscoveryResponse {
    std::string discovery_id;
    bool available = false;
    std::string message;
};

class RelayConnection {
public:
    explicit RelayConnection(Connection& connection);

    bool recv_initial_request(
        MessageType& type,
        TransferRequest& transfer_request,
        RelayDiscoveryRequest& discovery_request
    );

    bool send_discovery_request(const RelayDiscoveryRequest& request);
    bool recv_discovery_response(RelayDiscoveryResponse& response);

    bool recv_discovery_request(RelayDiscoveryRequest& request);
    bool send_discovery_response(const RelayDiscoveryResponse& response);

private:
    Connection& connection_;

    std::string serialize_request(const RelayDiscoveryRequest& request) const;
    bool deserialize_request(const std::string& json, RelayDiscoveryRequest& request) const;

    std::string serialize_response(const RelayDiscoveryResponse& response) const;
    bool deserialize_response(const std::string& json, RelayDiscoveryResponse& response) const;
};

} // namespace flashshare
