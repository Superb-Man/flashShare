#include "transfer/relay_discovery.h"

#include "net/socket.h"
#include "net/threadpool.h"

#include <stdexcept>
#include <utility>

namespace flashshare {

RelayDiscovery::RelayDiscovery(
    std::vector<RelayEndpoint> candidates,
    std::chrono::milliseconds timeout) : candidates_(std::move(candidates)), timeout_(timeout) {

    if (timeout_.count() <= 0) {
        throw std::invalid_argument("Relay discovery timeout must be greater than zero");
    }
}

std::chrono::milliseconds RelayDiscovery::remaining_time(Clock::time_point deadline) {
    const auto now = Clock::now();
    if (now >= deadline) {
        return std::chrono::milliseconds(0);
    }

    return std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
}

RelayProbeResult RelayDiscovery::probe(
    const RelayEndpoint& endpoint,
    const RelayDiscoveryRequest& request,
    Clock::time_point deadline
) const {
    RelayProbeResult result;
    result.endpoint = endpoint;

    auto remaining = remaining_time(deadline);
    if (remaining.count() <= 0) {
        result.message = "Relay discovery deadline expired before connection";
        return result;
    }

    Socket socket;
    if (!socket.create()) {
        result.message = "Cannot create discovery socket";
        return result;
    }

    if (!socket.relay_connect(endpoint.address, endpoint.port, remaining)) {
        result.message = "Cannot connect before relay discovery deadline";
        return result;
    }

    socket.set_nodelay(true);

    remaining = remaining_time(deadline);

    if (remaining.count() <= 0) {
        result.message = "Relay discovery deadline expired after connection";
        return result;
    }

    if (!socket.relay_set_send_timeout(remaining) || !socket.relay_set_recv_timeout(remaining)) {
        result.message = "Cannot configure relay discovery socket timeout";
        return result;
    }

    Connection connection(std::move(socket));
    RelayConnection relay_connection(connection);

    if (!relay_connection.send_discovery_request(request)) {
        result.message = "Cannot send relay discovery request";
        return result;
    }

    remaining = remaining_time(deadline);

    if (remaining.count() <= 0) {
        result.message = "Relay discovery deadline expired after request";
        return result;
    }

    if (!connection.socket().relay_set_recv_timeout(remaining)) {
        result.message = "Cannot update relay discovery receive timeout";
        return result;
    }

    RelayDiscoveryResponse response;
    if (!relay_connection.recv_discovery_response(response)) {
        result.message = "No relay discovery response before deadline";
        return result;
    }

    if (Clock::now() > deadline) {
        result.message = "Relay discovery response arrived after deadline";
        return result;
    }

    if (response.discovery_id != request.discovery_id) {
        result.message = "Relay discovery response ID does not match request";
        return result;
    }

    result.responded = true;
    result.available = response.available;
    result.message = response.message;
    return result;
}

std::vector<RelayProbeResult> RelayDiscovery::run(const RelayDiscoveryRequest& request) const {
    
    if (request.discovery_id.empty()) {
        throw std::invalid_argument("Relay discovery ID cannot be empty");
    }

    std::vector<RelayProbeResult> results(candidates_.size());
    for (size_t i = 0; i < candidates_.size(); ++i) {
        results[i].endpoint = candidates_[i];
        results[i].message = "Relay discovery was not started";
    }

    if (candidates_.empty()) {
        return results;
    }

    const Clock::time_point deadline = Clock::now() + timeout_;
    ThreadPool pool(candidates_.size(), candidates_.size());

    for (size_t i = 0; i < candidates_.size(); ++i) {
        const bool queued = pool.enqueue(
            [this, &request, &results, deadline, i] {
                try {
                    results[i] = probe(candidates_[i], request, deadline);
                } catch (const std::exception& error) {
                    results[i].message = error.what();
                } catch (...) {
                    results[i].message = "Unexpected relay discovery failure";
                }
            }
        );

        if (!queued) {
            results[i].message = "Relay discovery worker rejected the probe";
        }
    }

    pool.stop(true);

    return results;
}

} // namespace flashshare
