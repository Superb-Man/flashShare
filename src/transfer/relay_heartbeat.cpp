#include "transfer/relay_heartbeat.h"

#include "net/relay_connection.h"
#include "net/socket.h"
#include "net/threadpool.h"

#include <exception>
#include <utility>

// Sends short-lived heartbeat requests to active relay nodes concurrently.
// Replies received before the shared deadline are reported to membership;
// late or missing replies do not count for that round.
// Each reply is reported as it arrives, so the coordinator can continue
// streaming while the deadline timer waits for the rest.

namespace flashshare {

std::chrono::milliseconds RelayHeartbeatPoller::remaining_time(Clock::time_point deadline) {
    const auto now = Clock::now();
    if (now >= deadline) return std::chrono::milliseconds(0);
    return std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
}

RelayHeartbeatReply RelayHeartbeatPoller::probe(const RelayEndpoint& endpoint,
    const RelayMembership::HeartbeatPoll& poll
) const {
    RelayHeartbeatReply result;
    result.endpoint = endpoint;

    auto remaining = remaining_time(poll.deadline);
    if (remaining.count() <= 0) return result;

    Socket socket;
    if (!socket.create() || !socket.relay_connect(endpoint.address, endpoint.port, remaining)) {
        return result;
    }

    socket.set_nodelay(true);

    remaining = remaining_time(poll.deadline);

    if (remaining.count() <= 0 ||
        !socket.relay_set_send_timeout(remaining) ||
        !socket.relay_set_recv_timeout(remaining)) {
        return result;
    }

    Connection connection(std::move(socket));
    RelayConnection relay_connection(connection);
    RelayHeartbeatRequest request;


    request.transfer_id = poll.transfer_id;
    request.epoch = poll.epoch;

    if (!relay_connection.send_heartbeat_request(request)) return result;

    remaining = remaining_time(poll.deadline);
    if (remaining.count() <= 0 || !connection.socket().relay_set_recv_timeout(remaining)) {
        return result;
    }

    RelayHeartbeatResponse response;
    if (!relay_connection.recv_heartbeat_response(response)) return result;

    result.received_at = Clock::now();
    result.alive = result.received_at <= poll.deadline &&
                   response.alive &&
                   response.transfer_id == poll.transfer_id &&
                   response.epoch == poll.epoch;
    return result;
}

std::vector<RelayHeartbeatReply> RelayHeartbeatPoller::run(
    const RelayMembership::HeartbeatPoll& poll,
    const ReplyCallback& on_reply
) const {
    std::vector<RelayHeartbeatReply> results(poll.nodes.size());
    if (!poll.started || poll.transfer_id.empty() || poll.nodes.empty()) return results;

    // creating pool here might seem unnecessary for a small number of nodes,
    // but it allows us to probe multiple nodes concurrently, improving overall performance.

    ThreadPool pool(poll.nodes.size(), poll.nodes.size());

    for (size_t i = 0; i < poll.nodes.size(); ++i) {
        results[i].endpoint = poll.nodes[i];
        pool.enqueue([this, &poll, &results, &on_reply, i] {
            try {
                // store the result of probing this node
                // within the time defined in timer dispatcher
                
                results[i] = probe(poll.nodes[i], poll); 
                if (results[i].alive && on_reply) on_reply(results[i]);
            } catch (...) {
                // This node is missing from the heartbeat result.
            }
        });
    }

    pool.stop(true);
    return results;
}

} // namespace flashshare
