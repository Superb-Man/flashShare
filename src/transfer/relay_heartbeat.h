#pragma once

#include "transfer/relay_membership.h"

#include <chrono>
#include <functional>
#include <vector>

// Declares relay heartbeat probes and the reply data they collect.
// A poll checks active endpoints under one deadline and reports each reply
// to the coordinator's membership state.
// Replies include the endpoint and arrival time so membership can reject
// responses that miss the shared deadline.

namespace flashshare {

struct RelayHeartbeatReply {
    RelayEndpoint endpoint;
    bool alive = false;
    RelayMembership::Clock::time_point received_at{};
};

// Polls every active relay node concurrently under one shared deadline.
class RelayHeartbeatPoller {
public:
    using ReplyCallback = std::function<void(const RelayHeartbeatReply&)>;

    // Probes every active node, returns their results, 
    // and calls on_reply as each valid reply arrives so membership can record it.
    std::vector<RelayHeartbeatReply> run(
        const RelayMembership::HeartbeatPoll& poll,
        const ReplyCallback& on_reply = {}) const;

private:
    using Clock = RelayMembership::Clock;

    //Connects to one node, sends the heartbeat request,
    // and checks that its response matches the transfer and epoch and arrived on time.
    RelayHeartbeatReply probe(const RelayEndpoint& endpoint, const RelayMembership::HeartbeatPoll& poll) const;

    static std::chrono::milliseconds remaining_time(Clock::time_point deadline);
};

} // namespace flashshare
