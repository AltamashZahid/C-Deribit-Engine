#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include "common/latency_histogram.hpp"

namespace de {

struct ServerOptions {
    std::string bind = "127.0.0.1";
    std::uint16_t port = 8080;      // 0 = pick a free port (see MarketDataServer::port())
    int threads = 2;
    std::size_t max_queue = 256;    // per-client limit on queued non-conflatable messages
    std::size_t max_subscriptions_per_session = 64;
};

struct ServerStats {
    std::atomic<std::uint64_t> sessions_opened{0};
    std::atomic<std::int64_t> sessions_active{0};
    std::atomic<std::uint64_t> messages_out{0};
    std::atomic<std::uint64_t> bytes_out{0};
    std::atomic<std::uint64_t> conflated{0};            // queued updates replaced by a newer one
    std::atomic<std::uint64_t> slow_consumer_drops{0};
    std::atomic<std::uint64_t> publishes{0};
};

namespace detail {
struct ServerImpl;
}

// WebSocket server that fans out market data to local clients.
//
// Protocol (JSON text frames):
//   client -> {"op":"subscribe","instrument":"BTC-PERPETUAL"}   <- {"type":"subscribed",...}
//   client -> {"op":"unsubscribe","instrument":"BTC-PERPETUAL"} <- {"type":"unsubscribed",...}
//   client -> {"op":"ping"}                                     <- {"type":"pong","server_ns":...}
//   server -> {"type":"book",...} for every update of a subscribed instrument
//
// Design:
//  - N I/O threads; each session runs on its own strand, so a session's state needs no locks
//  - a published message is serialised once and shared (shared_ptr) by every subscriber
//  - latest-value delivery: a session holds at most one pending update per instrument; a
//    newer update replaces it (conflation). An update is only skipped if a newer one for
//    the same instrument arrives before it could be written, so clients never go
//    backwards, always end on the latest snapshot, and memory per client stays bounded.
//    A client that piles up `max_queue` unmergeable replies (it never reads) is dropped.
//  - the demand handler fires when an instrument gains its first subscriber or loses its
//    last one, so the engine only streams what someone is watching
class MarketDataServer {
public:
    // Called with the server's subscription lock held, so calls arrive in order.
    // Must not block or call back into the server.
    using DemandHandler = std::function<void(const std::string& instrument, bool wanted)>;

    MarketDataServer(ServerOptions options, DemandHandler on_demand);
    ~MarketDataServer();
    MarketDataServer(const MarketDataServer&) = delete;
    MarketDataServer& operator=(const MarketDataServer&) = delete;

    void start();  // binds and listens; throws boost::system::system_error on failure
    void stop();
    std::uint16_t port() const noexcept;

    // Thread-safe. `source_ns` is when the underlying data arrived (steady clock); the time
    // from then until the message is written to each client socket goes into fanout_latency().
    void publish(const std::string& instrument, std::shared_ptr<const std::string> payload, std::int64_t source_ns);

    std::size_t subscriber_count(const std::string& instrument) const;
    LatencyHistogram& fanout_latency() noexcept;
    const ServerStats& stats() const noexcept;

private:
    std::unique_ptr<detail::ServerImpl> impl_;
};

}  // namespace de
