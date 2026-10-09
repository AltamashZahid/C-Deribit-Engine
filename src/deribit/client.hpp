#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <optional>
#include <string>

#include <boost/json/object.hpp>
#include <boost/json/value.hpp>

#include "common/latency_histogram.hpp"
#include "deribit/order_book.hpp"

namespace de::deribit {

// Error codes produced locally. Negative so they never collide with Deribit's codes.
namespace errc {
inline constexpr int timeout = -1;            // no response within the request timeout
inline constexpr int disconnected = -2;       // connection dropped while the request was in flight
inline constexpr int not_authenticated = -3;  // private method without (valid) credentials
inline constexpr int shutting_down = -4;
inline constexpr int bad_response = -5;
inline constexpr int too_many_requests = 10028;  // Deribit's rate-limit error
}  // namespace errc

struct RpcError {
    int code = 0;
    std::string message;
};

struct RpcResult {
    boost::json::value result;      // the JSON-RPC "result" when ok()
    std::optional<RpcError> error;  // set on failure
    std::int64_t latency_ns = 0;    // request written to the socket -> response parsed
    bool ok() const noexcept { return !error.has_value(); }
};

using RpcCallback = std::function<void(RpcResult)>;

enum class OrderSide { Buy, Sell };

struct OrderRequest {
    std::string instrument;
    OrderSide side = OrderSide::Buy;
    double amount = 0;              // USD for inverse futures/perpetuals, coins for options
    std::string type = "limit";     // "limit" or "market"
    std::optional<double> price;    // required for limit orders
    std::string label;              // client tag, usable with cancel_by_label
    bool post_only = false;
    bool reduce_only = false;
    std::string time_in_force;      // "good_til_cancelled" (default), "immediate_or_cancel", ...
};

struct ClientOptions {
    std::string host = "test.deribit.com";
    std::string port = "443";
    std::string path = "/ws/api/v2";
    std::string client_id;
    std::string client_secret;
    std::string ca_file;              // empty = system trust store
    std::string book_interval = "100ms";
    int heartbeat_seconds = 10;
    int request_timeout_ms = 5000;
    int connect_timeout_ms = 10000;
    int max_backoff_ms = 30000;
    int max_rate_limit_retries = 3;
    double order_rate_per_sec = 5;
    double order_burst = 10;
    double rpc_rate_per_sec = 20;
    double rpc_burst = 50;
};

enum class ConnectionState { Idle, Connecting, Connected, Disconnected, Stopped };
std::string_view to_string(ConnectionState s) noexcept;

struct ClientStats {
    std::atomic<std::uint64_t> messages_in{0};
    std::atomic<std::uint64_t> bytes_in{0};
    std::atomic<std::uint64_t> book_updates{0};
    std::atomic<std::uint64_t> book_resyncs{0};
    std::atomic<std::uint64_t> connects{0};
    std::atomic<std::uint64_t> disconnects{0};
    std::atomic<std::uint64_t> timeouts{0};
    std::atomic<std::uint64_t> rate_limit_retries{0};
    std::atomic<std::uint64_t> throttled{0};
};

namespace detail {
struct ClientImpl;
}

// Deribit JSON-RPC client over a single TLS WebSocket.
//
// Threading: all networking, book maintenance and callbacks run on one internal I/O
// thread, so nothing on the hot path takes a lock. Every public method is thread-safe;
// when called from the I/O thread itself (e.g. from a book handler) the request is sent
// inline, without a thread hop.
//
// Reliability:
//  - reconnects with exponential backoff + jitter, then re-authenticates and
//    resubscribes; requests made while disconnected are queued and sent after reconnect
//  - requests that were in flight when the connection dropped fail with
//    errc::disconnected (for orders the outcome is unknown - reconcile by label/open orders)
//  - every request has a deadline (errc::timeout)
//  - client-side token buckets keep under Deribit's limits; a 10028 "too_many_requests"
//    reply is retried with backoff
//  - Deribit heartbeats are answered, and a silent connection is torn down
//  - book sequence gaps (prev_change_id mismatch) trigger a resubscribe for a new snapshot
class Client {
public:
    // Called on the I/O thread after every book snapshot/change has been applied.
    using BookHandler = std::function<void(const OrderBook& book, std::int64_t recv_ns)>;
    // Called on the I/O thread for subscription channels other than book.*.
    using NotificationHandler = std::function<void(const std::string& channel, const boost::json::value& data)>;
    using StateHandler = std::function<void(ConnectionState state, bool authenticated)>;

    explicit Client(ClientOptions options);
    ~Client();
    Client(const Client&) = delete;
    Client& operator=(const Client&) = delete;

    // Handlers must be set before start().
    void set_book_handler(BookHandler handler);
    void set_notification_handler(NotificationHandler handler);
    void set_state_handler(StateHandler handler);

    void start();
    void stop();  // graceful close, fails outstanding requests, joins the I/O thread

    // Generic JSON-RPC call. `origin_ns` (optional) is the timestamp of the event that
    // caused this request; the time from it until the request is serialised and ready to
    // write is recorded in tick_to_send_latency(), the write itself in socket_write_latency().
    void call(std::string method, boost::json::object params, RpcCallback cb, std::int64_t origin_ns = 0);
    std::future<RpcResult> call(std::string method, boost::json::object params);

    void place_order(const OrderRequest& order, RpcCallback cb, std::int64_t origin_ns = 0);
    std::future<RpcResult> place_order(const OrderRequest& order);
    std::future<RpcResult> cancel(const std::string& order_id);
    std::future<RpcResult> cancel_all(const std::string& instrument = {});
    std::future<RpcResult> cancel_by_label(const std::string& label);
    std::future<RpcResult> edit(const std::string& order_id, double amount, double price);
    std::future<RpcResult> get_order_book(const std::string& instrument, int depth);
    std::future<RpcResult> get_positions(const std::string& currency, const std::string& kind = {});
    std::future<RpcResult> get_open_orders(const std::string& instrument = {});
    std::future<RpcResult> get_instruments(const std::string& currency, const std::string& kind = {});
    std::future<RpcResult> get_instrument(const std::string& instrument);
    std::future<RpcResult> get_account_summary(const std::string& currency);

    // Maintains a local order book from book.<instrument>.<interval>.
    void subscribe_book(const std::string& instrument);
    void unsubscribe_book(const std::string& instrument);
    // Any other channel (ticker.*, trades.*, user.orders.* ...).
    void subscribe(const std::string& channel);
    void unsubscribe(const std::string& channel);
    std::string book_channel(const std::string& instrument) const;

    ConnectionState state() const noexcept;
    bool authenticated() const noexcept;
    bool has_credentials() const noexcept;

    LatencyHistogram& order_latency() noexcept;         // buy/sell/edit/cancel: sent -> response
    LatencyHistogram& rpc_latency() noexcept;           // all other requests: sent -> response
    LatencyHistogram& md_latency() noexcept;            // book frame received -> parsed + applied
    LatencyHistogram& tick_to_send_latency() noexcept;  // origin_ns -> request serialised, about to be written
    LatencyHistogram& socket_write_latency() noexcept;  // WebSocket framing + TLS + kernel send call
    const ClientStats& stats() const noexcept;

private:
    std::unique_ptr<detail::ClientImpl> impl_;
};

}  // namespace de::deribit
