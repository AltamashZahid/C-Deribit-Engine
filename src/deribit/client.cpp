#include "deribit/client.hpp"

#include <algorithm>
#include <chrono>
#include <deque>
#include <random>
#include <set>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <boost/asio/dispatch.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/ssl.hpp>
#include <boost/json.hpp>

#include "common/logger.hpp"
#include "common/rate_limiter.hpp"
#include "common/time.hpp"
#include "deribit/book_feed.hpp"
#include "deribit/tls_roots.hpp"

namespace de::deribit {

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace websocket = beast::websocket;
namespace ssl = asio::ssl;
namespace json = boost::json;
using tcp = asio::ip::tcp;
using namespace std::chrono_literals;

std::string_view to_string(ConnectionState s) noexcept {
    switch (s) {
        case ConnectionState::Idle: return "idle";
        case ConnectionState::Connecting: return "connecting";
        case ConnectionState::Connected: return "connected";
        case ConnectionState::Disconnected: return "disconnected";
        case ConnectionState::Stopped: return "stopped";
    }
    return "?";
}

namespace {

bool is_private_method(std::string_view m) { return m.starts_with("private/"); }

// Requests that go to Deribit's matching engine and share its (stricter) rate limit.
bool is_matching_method(std::string_view m) {
    return m == "private/buy" || m == "private/sell" || m == "private/edit" || m == "private/edit_by_label" ||
           m.starts_with("private/cancel") || m == "private/close_position";
}

// user.* channels and raw (non-aggregated) books require an authenticated session.
bool is_private_channel(std::string_view ch) { return ch.starts_with("user.") || ch.ends_with(".raw"); }

// "book.BTC-PERPETUAL.100ms" -> "BTC-PERPETUAL" (instrument names contain no dots).
std::string instrument_from_channel(std::string_view ch) {
    const auto first = ch.find('.');
    const auto last = ch.rfind('.');
    if (first == std::string_view::npos || last <= first) return std::string(ch);
    return std::string(ch.substr(first + 1, last - first - 1));
}

struct StringHash {
    using is_transparent = void;
    std::size_t operator()(std::string_view s) const noexcept { return std::hash<std::string_view>{}(s); }
};

RpcResult make_error(int code, std::string message) {
    RpcResult r;
    r.error = RpcError{code, std::move(message)};
    return r;
}

}  // namespace

namespace detail {

struct ClientImpl {
    using Stream = websocket::stream<ssl::stream<beast::tcp_stream>>;

    struct Connection {
        Connection(asio::io_context& io, ssl::context& tls, std::uint64_t conn_id) : ws(io, tls), id(conn_id) {}
        Stream ws;
        beast::flat_buffer buffer;
        std::deque<std::string> outbox;  // Beast allows one outstanding write; the rest wait here
        bool writing = false;
        bool close_requested = false;
        std::uint64_t id;
    };

    struct Request {
        std::string method;
        json::object params;
        RpcCallback cb;
        std::int64_t deadline_ns = 0;
        std::int64_t origin_ns = 0;
        int attempts = 0;
    };

    struct Pending {
        Request req;
        std::int64_t sent_ns = 0;
    };

    explicit ClientImpl(ClientOptions o)
        : opts(std::move(o)),
          order_bucket(opts.order_rate_per_sec, opts.order_burst),
          rpc_bucket(opts.rpc_rate_per_sec, opts.rpc_burst),
          arena(new unsigned char[kArenaBytes]),
          rng(std::random_device{}()) {
        tls.set_verify_mode(ssl::verify_peer);
        if (!opts.ca_file.empty()) {
            tls.load_verify_file(opts.ca_file);
        } else {
            const int n = load_system_root_certificates(tls.native_handle());
            if (n == 0) LOG_WARN("no system root certificates loaded; set DERIBIT_CA_FILE if TLS verification fails");
        }
    }

    // ---- configuration / shared state ------------------------------------------------
    static constexpr std::size_t kArenaBytes = 1 << 20;

    ClientOptions opts;
    asio::io_context io;
    ssl::context tls{ssl::context::tls_client};
    std::optional<asio::executor_work_guard<asio::io_context::executor_type>> work;
    std::thread thread;
    std::atomic<bool> stopped_public{false};
    std::atomic<ConnectionState> state_atomic{ConnectionState::Idle};
    std::atomic<bool> authed_atomic{false};

    Client::BookHandler book_handler;
    Client::NotificationHandler notification_handler;
    Client::StateHandler state_handler;

    LatencyHistogram order_lat, rpc_lat, md_lat, t2s_lat, socket_lat;
    ClientStats stats;

    // ---- I/O-thread-only state -------------------------------------------------------
    tcp::resolver resolver{io};
    asio::steady_timer sweep_timer{io}, reconnect_timer{io}, throttle_timer{io}, refresh_timer{io}, stop_guard{io};
    std::shared_ptr<Connection> conn;
    std::uint64_t next_conn_id = 1;
    std::uint64_t next_request_id = 1;
    bool stopping = false;
    bool ws_open = false;
    bool authed = false;
    bool throttle_armed = false;
    std::string auth_error;
    std::string refresh_token;
    int reconnect_attempt = 0;
    std::int64_t last_rx_ns = 0;

    std::unordered_map<std::uint64_t, Pending> pending;
    std::deque<Request> waiting;  // not sent yet: socket down or authentication outstanding
    std::deque<Request> throttled_orders, throttled_rpc;
    TokenBucket order_bucket, rpc_bucket;

    std::set<std::string, std::less<>> channels;  // desired subscriptions (survive reconnects)
    std::unordered_map<std::string, OrderBook, StringHash, std::equal_to<>> books;  // by channel
    std::unordered_set<std::string, StringHash, std::equal_to<>> resyncing;

    // JSON values for each incoming frame are carved out of this buffer and released
    // wholesale when the frame has been handled - no per-node malloc/free on the hot path.
    std::unique_ptr<unsigned char[]> arena;
    std::mt19937 rng;

    bool has_credentials() const { return !opts.client_id.empty() && !opts.client_secret.empty(); }

    void set_state(ConnectionState s) {
        state_atomic.store(s, std::memory_order_release);
        if (state_handler) {
            try {
                state_handler(s, authed);
            } catch (const std::exception& e) {
                LOG_ERROR("state handler threw: {}", e.what());
            }
        }
    }

    void set_authed(bool v) {
        authed = v;
        authed_atomic.store(v, std::memory_order_release);
    }

    void complete(Request& req, RpcResult result) {
        if (!req.cb) return;
        try {
            req.cb(std::move(result));
        } catch (const std::exception& e) {
            LOG_ERROR("callback for {} threw: {}", req.method, e.what());
        }
    }

    // ---- connection lifecycle ----------------------------------------------------------

    void connect() {
        if (stopping) return;
        set_state(ConnectionState::Connecting);
        auto c = std::make_shared<Connection>(io, tls, next_conn_id++);
        conn = c;
        LOG_INFO("connecting to wss://{}:{}{}", opts.host, opts.port, opts.path);
        resolver.async_resolve(opts.host, opts.port, [this, c](beast::error_code ec, tcp::resolver::results_type results) {
            if (c != conn) return;
            if (ec) return fail(c, "resolve", ec);
            auto& layer = beast::get_lowest_layer(c->ws);
            layer.expires_after(std::chrono::milliseconds(opts.connect_timeout_ms));
            layer.async_connect(results, [this, c](beast::error_code ec, const tcp::endpoint&) {
                if (c != conn) return;
                if (ec) return fail(c, "connect", ec);
                beast::error_code ignored;
                beast::get_lowest_layer(c->ws).socket().set_option(tcp::no_delay(true), ignored);
                auto& tls_stream = c->ws.next_layer();
                if (!SSL_set_tlsext_host_name(tls_stream.native_handle(), opts.host.c_str())) {
                    return fail(c, "tls sni", beast::error_code(static_cast<int>(::ERR_get_error()), asio::error::get_ssl_category()));
                }
                tls_stream.set_verify_callback(ssl::host_name_verification(opts.host));
                tls_stream.async_handshake(ssl::stream_base::client, [this, c](beast::error_code ec) {
                    if (c != conn) return;
                    if (ec) return fail(c, "tls handshake", ec);
                    beast::get_lowest_layer(c->ws).expires_never();
                    c->ws.set_option(websocket::stream_base::timeout::suggested(beast::role_type::client));
                    c->ws.set_option(websocket::stream_base::decorator(
                        [](websocket::request_type& req) { req.set(beast::http::field::user_agent, "deribit-engine/1.0"); }));
                    c->ws.read_message_max(64 * 1024 * 1024);
                    c->ws.async_handshake(opts.host, opts.path, [this, c](beast::error_code ec) {
                        if (c != conn) return;
                        if (ec) return fail(c, "websocket handshake", ec);
                        on_open(c);
                    });
                });
            });
        });
    }

    void on_open(const std::shared_ptr<Connection>& c) {
        reconnect_attempt = 0;
        ws_open = true;
        last_rx_ns = now_ns();
        stats.connects.fetch_add(1, std::memory_order_relaxed);
        LOG_INFO("connected to {}", opts.host);
        set_state(ConnectionState::Connected);
        read(c);

        json::object hb;
        hb["interval"] = opts.heartbeat_seconds;
        send_internal("public/set_heartbeat", std::move(hb), [](RpcResult r) {
            if (!r.ok()) LOG_WARN("set_heartbeat failed: {}", r.error->message);
        });
        auth_error.clear();  // a new session gets a fresh authentication attempt
        if (has_credentials()) authenticate(false);
        resubscribe(false);
        flush_waiting();
    }

    void fail(const std::shared_ptr<Connection>& c, std::string_view what, beast::error_code ec) {
        if (c != conn) return;
        if (stopping) LOG_DEBUG("connection closed during shutdown ({}: {})", what, ec.message());
        else LOG_WARN("deribit connection error during {}: {}", what, ec.message());
        teardown(c);
        if (stopping) {
            stop_guard.cancel();
            return;
        }
        schedule_reconnect();
    }

    void teardown(const std::shared_ptr<Connection>& c) {
        const bool was_open = ws_open;
        conn.reset();
        ws_open = false;
        set_authed(false);
        refresh_timer.cancel();
        beast::error_code ignored;
        beast::get_lowest_layer(c->ws).socket().close(ignored);  // aborts outstanding operations
        if (was_open) stats.disconnects.fetch_add(1, std::memory_order_relaxed);

        for (auto& [ch, book] : books) book.invalidate();
        resyncing.clear();

        // Requests already written may or may not have reached Deribit. Fail them instead of
        // resending: resending an order could duplicate it.
        std::vector<Pending> lost;
        lost.reserve(pending.size());
        for (auto& [id, p] : pending) lost.push_back(std::move(p));
        pending.clear();
        for (auto& p : lost) {
            const bool order = is_matching_method(p.req.method);
            complete(p.req, make_error(errc::disconnected,
                                       order ? "connection lost before response; order state unknown - check open orders"
                                             : "connection lost before response"));
        }
        set_state(stopping ? ConnectionState::Stopped : ConnectionState::Disconnected);
    }

    void schedule_reconnect() {
        const auto base = std::min<std::int64_t>(opts.max_backoff_ms, std::int64_t{500} << std::min(reconnect_attempt, 10));
        std::uniform_int_distribution<std::int64_t> jitter(0, base / 4);
        const auto delay = base + jitter(rng);
        ++reconnect_attempt;
        LOG_INFO("reconnecting in {} ms (attempt {})", delay, reconnect_attempt);
        reconnect_timer.expires_after(std::chrono::milliseconds(delay));
        reconnect_timer.async_wait([this](beast::error_code ec) {
            if (ec || stopping) return;
            connect();
        });
    }

    void begin_shutdown() {
        stopping = true;
        sweep_timer.cancel();
        reconnect_timer.cancel();
        throttle_timer.cancel();
        refresh_timer.cancel();
        resolver.cancel();

        std::vector<Request> unsent;
        for (auto* q : {&waiting, &throttled_orders, &throttled_rpc}) {
            for (auto& r : *q) unsent.push_back(std::move(r));
            q->clear();
        }
        for (auto& r : unsent) complete(r, make_error(errc::shutting_down, "client is stopping"));

        auto c = conn;
        if (!c) {
            set_state(ConnectionState::Stopped);
            return;
        }
        if (!ws_open) {  // still connecting: just abort
            teardown(c);
            return;
        }
        stop_guard.expires_after(2s);
        stop_guard.async_wait([this, c](beast::error_code ec) {
            if (ec) return;
            LOG_WARN("websocket close timed out");
            if (c == conn) teardown(c);
        });
        if (c->writing) c->close_requested = true;
        else start_close(c);
    }

    void start_close(const std::shared_ptr<Connection>& c) {
        c->ws.async_close(websocket::close_code::normal, [this, c](beast::error_code) {
            stop_guard.cancel();
            if (c == conn) teardown(c);
        });
    }

    // ---- reading -------------------------------------------------------------------------

    void read(const std::shared_ptr<Connection>& c) {
        c->ws.async_read(c->buffer, [this, c](beast::error_code ec, std::size_t bytes) {
            if (c != conn) return;
            if (ec) return fail(c, "read", ec);
            const std::int64_t recv_ns = now_ns();
            last_rx_ns = recv_ns;
            stats.messages_in.fetch_add(1, std::memory_order_relaxed);
            stats.bytes_in.fetch_add(bytes, std::memory_order_relaxed);
            const auto data = c->buffer.cdata();
            handle_frame(std::string_view(static_cast<const char*>(data.data()), data.size()), recv_ns);
            c->buffer.consume(c->buffer.size());
            if (c == conn) read(c);
        });
    }

    void handle_frame(std::string_view frame, std::int64_t recv_ns) {
        json::monotonic_resource mr(arena.get(), kArenaBytes);
        boost::system::error_code ec;
        json::value v = json::parse(frame, ec, &mr);
        if (ec || !v.is_object()) {
            LOG_WARN("ignoring unparseable frame ({} bytes): {}", frame.size(), ec.message());
            return;
        }
        try {
            auto& obj = v.get_object();
            if (const auto* id = obj.if_contains("id"); id && !id->is_null()) {
                on_response(*id, obj);
                return;
            }
            const auto* method = obj.if_contains("method");
            if (!method || !method->is_string()) return;
            const auto& m = method->get_string();
            if (m == "subscription") {
                const auto& params = obj.at("params").as_object();
                on_subscription(params.at("channel").as_string(), params.at("data"), recv_ns);
            } else if (m == "heartbeat") {
                const auto* params = obj.if_contains("params");
                const auto* type = params && params->is_object() ? params->get_object().if_contains("type") : nullptr;
                if (type && type->is_string() && type->get_string() == "test_request") send_internal("public/test", {}, {});
            }
        } catch (const std::exception& e) {
            LOG_WARN("malformed message from Deribit: {}", e.what());
        }
    }

    void on_response(const json::value& id_value, const json::object& obj) {
        if (!id_value.is_int64() && !id_value.is_uint64()) return;
        const auto id = id_value.to_number<std::uint64_t>();
        auto it = pending.find(id);
        if (it == pending.end()) {
            LOG_DEBUG("response for unknown or expired request id {}", id);
            return;
        }
        Pending p = std::move(it->second);
        pending.erase(it);
        const std::int64_t latency = now_ns() - p.sent_ns;
        const bool matching = is_matching_method(p.req.method);
        (matching ? order_lat : rpc_lat).record(latency);

        RpcResult r;
        r.latency_ns = latency;
        if (const auto* err = obj.if_contains("error"); err && err->is_object()) {
            const auto& e = err->get_object();
            const int code = e.contains("code") ? static_cast<int>(e.at("code").to_number<std::int64_t>()) : 0;
            std::string message = e.contains("message") && e.at("message").is_string()
                                      ? std::string(e.at("message").get_string())
                                      : std::string("unknown error");
            if (const auto* d = e.if_contains("data"); d && !d->is_null()) message += " " + json::serialize(*d);
            if (code == errc::too_many_requests && p.req.attempts < opts.max_rate_limit_retries) {
                stats.rate_limit_retries.fetch_add(1, std::memory_order_relaxed);
                retry_later(std::move(p.req));
                return;
            }
            r.error = RpcError{code, std::move(message)};
        } else if (const auto* res = obj.if_contains("result")) {
            r.result = json::value(*res, json::storage_ptr{});  // copy out of the frame arena
        } else {
            r.error = RpcError{errc::bad_response, "response has neither result nor error"};
        }
        complete(p.req, std::move(r));
    }

    void on_subscription(std::string_view channel, const json::value& data, std::int64_t recv_ns) {
        if (!channel.starts_with("book.")) {
            if (notification_handler) {
                try {
                    notification_handler(std::string(channel), data);
                } catch (const std::exception& e) {
                    LOG_ERROR("notification handler threw: {}", e.what());
                }
            }
            return;
        }
        auto it = books.find(channel);
        if (it == books.end()) {
            if (!channels.contains(channel)) return;  // late message after unsubscribe
            it = books.emplace(std::string(channel), OrderBook(instrument_from_channel(channel))).first;
        }
        OrderBook& book = it->second;
        BookApply result;
        try {
            result = apply_book_message(book, data.as_object());
        } catch (const std::exception& e) {
            LOG_WARN("bad book message on {}: {}", channel, e.what());
            book.invalidate();
            result = BookApply::Invalid;
        }
        switch (result) {
            case BookApply::Snapshot:
                if (auto r = resyncing.find(channel); r != resyncing.end()) {
                    LOG_INFO("{} resynchronised at change_id {}", channel, book.change_id());
                    resyncing.erase(r);
                }
                [[fallthrough]];
            case BookApply::Applied:
                stats.book_updates.fetch_add(1, std::memory_order_relaxed);
                md_lat.record(now_ns() - recv_ns);
                if (book_handler) {
                    try {
                        book_handler(book, recv_ns);
                    } catch (const std::exception& e) {
                        LOG_ERROR("book handler threw: {}", e.what());
                    }
                }
                break;
            case BookApply::Gap:
            case BookApply::Invalid:
                if (!resyncing.contains(channel)) {
                    LOG_WARN("{}: {} update, requesting a fresh snapshot", channel, to_string(result));
                    resync(std::string(channel));
                }
                break;
        }
    }

    // ---- requests ------------------------------------------------------------------------

    void submit(Request req) {
        if (stopping) return complete(req, make_error(errc::shutting_down, "client is stopping"));
        if (req.deadline_ns == 0) req.deadline_ns = now_ns() + ms_to_ns(opts.request_timeout_ms);
        if (is_private_method(req.method)) {
            if (!has_credentials())
                return complete(req, make_error(errc::not_authenticated,
                                                "private method requires DERIBIT_CLIENT_ID / DERIBIT_CLIENT_SECRET"));
            if (ws_open && !authed && !auth_error.empty())
                return complete(req, make_error(errc::not_authenticated, "authentication failed: " + auth_error));
            if (!authed) {
                waiting.push_back(std::move(req));
                return;
            }
        } else if (!ws_open) {
            waiting.push_back(std::move(req));
            return;
        }
        const bool matching = is_matching_method(req.method);
        auto& queue = matching ? throttled_orders : throttled_rpc;
        auto& bucket = matching ? order_bucket : rpc_bucket;
        if (!queue.empty() || !bucket.try_acquire(now_ns())) {
            stats.throttled.fetch_add(1, std::memory_order_relaxed);
            queue.push_back(std::move(req));
            arm_throttle();
            return;
        }
        send(std::move(req));
    }

    void send(Request req) {
        auto c = conn;
        if (!c || !ws_open || (is_private_method(req.method) && !authed)) {
            waiting.push_back(std::move(req));
            return;
        }
        const std::uint64_t id = next_request_id++;
        json::object msg;
        msg["jsonrpc"] = "2.0";
        msg["id"] = id;
        msg["method"] = req.method;
        msg["params"] = req.params;
        std::string payload = json::serialize(msg);
        const std::int64_t origin = req.origin_ns;
        // Note: never log `payload` - public/auth carries the client secret.
        LOG_DEBUG("-> {} (id {})", req.method, id);
        const std::int64_t before_write = now_ns();
        pending.emplace(id, Pending{std::move(req), before_write});
        if (origin > 0) t2s_lat.record(before_write - origin);
        write(c, std::move(payload));
        // When the outbox was empty, async_write has already framed, encrypted and handed the
        // bytes to the kernel by the time it returns.
        if (origin > 0) socket_lat.record(now_ns() - before_write);
    }

    // For protocol housekeeping (auth, heartbeat, subscriptions): skips the queues.
    void send_internal(std::string method, json::object params, RpcCallback cb) {
        if (!ws_open) return;
        Request r;
        r.method = std::move(method);
        r.params = std::move(params);
        r.cb = std::move(cb);
        r.deadline_ns = now_ns() + ms_to_ns(opts.request_timeout_ms);
        rpc_bucket.try_acquire(now_ns());  // still counts against the budget
        auto c = conn;
        const std::uint64_t id = next_request_id++;
        json::object msg;
        msg["jsonrpc"] = "2.0";
        msg["id"] = id;
        msg["method"] = r.method;
        msg["params"] = r.params;
        pending.emplace(id, Pending{std::move(r), now_ns()});
        write(c, json::serialize(msg));
    }

    void write(const std::shared_ptr<Connection>& c, std::string payload) {
        c->outbox.push_back(std::move(payload));
        if (!c->writing) do_write(c);
    }

    void do_write(const std::shared_ptr<Connection>& c) {
        c->writing = true;
        c->ws.text(true);
        c->ws.async_write(asio::buffer(c->outbox.front()), [this, c](beast::error_code ec, std::size_t) {
            c->writing = false;
            if (c != conn) return;
            if (ec) return fail(c, "write", ec);
            c->outbox.pop_front();
            if (c->close_requested) return start_close(c);
            if (!c->outbox.empty()) do_write(c);
        });
    }

    void retry_later(Request req) {
        ++req.attempts;
        const auto delay = std::chrono::milliseconds(100 << (req.attempts - 1));
        LOG_WARN("{} rate limited by Deribit, retry {} in {} ms", req.method, req.attempts, delay.count());
        auto timer = std::make_shared<asio::steady_timer>(io, delay);
        timer->async_wait([this, timer, req = std::move(req)](beast::error_code) mutable {
            if (stopping) return complete(req, make_error(errc::shutting_down, "client is stopping"));
            submit(std::move(req));
        });
    }

    void flush_waiting() {
        std::deque<Request> w;
        w.swap(waiting);
        for (auto& r : w) submit(std::move(r));
    }

    void arm_throttle() {
        if (throttle_armed) return;
        const auto now = now_ns();
        std::int64_t wait = -1;
        if (!throttled_orders.empty()) wait = order_bucket.wait_ns(now);
        if (!throttled_rpc.empty()) {
            const auto w = rpc_bucket.wait_ns(now);
            wait = wait < 0 ? w : std::min(wait, w);
        }
        if (wait < 0) return;
        throttle_armed = true;
        throttle_timer.expires_after(std::chrono::nanoseconds(std::max<std::int64_t>(wait, 100'000)));
        throttle_timer.async_wait([this](beast::error_code ec) {
            throttle_armed = false;
            if (ec || stopping) return;
            drain_throttled(throttled_orders, order_bucket);
            drain_throttled(throttled_rpc, rpc_bucket);
            arm_throttle();
        });
    }

    void drain_throttled(std::deque<Request>& queue, TokenBucket& bucket) {
        while (!queue.empty() && bucket.try_acquire(now_ns())) {
            Request r = std::move(queue.front());
            queue.pop_front();
            send(std::move(r));
        }
    }

    // ---- periodic housekeeping ----------------------------------------------------------

    void start_sweep() {
        sweep_timer.expires_after(100ms);
        sweep_timer.async_wait([this](beast::error_code ec) {
            if (ec || stopping) return;
            sweep();
            start_sweep();
        });
    }

    void sweep() {
        const auto now = now_ns();
        std::vector<Request> expired;
        for (auto it = pending.begin(); it != pending.end();) {
            if (it->second.req.deadline_ns <= now) {
                expired.push_back(std::move(it->second.req));
                it = pending.erase(it);
            } else {
                ++it;
            }
        }
        for (auto* q : {&waiting, &throttled_orders, &throttled_rpc}) {
            for (auto it = q->begin(); it != q->end();) {
                if (it->deadline_ns <= now) {
                    expired.push_back(std::move(*it));
                    it = q->erase(it);
                } else {
                    ++it;
                }
            }
        }
        for (auto& r : expired) {
            stats.timeouts.fetch_add(1, std::memory_order_relaxed);
            const bool order = is_matching_method(r.method);
            complete(r, make_error(errc::timeout, order ? "timed out waiting for response; order state unknown"
                                                        : "request timed out"));
        }
        // Deribit sends heartbeats every heartbeat_seconds; silence means a dead link.
        if (ws_open && now - last_rx_ns > 3 * ms_to_ns(1000LL * opts.heartbeat_seconds)) {
            LOG_WARN("no data from Deribit for {} s, reconnecting", (now - last_rx_ns) / 1'000'000'000);
            auto c = conn;
            fail(c, "heartbeat", asio::error::timed_out);
        }
    }

    // ---- auth / subscriptions -------------------------------------------------------------

    void authenticate(bool use_refresh) {
        json::object p;
        if (use_refresh && !refresh_token.empty()) {
            p["grant_type"] = "refresh_token";
            p["refresh_token"] = refresh_token;
        } else {
            p["grant_type"] = "client_credentials";
            p["client_id"] = opts.client_id;
            p["client_secret"] = opts.client_secret;
        }
        const auto conn_id = conn ? conn->id : 0;
        send_internal("public/auth", std::move(p), [this, conn_id, use_refresh](RpcResult r) {
            if (!conn || conn->id != conn_id) return;
            if (!r.ok()) {
                if (use_refresh) {
                    LOG_WARN("token refresh failed ({}), authenticating again", r.error->message);
                    refresh_token.clear();
                    return authenticate(false);
                }
                auth_error = r.error->message;
                LOG_ERROR("authentication failed: {} (code {})", r.error->message, r.error->code);
                fail_waiting_private();
                return;
            }
            const auto& res = r.result.as_object();
            refresh_token = std::string(res.at("refresh_token").as_string());
            const auto expires_in = res.at("expires_in").to_number<std::int64_t>();
            auth_error.clear();
            const bool first = !authed;
            set_authed(true);
            if (first) {
                LOG_INFO("authenticated (token valid for {} s)", expires_in);
                set_state(ConnectionState::Connected);
                resubscribe(true);
                flush_waiting();
            }
            refresh_timer.expires_after(std::chrono::seconds(std::max<std::int64_t>(30, expires_in * 8 / 10)));
            refresh_timer.async_wait([this](beast::error_code ec) {
                if (!ec && !stopping && ws_open) authenticate(true);
            });
        });
    }

    void fail_waiting_private() {
        std::vector<Request> failed;
        for (auto it = waiting.begin(); it != waiting.end();) {
            if (is_private_method(it->method)) {
                failed.push_back(std::move(*it));
                it = waiting.erase(it);
            } else {
                ++it;
            }
        }
        for (auto& r : failed) complete(r, make_error(errc::not_authenticated, "authentication failed: " + auth_error));
    }

    void send_subscribe(const std::vector<std::string>& chans, bool priv) {
        if (chans.empty()) return;
        json::object p;
        json::array arr;
        for (const auto& ch : chans) arr.emplace_back(ch);
        p["channels"] = std::move(arr);
        send_internal(priv ? "private/subscribe" : "public/subscribe", std::move(p), [chans](RpcResult r) {
            if (!r.ok()) LOG_WARN("subscribe to {} failed: {}", chans.front(), r.error->message);
            else LOG_INFO("subscribed: {}", json::serialize(r.result));
        });
    }

    // On connect: public channels. After authentication: private channels.
    void resubscribe(bool priv) {
        std::vector<std::string> list;
        for (const auto& ch : channels) {
            if (is_private_channel(ch) == priv) list.push_back(ch);
        }
        send_subscribe(list, priv);
    }

    void add_channel(const std::string& ch) {
        if (!channels.insert(ch).second) return;
        const bool priv = is_private_channel(ch);
        if (ws_open && (!priv || authed)) send_subscribe({ch}, priv);
    }

    void remove_channel(const std::string& ch) {
        auto it = channels.find(ch);
        if (it == channels.end()) return;
        channels.erase(it);
        if (auto b = books.find(ch); b != books.end()) books.erase(b);
        if (auto r = resyncing.find(ch); r != resyncing.end()) resyncing.erase(r);
        if (!ws_open) return;
        json::object p;
        p["channels"] = json::array{json::value(ch)};
        send_internal(is_private_channel(ch) ? "private/unsubscribe" : "public/unsubscribe", std::move(p), {});
    }

    // Deribit sends a fresh snapshot on every subscribe, so unsubscribe + subscribe resyncs.
    void resync(const std::string& ch) {
        resyncing.insert(ch);
        stats.book_resyncs.fetch_add(1, std::memory_order_relaxed);
        const bool priv = is_private_channel(ch);
        json::object p;
        p["channels"] = json::array{json::value(ch)};
        send_internal(priv ? "private/unsubscribe" : "public/unsubscribe", std::move(p), [this, ch, priv](RpcResult) {
            if (!channels.contains(ch) || !ws_open) return;
            send_subscribe({ch}, priv);
        });
    }
};

}  // namespace detail

// ---- public API -----------------------------------------------------------------------------

Client::Client(ClientOptions options) : impl_(std::make_unique<detail::ClientImpl>(std::move(options))) {}

Client::~Client() { stop(); }

void Client::set_book_handler(BookHandler handler) { impl_->book_handler = std::move(handler); }
void Client::set_notification_handler(NotificationHandler handler) { impl_->notification_handler = std::move(handler); }
void Client::set_state_handler(StateHandler handler) { impl_->state_handler = std::move(handler); }

void Client::start() {
    auto& m = *impl_;
    if (m.thread.joinable() || m.stopped_public.load()) return;
    m.work.emplace(asio::make_work_guard(m.io));
    m.thread = std::thread([&m] {
        // A handler that throws must not kill the I/O thread: log and keep running.
        for (;;) {
            try {
                m.io.run();
                return;
            } catch (const std::exception& e) {
                LOG_ERROR("unhandled exception on deribit I/O thread: {}", e.what());
            }
        }
    });
    asio::post(m.io, [&m] {
        m.start_sweep();
        m.connect();
    });
}

void Client::stop() {
    auto& m = *impl_;
    if (m.stopped_public.exchange(true)) return;
    if (!m.thread.joinable()) return;
    asio::post(m.io, [&m] { m.begin_shutdown(); });
    m.work.reset();
    m.thread.join();
}

void Client::call(std::string method, boost::json::object params, RpcCallback cb, std::int64_t origin_ns) {
    auto& m = *impl_;
    if (m.stopped_public.load(std::memory_order_acquire)) {
        if (cb) cb(make_error(errc::shutting_down, "client is stopped"));
        return;
    }
    detail::ClientImpl::Request r;
    r.method = std::move(method);
    r.params = std::move(params);
    r.cb = std::move(cb);
    r.origin_ns = origin_ns;
    asio::dispatch(m.io.get_executor(), [&m, r = std::move(r)]() mutable { m.submit(std::move(r)); });
}

std::future<RpcResult> Client::call(std::string method, boost::json::object params) {
    auto promise = std::make_shared<std::promise<RpcResult>>();
    auto future = promise->get_future();
    call(std::move(method), std::move(params), [promise](RpcResult r) { promise->set_value(std::move(r)); });
    return future;
}

namespace {
json::object order_params(const OrderRequest& o) {
    json::object p;
    p["instrument_name"] = o.instrument;
    p["amount"] = o.amount;
    p["type"] = o.type;
    if (o.price) p["price"] = *o.price;
    if (!o.label.empty()) p["label"] = o.label;
    if (o.post_only) p["post_only"] = true;
    if (o.reduce_only) p["reduce_only"] = true;
    if (!o.time_in_force.empty()) p["time_in_force"] = o.time_in_force;
    return p;
}
const char* order_method(OrderSide side) { return side == OrderSide::Buy ? "private/buy" : "private/sell"; }
}  // namespace

void Client::place_order(const OrderRequest& order, RpcCallback cb, std::int64_t origin_ns) {
    call(order_method(order.side), order_params(order), std::move(cb), origin_ns);
}

std::future<RpcResult> Client::place_order(const OrderRequest& order) {
    return call(order_method(order.side), order_params(order));
}

std::future<RpcResult> Client::cancel(const std::string& order_id) {
    return call("private/cancel", {{"order_id", order_id}});
}

std::future<RpcResult> Client::cancel_all(const std::string& instrument) {
    if (instrument.empty()) return call("private/cancel_all", {});
    return call("private/cancel_all_by_instrument", {{"instrument_name", instrument}});
}

std::future<RpcResult> Client::cancel_by_label(const std::string& label) {
    return call("private/cancel_by_label", {{"label", label}});
}

std::future<RpcResult> Client::edit(const std::string& order_id, double amount, double price) {
    return call("private/edit", {{"order_id", order_id}, {"amount", amount}, {"price", price}});
}

std::future<RpcResult> Client::get_order_book(const std::string& instrument, int depth) {
    return call("public/get_order_book", {{"instrument_name", instrument}, {"depth", depth}});
}

std::future<RpcResult> Client::get_positions(const std::string& currency, const std::string& kind) {
    json::object p{{"currency", currency}};
    if (!kind.empty()) p["kind"] = kind;
    return call("private/get_positions", std::move(p));
}

std::future<RpcResult> Client::get_open_orders(const std::string& instrument) {
    if (instrument.empty()) return call("private/get_open_orders", {});
    return call("private/get_open_orders_by_instrument", {{"instrument_name", instrument}});
}

std::future<RpcResult> Client::get_instruments(const std::string& currency, const std::string& kind) {
    json::object p{{"currency", currency}, {"expired", false}};
    if (!kind.empty()) p["kind"] = kind;
    return call("public/get_instruments", std::move(p));
}

std::future<RpcResult> Client::get_instrument(const std::string& instrument) {
    return call("public/get_instrument", {{"instrument_name", instrument}});
}

std::future<RpcResult> Client::get_account_summary(const std::string& currency) {
    return call("private/get_account_summary", {{"currency", currency}});
}

std::string Client::book_channel(const std::string& instrument) const {
    return "book." + instrument + "." + impl_->opts.book_interval;
}

void Client::subscribe_book(const std::string& instrument) { subscribe(book_channel(instrument)); }
void Client::unsubscribe_book(const std::string& instrument) { unsubscribe(book_channel(instrument)); }

void Client::subscribe(const std::string& channel) {
    auto& m = *impl_;
    asio::dispatch(m.io.get_executor(), [&m, channel] { m.add_channel(channel); });
}

void Client::unsubscribe(const std::string& channel) {
    auto& m = *impl_;
    asio::dispatch(m.io.get_executor(), [&m, channel] { m.remove_channel(channel); });
}

ConnectionState Client::state() const noexcept { return impl_->state_atomic.load(std::memory_order_acquire); }
bool Client::authenticated() const noexcept { return impl_->authed_atomic.load(std::memory_order_acquire); }
bool Client::has_credentials() const noexcept { return impl_->has_credentials(); }
LatencyHistogram& Client::order_latency() noexcept { return impl_->order_lat; }
LatencyHistogram& Client::rpc_latency() noexcept { return impl_->rpc_lat; }
LatencyHistogram& Client::md_latency() noexcept { return impl_->md_lat; }
LatencyHistogram& Client::tick_to_send_latency() noexcept { return impl_->t2s_lat; }
LatencyHistogram& Client::socket_write_latency() noexcept { return impl_->socket_lat; }
const ClientStats& Client::stats() const noexcept { return impl_->stats; }

}  // namespace de::deribit
