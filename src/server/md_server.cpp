#include "server/md_server.hpp"

#include <algorithm>
#include <deque>
#include <mutex>
#include <optional>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <boost/asio/dispatch.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/strand.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/json.hpp>

#include "common/logger.hpp"
#include "common/time.hpp"

namespace de {

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace websocket = beast::websocket;
namespace json = boost::json;
using tcp = asio::ip::tcp;

namespace detail {

class Session;

struct ServerImpl {
    ServerImpl(ServerOptions o, MarketDataServer::DemandHandler d) : opts(std::move(o)), demand(std::move(d)) {}

    ServerOptions opts;
    MarketDataServer::DemandHandler demand;
    asio::io_context io;
    tcp::acceptor acceptor{io};
    std::optional<asio::executor_work_guard<asio::io_context::executor_type>> work;
    std::vector<std::thread> threads;
    std::atomic<std::uint16_t> bound_port{0};

    struct Topic {
        std::shared_ptr<const std::string> name;  // shared by every queued message (conflation key)
        std::vector<std::shared_ptr<Session>> sessions;
    };

    mutable std::mutex mu;  // guards subs and sessions
    std::unordered_map<std::string, Topic> subs;
    std::unordered_set<std::shared_ptr<Session>> sessions;

    LatencyHistogram fanout;
    ServerStats stats;

    void do_accept();
    void subscribe(const std::shared_ptr<Session>& s, const std::string& instrument);
    void unsubscribe(const std::shared_ptr<Session>& s, const std::string& instrument);
    void remove_session(const std::shared_ptr<Session>& s);
};

namespace {

bool valid_instrument(std::string_view s) {
    if (s.empty() || s.size() > 64) return false;
    return std::all_of(s.begin(), s.end(), [](char c) {
        return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '_';
    });
}

std::shared_ptr<const std::string> make_reply(json::object obj) {
    return std::make_shared<const std::string>(json::serialize(obj));
}

}  // namespace

// One connected client. Every member function runs on the session's strand.
class Session : public std::enable_shared_from_this<Session> {
public:
    Session(tcp::socket&& socket, ServerImpl& server) : ws_(std::move(socket)), srv_(server) {}

    void run() {
        asio::dispatch(ws_.get_executor(), [self = shared_from_this()] { self->on_run(); });
    }

    // Thread-safe: hops onto the session's strand.
    void deliver(std::shared_ptr<const std::string> topic, std::shared_ptr<const std::string> payload, std::int64_t source_ns) {
        asio::post(ws_.get_executor(), [self = shared_from_this(), topic = std::move(topic), payload = std::move(payload),
                                        source_ns]() mutable {
            self->enqueue({std::move(payload), source_ns, std::move(topic)});
        });
    }

    void shutdown() {
        asio::post(ws_.get_executor(), [self = shared_from_this()] { self->close("server shutdown"); });
    }

private:
    struct Outgoing {
        std::shared_ptr<const std::string> data;
        std::int64_t source_ns = 0;
        std::shared_ptr<const std::string> topic;  // instrument for market data; null for control replies
    };

    void on_run() {
        ws_.set_option(websocket::stream_base::timeout::suggested(beast::role_type::server));
        ws_.set_option(websocket::stream_base::decorator(
            [](websocket::response_type& res) { res.set(beast::http::field::server, "deribit-engine-md/1.0"); }));
        ws_.read_message_max(4096);
        ws_.async_accept([self = shared_from_this()](beast::error_code ec) {
            if (ec) return self->close("handshake failed: " + ec.message());
            self->do_read();
        });
    }

    void do_read() {
        if (closed_) return;
        ws_.async_read(buffer_, [self = shared_from_this()](beast::error_code ec, std::size_t) {
            if (ec) return self->close(ec == websocket::error::closed ? "client closed" : ec.message());
            const auto data = self->buffer_.cdata();
            self->on_message(std::string_view(static_cast<const char*>(data.data()), data.size()));
            self->buffer_.consume(self->buffer_.size());
            self->do_read();
        });
    }

    void on_message(std::string_view text) {
        boost::system::error_code ec;
        auto v = json::parse(text, ec);
        const json::object* obj = (!ec && v.is_object()) ? &v.get_object() : nullptr;
        const json::value* op = obj ? obj->if_contains("op") : nullptr;
        if (!op || !op->is_string()) return reply_error("expected {\"op\":...}");
        const auto& o = op->get_string();

        if (o == "ping") return send_reply({{"type", "pong"}, {"server_ns", now_ns()}});

        if (o == "subscribe" || o == "unsubscribe") {
            const auto* inst = obj->if_contains("instrument");
            if (!inst || !inst->is_string() || !valid_instrument(inst->get_string()))
                return reply_error("missing or invalid \"instrument\"");
            std::string name(inst->get_string());
            if (o == "subscribe") {
                if (!subs_.contains(name)) {
                    if (subs_.size() >= srv_.opts.max_subscriptions_per_session) return reply_error("too many subscriptions");
                    subs_.insert(name);
                    srv_.subscribe(shared_from_this(), name);
                }
                return send_reply({{"type", "subscribed"}, {"instrument", name}});
            }
            if (subs_.erase(name)) srv_.unsubscribe(shared_from_this(), name);
            return send_reply({{"type", "unsubscribed"}, {"instrument", name}});
        }
        reply_error("unknown op");
    }

    void send_reply(json::object reply) { enqueue({make_reply(std::move(reply)), 0, nullptr}); }
    void reply_error(std::string_view message) { send_reply({{"type", "error"}, {"message", message}}); }

    void enqueue(Outgoing msg) {
        if (closed_) return;
        // Book messages are full snapshots, so a newer one makes a still-queued update for
        // the same instrument worthless: overwrite it in place (conflation). A client that
        // keeps up never has one queued; a slow client gets fewer but always current
        // snapshots instead of an ever-older backlog.
        if (msg.topic) {
            const std::size_t first = writing_ ? 1 : 0;  // front() is being written
            for (std::size_t i = queue_.size(); i-- > first;) {
                if (queue_[i].topic && *queue_[i].topic == *msg.topic) {
                    queue_[i] = std::move(msg);
                    srv_.stats.conflated.fetch_add(1, std::memory_order_relaxed);
                    return;
                }
            }
        }
        if (queue_.size() >= srv_.opts.max_queue) {
            // Only unmergeable messages pile up like this (e.g. a client spamming requests
            // without reading the replies).
            srv_.stats.slow_consumer_drops.fetch_add(1, std::memory_order_relaxed);
            LOG_WARN("market data client has {} unsent messages queued, disconnecting it", queue_.size());
            return close("slow consumer");
        }
        queue_.push_back(std::move(msg));
        if (!writing_) do_write();
    }

    void do_write() {
        writing_ = true;
        ws_.text(true);
        ws_.async_write(asio::buffer(*queue_.front().data), [self = shared_from_this()](beast::error_code ec, std::size_t n) {
            self->on_write(ec, n);
        });
    }

    void on_write(beast::error_code ec, std::size_t bytes) {
        writing_ = false;
        if (closed_) return;
        if (ec) return close("write failed: " + ec.message());
        const auto& sent = queue_.front();
        if (sent.source_ns > 0) srv_.fanout.record(now_ns() - sent.source_ns);
        srv_.stats.messages_out.fetch_add(1, std::memory_order_relaxed);
        srv_.stats.bytes_out.fetch_add(bytes, std::memory_order_relaxed);
        queue_.pop_front();
        if (!queue_.empty()) do_write();
    }

    void close(const std::string& reason) {
        if (closed_) return;
        closed_ = true;
        LOG_DEBUG("market data session closed: {}", reason);
        // Keep the message an in-flight write is still reading from; drop the rest.
        if (writing_ && !queue_.empty()) queue_.erase(queue_.begin() + 1, queue_.end());
        else queue_.clear();
        auto self = shared_from_this();
        for (const auto& inst : subs_) srv_.unsubscribe(self, inst);
        subs_.clear();
        srv_.remove_session(self);
        beast::error_code ignored;
        beast::get_lowest_layer(ws_).socket().shutdown(tcp::socket::shutdown_both, ignored);
        beast::get_lowest_layer(ws_).socket().close(ignored);
    }

    websocket::stream<beast::tcp_stream> ws_;
    beast::flat_buffer buffer_;
    ServerImpl& srv_;
    std::deque<Outgoing> queue_;
    std::unordered_set<std::string> subs_;
    bool writing_ = false;
    bool closed_ = false;
};

void ServerImpl::do_accept() {
    // Each connection gets its own strand: its handlers never run concurrently.
    acceptor.async_accept(asio::make_strand(io), [this](beast::error_code ec, tcp::socket socket) {
        if (ec) {
            if (ec == asio::error::operation_aborted) return;
            LOG_WARN("accept failed: {}", ec.message());
        } else {
            beast::error_code ignored;
            socket.set_option(tcp::no_delay(true), ignored);
            auto s = std::make_shared<Session>(std::move(socket), *this);
            {
                std::lock_guard lock(mu);
                sessions.insert(s);
            }
            stats.sessions_opened.fetch_add(1, std::memory_order_relaxed);
            stats.sessions_active.fetch_add(1, std::memory_order_relaxed);
            s->run();
        }
        if (acceptor.is_open()) do_accept();
    });
}

void ServerImpl::subscribe(const std::shared_ptr<Session>& s, const std::string& instrument) {
    std::lock_guard lock(mu);
    auto& topic = subs[instrument];
    if (!topic.name) topic.name = std::make_shared<const std::string>(instrument);
    const bool first = topic.sessions.empty();
    topic.sessions.push_back(s);
    if (first && demand) demand(instrument, true);
}

void ServerImpl::unsubscribe(const std::shared_ptr<Session>& s, const std::string& instrument) {
    std::lock_guard lock(mu);
    auto it = subs.find(instrument);
    if (it == subs.end()) return;
    auto& list = it->second.sessions;
    if (auto pos = std::find(list.begin(), list.end(), s); pos != list.end()) {
        *pos = std::move(list.back());
        list.pop_back();
    }
    if (list.empty()) {
        subs.erase(it);
        if (demand) demand(instrument, false);
    }
}

void ServerImpl::remove_session(const std::shared_ptr<Session>& s) {
    std::lock_guard lock(mu);
    if (sessions.erase(s)) stats.sessions_active.fetch_sub(1, std::memory_order_relaxed);
}

}  // namespace detail

MarketDataServer::MarketDataServer(ServerOptions options, DemandHandler on_demand)
    : impl_(std::make_unique<detail::ServerImpl>(std::move(options), std::move(on_demand))) {}

MarketDataServer::~MarketDataServer() { stop(); }

void MarketDataServer::start() {
    auto& m = *impl_;
    if (!m.threads.empty()) return;
    const tcp::endpoint endpoint{asio::ip::make_address(m.opts.bind), m.opts.port};
    m.acceptor.open(endpoint.protocol());
#ifndef _WIN32
    // On Windows SO_REUSEADDR would let a second server steal the port, so only set it on POSIX.
    m.acceptor.set_option(asio::socket_base::reuse_address(true));
#endif
    m.acceptor.bind(endpoint);
    m.acceptor.listen(asio::socket_base::max_listen_connections);
    m.bound_port = m.acceptor.local_endpoint().port();
    m.work.emplace(asio::make_work_guard(m.io));
    m.do_accept();
    const int n = std::max(1, m.opts.threads);
    for (int i = 0; i < n; ++i) {
        m.threads.emplace_back([&m] {
            for (;;) {
                try {
                    m.io.run();
                    return;
                } catch (const std::exception& e) {
                    LOG_ERROR("unhandled exception on market data server thread: {}", e.what());
                }
            }
        });
    }
    LOG_INFO("market data server listening on ws://{}:{} ({} threads)", m.opts.bind, m.bound_port.load(), n);
}

void MarketDataServer::stop() {
    auto& m = *impl_;
    if (m.threads.empty()) return;
    asio::post(m.io, [&m] {
        beast::error_code ignored;
        m.acceptor.close(ignored);
        std::vector<std::shared_ptr<detail::Session>> all;
        {
            std::lock_guard lock(m.mu);
            all.assign(m.sessions.begin(), m.sessions.end());
        }
        for (auto& s : all) s->shutdown();
    });
    m.work.reset();
    for (auto& t : m.threads) t.join();
    m.threads.clear();
    std::lock_guard lock(m.mu);
    m.subs.clear();
    m.sessions.clear();
}

std::uint16_t MarketDataServer::port() const noexcept { return impl_->bound_port.load(); }

void MarketDataServer::publish(const std::string& instrument, std::shared_ptr<const std::string> payload,
                               std::int64_t source_ns) {
    auto& m = *impl_;
    // Copy the subscriber list under the lock, deliver outside it. The buffer is reused
    // per thread so publishing doesn't allocate in steady state.
    thread_local std::vector<std::shared_ptr<detail::Session>> targets;
    std::shared_ptr<const std::string> topic;
    {
        std::lock_guard lock(m.mu);
        auto it = m.subs.find(instrument);
        if (it == m.subs.end()) return;
        topic = it->second.name;
        targets.assign(it->second.sessions.begin(), it->second.sessions.end());
    }
    m.stats.publishes.fetch_add(1, std::memory_order_relaxed);
    for (auto& s : targets) s->deliver(topic, payload, source_ns);
    targets.clear();
}

std::size_t MarketDataServer::subscriber_count(const std::string& instrument) const {
    std::lock_guard lock(impl_->mu);
    auto it = impl_->subs.find(instrument);
    return it == impl_->subs.end() ? 0 : it->second.sessions.size();
}

LatencyHistogram& MarketDataServer::fanout_latency() noexcept { return impl_->fanout; }
const ServerStats& MarketDataServer::stats() const noexcept { return impl_->stats; }

}  // namespace de
