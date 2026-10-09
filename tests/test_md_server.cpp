#include <chrono>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <boost/asio/connect.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/json.hpp>

#include "common/time.hpp"
#include "server/md_server.hpp"
#include "test.hpp"

using namespace de;
namespace asio = boost::asio;
namespace beast = boost::beast;
namespace websocket = beast::websocket;
namespace json = boost::json;
using tcp = asio::ip::tcp;

namespace {

struct DemandLog {
    std::mutex mu;
    std::vector<std::pair<std::string, bool>> events;
    MarketDataServer::DemandHandler handler() {
        return [this](const std::string& inst, bool wanted) {
            std::lock_guard lock(mu);
            events.emplace_back(inst, wanted);
        };
    }
    std::vector<std::pair<std::string, bool>> snapshot() {
        std::lock_guard lock(mu);
        return events;
    }
};

// Blocking WebSocket client for tests.
struct TestClient {
    asio::io_context io;
    websocket::stream<tcp::socket> ws{io};

    explicit TestClient(std::uint16_t port) {
        tcp::resolver resolver(io);
        asio::connect(ws.next_layer(), resolver.resolve("127.0.0.1", std::to_string(port)));
        ws.handshake("127.0.0.1", "/");
    }
    void send(const std::string& text) { ws.write(asio::buffer(text)); }
    json::object recv() {
        beast::flat_buffer b;
        ws.read(b);
        return json::parse(beast::buffers_to_string(b.data())).as_object();
    }
};

template <class Pred>
bool wait_until(Pred pred, int timeout_ms = 3000) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return pred();
}

ServerOptions test_options() {
    ServerOptions o;
    o.port = 0;  // ephemeral
    o.threads = 2;
    return o;
}

std::string type_of(const json::object& o) { return std::string(o.at("type").as_string()); }

}  // namespace

TEST(server_subscribe_publish_unsubscribe) {
    DemandLog demand;
    MarketDataServer server(test_options(), demand.handler());
    server.start();
    TestClient c(server.port());

    c.send(R"({"op":"subscribe","instrument":"BTC-PERPETUAL"})");
    auto ack = c.recv();
    CHECK_EQ(type_of(ack), std::string("subscribed"));
    CHECK(wait_until([&] { return server.subscriber_count("BTC-PERPETUAL") == 1; }));

    const auto src = now_ns();
    server.publish("BTC-PERPETUAL", std::make_shared<const std::string>(R"({"type":"book","n":1})"), src);
    server.publish("ETH-PERPETUAL", std::make_shared<const std::string>(R"({"type":"book","n":99})"), src);  // not subscribed
    server.publish("BTC-PERPETUAL", std::make_shared<const std::string>(R"({"type":"book","n":2})"), src);
    // Latest-value delivery: #1 may be superseded by #2 if it was still queued behind an
    // in-flight write, but updates never go backwards and the newest always arrives.
    int n = c.recv().at("n").to_number<int>();
    CHECK(n == 1 || n == 2);
    if (n == 1) n = c.recv().at("n").to_number<int>();
    CHECK_EQ(n, 2);

    c.send(R"({"op":"unsubscribe","instrument":"BTC-PERPETUAL"})");
    CHECK_EQ(type_of(c.recv()), std::string("unsubscribed"));
    CHECK(wait_until([&] { return server.subscriber_count("BTC-PERPETUAL") == 0; }));

    const auto events = demand.snapshot();
    REQUIRE(events.size() == 2);
    CHECK(events[0] == std::make_pair(std::string("BTC-PERPETUAL"), true));
    CHECK(events[1] == std::make_pair(std::string("BTC-PERPETUAL"), false));
    CHECK(wait_until([&] { return server.fanout_latency().count() >= 1; }));  // acks aren't timed
    server.stop();
}

TEST(server_ping_and_protocol_errors) {
    MarketDataServer server(test_options(), {});
    server.start();
    TestClient c(server.port());
    c.send(R"({"op":"ping"})");
    auto pong = c.recv();
    CHECK_EQ(type_of(pong), std::string("pong"));
    CHECK(pong.contains("server_ns"));
    c.send("not json");
    CHECK_EQ(type_of(c.recv()), std::string("error"));
    c.send(R"({"op":"subscribe","instrument":"bad name with spaces"})");
    CHECK_EQ(type_of(c.recv()), std::string("error"));
    c.send(R"({"op":"dance"})");
    CHECK_EQ(type_of(c.recv()), std::string("error"));
    server.stop();
}

TEST(server_fans_out_to_all_subscribers_and_demands_once) {
    DemandLog demand;
    MarketDataServer server(test_options(), demand.handler());
    server.start();
    std::vector<std::unique_ptr<TestClient>> clients;
    for (int i = 0; i < 5; ++i) {
        clients.push_back(std::make_unique<TestClient>(server.port()));
        clients.back()->send(R"({"op":"subscribe","instrument":"ETH-PERPETUAL"})");
        CHECK_EQ(type_of(clients.back()->recv()), std::string("subscribed"));
    }
    server.publish("ETH-PERPETUAL", std::make_shared<const std::string>(R"({"type":"book","n":7})"), now_ns());
    for (auto& c : clients) CHECK_EQ(c->recv().at("n").to_number<int>(), 7);
    CHECK_EQ(demand.snapshot().size(), 1u);  // only the first subscriber triggers demand

    clients.clear();  // disconnect everyone: the last one leaving releases the instrument
    CHECK(wait_until([&] { return demand.snapshot().size() == 2; }));
    CHECK(wait_until([&] { return server.stats().sessions_active.load() == 0; }));
    server.stop();
}

TEST(server_conflates_updates_for_slow_consumer) {
    auto opts = test_options();
    opts.max_queue = 8;
    MarketDataServer server(opts, {});
    server.start();
    TestClient slow(server.port());
    slow.send(R"({"op":"subscribe","instrument":"BTC-PERPETUAL"})");
    CHECK_EQ(type_of(slow.recv()), std::string("subscribed"));

    // The client stops reading while 300 large updates are published. Once the socket
    // buffers are full the server's queue hits max_queue and newer updates overwrite
    // queued ones instead of growing the queue.
    const std::string pad(64 * 1024, 'x');
    constexpr int kUpdates = 300;
    for (int i = 0; i < kUpdates; ++i) {
        auto msg = std::make_shared<const std::string>(R"({"type":"book","n":)" + std::to_string(i) + R"(,"pad":")" + pad + "\"}");
        server.publish("BTC-PERPETUAL", std::move(msg), now_ns());
    }
    CHECK(wait_until([&] { return server.stats().conflated.load() > 0; }));
    CHECK_EQ(server.stats().slow_consumer_drops.load(), 0u);

    // When it resumes, it sees strictly increasing updates ending with the latest one.
    int last = -1, received = 0;
    while (last != kUpdates - 1) {
        const int n = slow.recv().at("n").to_number<int>();
        CHECK(n > last);
        last = n;
        ++received;
    }
    CHECK(received < kUpdates);
    server.stop();
}

TEST(server_disconnects_client_that_never_reads_replies) {
    auto opts = test_options();
    opts.max_queue = 8;
    MarketDataServer server(opts, {});
    server.start();
    TestClient spammer(server.port());
    // Replies to pings can't be conflated: once 8 are stuck behind full socket buffers the
    // session is closed rather than buffering without bound.
    try {
        for (int i = 0; i < 2'000'000 && server.stats().slow_consumer_drops.load() == 0; ++i) spammer.send(R"({"op":"ping"})");
    } catch (const std::exception&) {
        // the server closed the connection under us
    }
    CHECK(wait_until([&] { return server.stats().slow_consumer_drops.load() == 1; }));
    CHECK(wait_until([&] { return server.stats().sessions_active.load() == 0; }));

    // Other clients are unaffected.
    TestClient fine(server.port());
    fine.send(R"({"op":"ping"})");
    CHECK_EQ(type_of(fine.recv()), std::string("pong"));
    server.stop();
}

TEST(server_stop_with_connected_clients_is_prompt) {
    MarketDataServer server(test_options(), {});
    server.start();
    TestClient a(server.port()), b(server.port());
    a.send(R"({"op":"subscribe","instrument":"X"})");
    a.recv();
    const auto start = std::chrono::steady_clock::now();
    server.stop();
    CHECK(std::chrono::steady_clock::now() - start < std::chrono::seconds(2));
}
