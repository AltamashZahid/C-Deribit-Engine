// Client tests against a local fake Deribit server that speaks real TLS + WebSocket.
// A throwaway self-signed certificate for 127.0.0.1 is generated at start-up and handed
// to the client as its CA file, so the full verification path is exercised too.

#include <atomic>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <boost/asio/connect.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/ssl.hpp>
#include <boost/json.hpp>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

#include "deribit/client.hpp"
#include "test.hpp"

using namespace de;
using namespace de::deribit;
namespace asio = boost::asio;
namespace beast = boost::beast;
namespace websocket = beast::websocket;
namespace ssl = asio::ssl;
namespace json = boost::json;
using tcp = asio::ip::tcp;

namespace {

struct TestCert {
    std::string cert_pem, key_pem, ca_path;
};

std::string bio_to_string(BIO* bio) {
    char* data = nullptr;
    const long len = BIO_get_mem_data(bio, &data);
    return std::string(data, static_cast<std::size_t>(len));
}

// Self-signed P-256 certificate with SAN IP:127.0.0.1, written once per process.
const TestCert& test_cert() {
    static const TestCert cert = [] {
        TestCert c;
        EVP_PKEY* key = EVP_EC_gen("P-256");
        X509* x = X509_new();
        X509_set_version(x, 2);
        ASN1_INTEGER_set(X509_get_serialNumber(x), 1);
        X509_gmtime_adj(X509_getm_notBefore(x), -3600);
        X509_gmtime_adj(X509_getm_notAfter(x), 86400);
        X509_set_pubkey(x, key);
        X509_NAME* name = X509_get_subject_name(x);
        X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC, reinterpret_cast<const unsigned char*>("127.0.0.1"), -1, -1, 0);
        X509_set_issuer_name(x, name);
        X509V3_CTX ctx;
        X509V3_set_ctx_nodb(&ctx);
        X509V3_set_ctx(&ctx, x, x, nullptr, nullptr, 0);
        for (auto [nid, value] : {std::pair{NID_subject_alt_name, "IP:127.0.0.1"}, std::pair{NID_basic_constraints, "critical,CA:TRUE"}}) {
            X509_EXTENSION* ext = X509V3_EXT_conf_nid(nullptr, &ctx, nid, value);
            X509_add_ext(x, ext, -1);
            X509_EXTENSION_free(ext);
        }
        X509_sign(x, key, EVP_sha256());
        BIO* cb = BIO_new(BIO_s_mem());
        PEM_write_bio_X509(cb, x);
        c.cert_pem = bio_to_string(cb);
        BIO_free(cb);
        BIO* kb = BIO_new(BIO_s_mem());
        PEM_write_bio_PrivateKey(kb, key, nullptr, nullptr, 0, nullptr, nullptr);
        c.key_pem = bio_to_string(kb);
        BIO_free(kb);
        X509_free(x);
        EVP_PKEY_free(key);
        const auto path = std::filesystem::temp_directory_path() /
                          ("de_test_ca_" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + ".pem");
        std::ofstream(path, std::ios::binary) << c.cert_pem;
        c.ca_path = path.string();
        return c;
    }();
    return cert;
}

// Scripted Deribit stand-in. Serves one connection at a time (sync I/O on its own
// thread); `handler` is called for every JSON-RPC request and replies via Conn.
class FakeDeribit {
public:
    struct Conn {
        websocket::stream<ssl::stream<tcp::socket>>& ws;
        int index;  // 1 for the first connection, 2 after a reconnect, ...
        bool drop = false;  // set by the handler to cut the TCP connection
        void send(const json::value& v) { ws.write(asio::buffer(json::serialize(v))); }
        void result(const json::object& req, json::value result) {
            send(json::object{{"jsonrpc", "2.0"}, {"id", req.at("id")}, {"result", std::move(result)}});
        }
        void error(const json::object& req, int code, const char* message) {
            send(json::object{{"jsonrpc", "2.0"}, {"id", req.at("id")}, {"error", {{"code", code}, {"message", message}}}});
        }
        void notify(const std::string& channel, json::value data) {
            send(json::object{{"jsonrpc", "2.0"}, {"method", "subscription"}, {"params", {{"channel", channel}, {"data", std::move(data)}}}});
        }
    };
    using Handler = std::function<void(Conn&, const json::object& request, const std::string& method)>;

    explicit FakeDeribit(Handler handler) : handler_(std::move(handler)) {
        const auto& cert = test_cert();
        tls_.use_certificate_chain(asio::buffer(cert.cert_pem));
        tls_.use_private_key(asio::buffer(cert.key_pem), ssl::context::pem);
        acceptor_.open(tcp::v4());
        acceptor_.bind({asio::ip::make_address("127.0.0.1"), 0});
        acceptor_.listen();
        port_ = acceptor_.local_endpoint().port();
        thread_ = std::thread([this] { run(); });
    }

    ~FakeDeribit() {
        stop_ = true;
        try {  // unblock accept()
            asio::io_context io;
            tcp::socket s(io);
            s.connect({asio::ip::make_address("127.0.0.1"), port_});
        } catch (...) {
        }
        thread_.join();
    }

    std::string port() const { return std::to_string(port_); }
    int connections() const { return connections_.load(); }

    // Methods received so far, as "conn:method".
    std::vector<std::string> log() {
        std::lock_guard lock(mu_);
        return log_;
    }
    bool saw(const std::string& entry) {
        auto l = log();
        return std::find(l.begin(), l.end(), entry) != l.end();
    }

private:
    void run() {
        while (!stop_) {
            tcp::socket socket(io_);
            beast::error_code ec;
            acceptor_.accept(socket, ec);
            if (ec || stop_) break;
            const int index = ++connections_;
            try {
                websocket::stream<ssl::stream<tcp::socket>> ws(std::move(socket), tls_);
                ws.next_layer().handshake(ssl::stream_base::server);
                ws.accept();
                Conn conn{ws, index};
                for (;;) {
                    beast::flat_buffer buffer;
                    ws.read(buffer);
                    const auto req = json::parse(beast::buffers_to_string(buffer.data())).as_object();
                    const std::string method(req.at("method").as_string());
                    {
                        std::lock_guard lock(mu_);
                        log_.push_back(std::to_string(index) + ":" + method);
                    }
                    handler_(conn, req, method);
                    if (conn.drop) {
                        beast::get_lowest_layer(ws).close();
                        break;
                    }
                }
            } catch (const std::exception&) {
                // client went away; wait for the next connection
            }
        }
    }

    Handler handler_;
    asio::io_context io_;
    ssl::context tls_{ssl::context::tls_server};
    tcp::acceptor acceptor_{io_};
    std::uint16_t port_ = 0;
    std::thread thread_;
    std::atomic<bool> stop_{false};
    std::atomic<int> connections_{0};
    std::mutex mu_;
    std::vector<std::string> log_;
};

// Default replies for the housekeeping calls every connection makes.
bool housekeeping(FakeDeribit::Conn& c, const json::object& req, const std::string& m) {
    if (m == "public/set_heartbeat" || m == "public/test") {
        c.result(req, "ok");
        return true;
    }
    if (m == "public/auth") {
        c.result(req, {{"access_token", "at"}, {"refresh_token", "rt"}, {"expires_in", 900}, {"token_type", "bearer"}});
        return true;
    }
    if (m == "public/unsubscribe" || m == "private/unsubscribe") {
        c.result(req, req.at("params").at("channels"));
        return true;
    }
    return false;
}

ClientOptions options_for(const FakeDeribit& server, bool credentials = false) {
    ClientOptions o;
    o.host = "127.0.0.1";
    o.port = server.port();
    o.ca_file = test_cert().ca_path;
    o.request_timeout_ms = 2000;
    if (credentials) {
        o.client_id = "id";
        o.client_secret = "secret";
    }
    return o;
}

template <class Pred>
bool wait_until(Pred pred, int timeout_ms = 5000) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return pred();
}

json::array levels(std::initializer_list<std::pair<double, double>> l, const char* action = "new") {
    json::array a;
    for (auto [p, q] : l) a.push_back(json::array{action, p, q});
    return a;
}

}  // namespace

TEST(client_authenticates_then_places_order) {
    FakeDeribit server([](FakeDeribit::Conn& c, const json::object& req, const std::string& m) {
        if (housekeeping(c, req, m)) return;
        if (m == "private/buy") {
            const auto& p = req.at("params").as_object();
            c.result(req, {{"order", {{"order_id", "ord-1"}, {"order_state", "open"}, {"price", p.at("price")},
                                      {"amount", p.at("amount")}, {"post_only", p.at("post_only")}}},
                           {"trades", json::array{}}});
        }
    });
    Client client(options_for(server, true));
    client.start();
    OrderRequest o;
    o.instrument = "BTC-PERPETUAL";
    o.amount = 10;
    o.price = 50000.5;
    o.post_only = true;
    auto r = client.place_order(o).get();  // queued until the session is authenticated
    REQUIRE(r.ok());
    CHECK_EQ(std::string(r.result.at("order").at("order_id").as_string()), std::string("ord-1"));
    CHECK_EQ(r.result.at("order").at("price").to_number<double>(), 50000.5);
    CHECK(r.latency_ns > 0);
    CHECK(client.authenticated());
    const auto log = server.log();
    const auto auth = std::find(log.begin(), log.end(), "1:public/auth");
    const auto buy = std::find(log.begin(), log.end(), "1:private/buy");
    CHECK(auth != log.end() && buy != log.end() && auth < buy);
    CHECK_EQ(client.order_latency().count(), 1u);
    client.stop();
}

TEST(client_reports_exchange_errors) {
    FakeDeribit server([](FakeDeribit::Conn& c, const json::object& req, const std::string& m) {
        if (housekeeping(c, req, m)) return;
        if (m == "private/sell") c.error(req, 10009, "not_enough_funds");
    });
    Client client(options_for(server, true));
    client.start();
    OrderRequest o;
    o.instrument = "BTC-PERPETUAL";
    o.side = OrderSide::Sell;
    o.amount = 10;
    o.type = "market";
    auto r = client.place_order(o).get();
    REQUIRE(!r.ok());
    CHECK_EQ(r.error->code, 10009);
    CHECK(r.error->message.find("not_enough_funds") != std::string::npos);
    client.stop();
}

TEST(client_retries_rate_limited_requests) {
    std::atomic<int> calls{0};
    FakeDeribit server([&](FakeDeribit::Conn& c, const json::object& req, const std::string& m) {
        if (housekeeping(c, req, m)) return;
        if (m == "public/get_time") {
            if (++calls == 1) c.error(req, 10028, "too_many_requests");
            else c.result(req, 1234);
        }
    });
    Client client(options_for(server));
    client.start();
    auto r = client.call("public/get_time", {}).get();
    REQUIRE(r.ok());
    CHECK_EQ(r.result.to_number<int>(), 1234);
    CHECK_EQ(calls.load(), 2);
    CHECK_EQ(client.stats().rate_limit_retries.load(), 1u);
    client.stop();
}

TEST(client_private_call_without_credentials_fails_fast) {
    FakeDeribit server([](FakeDeribit::Conn& c, const json::object& req, const std::string& m) { housekeeping(c, req, m); });
    Client client(options_for(server));
    client.start();
    auto r = client.get_positions("BTC").get();
    REQUIRE(!r.ok());
    CHECK_EQ(r.error->code, errc::not_authenticated);
    client.stop();
}

TEST(client_failed_auth_fails_private_requests) {
    FakeDeribit server([](FakeDeribit::Conn& c, const json::object& req, const std::string& m) {
        if (m == "public/auth") return c.error(req, 13004, "invalid_credentials");
        housekeeping(c, req, m);
    });
    Client client(options_for(server, true));
    client.start();
    auto r = client.get_positions("BTC").get();
    REQUIRE(!r.ok());
    CHECK_EQ(r.error->code, errc::not_authenticated);
    CHECK(r.error->message.find("invalid_credentials") != std::string::npos);
    CHECK(!client.authenticated());
    client.stop();
}

TEST(client_request_times_out) {
    FakeDeribit server([](FakeDeribit::Conn& c, const json::object& req, const std::string& m) {
        housekeeping(c, req, m);  // never answers public/get_time
    });
    auto opts = options_for(server);
    opts.request_timeout_ms = 300;
    Client client(opts);
    client.start();
    const auto start = std::chrono::steady_clock::now();
    auto r = client.call("public/get_time", {}).get();
    const auto elapsed = std::chrono::steady_clock::now() - start;
    REQUIRE(!r.ok());
    CHECK_EQ(r.error->code, errc::timeout);
    CHECK(elapsed >= std::chrono::milliseconds(250) && elapsed < std::chrono::seconds(3));
    CHECK_EQ(client.stats().timeouts.load(), 1u);
    client.stop();
}

TEST(client_maintains_book_and_resyncs_after_gap) {
    std::atomic<int> subscribes{0};
    FakeDeribit server([&](FakeDeribit::Conn& c, const json::object& req, const std::string& m) {
        if (housekeeping(c, req, m)) return;
        if (m != "public/subscribe") return;
        const std::string ch(req.at("params").at("channels").as_array()[0].as_string());
        c.result(req, json::array{json::value(ch)});
        if (++subscribes == 1) {
            c.notify(ch, {{"type", "snapshot"}, {"change_id", 1}, {"timestamp", 1}, {"instrument_name", "BTC-PERPETUAL"},
                          {"bids", levels({{100, 5}, {99, 5}})}, {"asks", levels({{101, 5}})}});
            c.notify(ch, {{"type", "change"}, {"change_id", 2}, {"prev_change_id", 1}, {"timestamp", 2},
                          {"bids", levels({{100.5, 1}})}, {"asks", json::array{}}});
            // change_id 3..5 "lost": this one doesn't follow 2
            c.notify(ch, {{"type", "change"}, {"change_id", 6}, {"prev_change_id", 5}, {"timestamp", 6},
                          {"bids", levels({{98, 1}})}, {"asks", json::array{}}});
        } else {
            c.notify(ch, {{"type", "snapshot"}, {"change_id", 10}, {"timestamp", 10}, {"instrument_name", "BTC-PERPETUAL"},
                          {"bids", levels({{100, 7}})}, {"asks", levels({{100.5, 2}})}});
        }
    });
    Client client(options_for(server));
    std::mutex mu;
    std::vector<std::int64_t> change_ids;
    double last_best_bid = 0;
    client.set_book_handler([&](const OrderBook& b, std::int64_t) {
        std::lock_guard lock(mu);
        change_ids.push_back(b.change_id());
        last_best_bid = b.best_bid()->price;
    });
    client.subscribe_book("BTC-PERPETUAL");
    client.start();
    REQUIRE(wait_until([&] {
        std::lock_guard lock(mu);
        return change_ids.size() == 3;
    }));
    {
        std::lock_guard lock(mu);
        CHECK(change_ids == std::vector<std::int64_t>({1, 2, 10}));  // the gapped update was never applied
        CHECK_EQ(last_best_bid, 100.0);
    }
    CHECK(server.saw("1:public/unsubscribe"));
    CHECK_EQ(subscribes.load(), 2);
    CHECK_EQ(client.stats().book_resyncs.load(), 1u);
    CHECK_EQ(client.md_latency().count(), 3u);
    client.stop();
}

TEST(client_reconnects_resubscribes_and_fails_inflight) {
    FakeDeribit server([](FakeDeribit::Conn& c, const json::object& req, const std::string& m) {
        if (housekeeping(c, req, m)) return;
        if (m == "public/subscribe") return c.result(req, req.at("params").at("channels"));
        if (m == "public/get_time" && c.index == 1) c.drop = true;  // die mid-request
        else if (m == "public/get_time") c.result(req, 42);
    });
    Client client(options_for(server));
    client.subscribe("ticker.BTC-PERPETUAL.100ms");
    client.start();
    REQUIRE(wait_until([&] { return server.saw("1:public/subscribe"); }));
    auto lost = client.call("public/get_time", {}).get();
    REQUIRE(!lost.ok());
    CHECK_EQ(lost.error->code, errc::disconnected);
    // Backoff starts at ~500 ms; the subscription must be restored on the new connection.
    CHECK(wait_until([&] { return server.saw("2:public/subscribe"); }));
    auto r = client.call("public/get_time", {}).get();
    REQUIRE(r.ok());
    CHECK_EQ(r.result.to_number<int>(), 42);
    CHECK_EQ(server.connections(), 2);
    CHECK_EQ(client.stats().disconnects.load(), 1u);
    client.stop();
}

TEST(client_answers_heartbeat_test_request) {
    FakeDeribit server([](FakeDeribit::Conn& c, const json::object& req, const std::string& m) {
        housekeeping(c, req, m);
        if (m == "public/set_heartbeat")
            c.send(json::object{{"jsonrpc", "2.0"}, {"method", "heartbeat"}, {"params", {{"type", "test_request"}}}});
    });
    Client client(options_for(server));
    client.start();
    CHECK(wait_until([&] { return server.saw("1:public/test"); }));
    client.stop();
}

TEST(client_stop_fails_queued_requests_and_rejects_new_ones) {
    // Nothing listens on this port, so the client keeps reconnecting.
    asio::io_context io;
    tcp::acceptor probe(io, {asio::ip::make_address("127.0.0.1"), 0});
    const auto port = std::to_string(probe.local_endpoint().port());
    probe.close();

    ClientOptions o;
    o.host = "127.0.0.1";
    o.port = port;
    o.request_timeout_ms = 10000;
    Client client(o);
    client.start();
    auto queued = client.call("public/get_time", {});
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    const auto start = std::chrono::steady_clock::now();
    client.stop();
    CHECK(std::chrono::steady_clock::now() - start < std::chrono::seconds(3));
    REQUIRE(queued.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
    CHECK_EQ(queued.get().error->code, errc::shutting_down);
    auto after = client.call("public/get_time", {}).get();
    CHECK_EQ(after.error->code, errc::shutting_down);
}
