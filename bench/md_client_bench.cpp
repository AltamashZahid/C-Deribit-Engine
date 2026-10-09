// Load generator for the market data server: opens N WebSocket clients, subscribes them
// all to one instrument and measures delivery latency (engine receive time "recv_ns" in
// each message -> client receive time, same machine steady clock) and throughput.
//
//   md_client_bench [--port 8080] [--clients 10] [--instrument SYN-PERP] [--seconds 10] [--threads 2]
//                   [--csv results.csv]   (appends one machine-readable result row)
//
// Pair it with `deribit_engine --no-cli --synthetic 0` for a pure server load test, or
// with the normal engine and a real instrument such as BTC-PERPETUAL.

#include <algorithm>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cstdio>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <boost/asio/connect.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/strand.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>

#include "common/latency_histogram.hpp"
#include "common/time.hpp"

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace websocket = beast::websocket;
using tcp = asio::ip::tcp;
using de::LatencyHistogram;

namespace {

struct Shared {
    LatencyHistogram latency;
    std::atomic<std::uint64_t> messages{0};
    std::atomic<std::uint64_t> bytes{0};
    std::atomic<int> ready{0};
    std::atomic<int> failed{0};
    std::atomic<bool> measuring{false};
};

// Extracts the integer after "recv_ns": without parsing the whole JSON document.
bool find_recv_ns(std::string_view msg, std::int64_t& out) {
    constexpr std::string_view key = "\"recv_ns\":";
    const auto pos = msg.find(key);
    if (pos == std::string_view::npos) return false;
    const char* first = msg.data() + pos + key.size();
    return std::from_chars(first, msg.data() + msg.size(), out).ec == std::errc{};
}

class BenchClient : public std::enable_shared_from_this<BenchClient> {
public:
    BenchClient(asio::io_context& io, Shared& shared) : ws_(asio::make_strand(io)), shared_(shared) {}

    void start(const tcp::resolver::results_type& endpoints, const std::string& host, const std::string& instrument) {
        auto self = shared_from_this();
        beast::get_lowest_layer(ws_).async_connect(endpoints, [self, host, instrument](beast::error_code ec, const tcp::endpoint&) {
            if (ec) return self->fail(ec);
            beast::get_lowest_layer(self->ws_).socket().set_option(tcp::no_delay(true));
            self->ws_.async_handshake(host, "/", [self, instrument](beast::error_code ec) {
                if (ec) return self->fail(ec);
                self->subscribe_ = R"({"op":"subscribe","instrument":")" + instrument + "\"}";
                self->ws_.async_write(asio::buffer(self->subscribe_), [self](beast::error_code ec, std::size_t) {
                    if (ec) return self->fail(ec);
                    self->shared_.ready.fetch_add(1);
                    self->read();
                });
            });
        });
    }

    void close() {
        asio::post(ws_.get_executor(), [self = shared_from_this()] {
            beast::error_code ignored;
            beast::get_lowest_layer(self->ws_).socket().close(ignored);
        });
    }

private:
    void read() {
        ws_.async_read(buffer_, [self = shared_from_this()](beast::error_code ec, std::size_t n) {
            if (ec) return;  // closed at the end of the run
            const auto now = de::now_ns();
            const auto data = self->buffer_.cdata();
            const std::string_view msg(static_cast<const char*>(data.data()), data.size());
            std::int64_t recv_ns = 0;
            if (self->shared_.measuring.load(std::memory_order_relaxed) && find_recv_ns(msg, recv_ns)) {
                self->shared_.latency.record(now - recv_ns);
                self->shared_.messages.fetch_add(1, std::memory_order_relaxed);
                self->shared_.bytes.fetch_add(n, std::memory_order_relaxed);
            }
            self->buffer_.consume(n);
            self->read();
        });
    }

    void fail(beast::error_code ec) {
        shared_.failed.fetch_add(1);
        std::fprintf(stderr, "client failed: %s\n", ec.message().c_str());
    }

    websocket::stream<beast::tcp_stream> ws_;
    beast::flat_buffer buffer_;
    Shared& shared_;
    std::string subscribe_;
};

}  // namespace

int main(int argc, char** argv) {
    std::string host = "127.0.0.1", port = "8080", instrument = "SYN-PERP";
    int clients = 10, seconds = 10, threads = 2;
    std::string csv_path;
    for (int i = 1; i + 1 < argc; i += 2) {
        const std::string a = argv[i], v = argv[i + 1];
        if (a == "--host") host = v;
        else if (a == "--port") port = v;
        else if (a == "--clients") clients = std::max(1, std::stoi(v));
        else if (a == "--instrument") instrument = v;
        else if (a == "--seconds") seconds = std::max(1, std::stoi(v));
        else if (a == "--threads") threads = std::max(1, std::stoi(v));
        else if (a == "--csv") csv_path = v;
        else {
            std::fprintf(stderr, "unknown option %s\n", a.c_str());
            return 2;
        }
    }

    asio::io_context io;
    Shared shared;
    auto endpoints = tcp::resolver(io).resolve(host, port);
    std::vector<std::shared_ptr<BenchClient>> all;
    for (int i = 0; i < clients; ++i) {
        all.push_back(std::make_shared<BenchClient>(io, shared));
        all.back()->start(endpoints, host, instrument);
    }
    auto guard = asio::make_work_guard(io);
    std::vector<std::thread> pool;
    for (int i = 0; i < threads; ++i) pool.emplace_back([&io] { io.run(); });

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (shared.ready.load() + shared.failed.load() < clients && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    if (shared.ready.load() == 0) {
        std::fprintf(stderr, "no client could connect to ws://%s:%s\n", host.c_str(), port.c_str());
        guard.reset();
        io.stop();
        for (auto& t : pool) t.join();
        return 1;
    }
    std::printf("%d/%d clients subscribed to %s, measuring for %d s...\n", shared.ready.load(), clients, instrument.c_str(), seconds);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));  // warm-up
    shared.measuring = true;
    const auto t0 = de::now_ns();
    std::this_thread::sleep_for(std::chrono::seconds(seconds));
    shared.measuring = false;
    const double elapsed = static_cast<double>(de::now_ns() - t0) / 1e9;

    for (auto& c : all) c->close();
    guard.reset();
    for (auto& t : pool) t.join();

    const auto msgs = shared.messages.load();
    std::printf("messages received : %llu (%.0f msg/s total, %.0f msg/s per client)\n", static_cast<unsigned long long>(msgs),
                static_cast<double>(msgs) / elapsed, static_cast<double>(msgs) / elapsed / shared.ready.load());
    std::printf("throughput        : %.2f MB/s\n", static_cast<double>(shared.bytes.load()) / elapsed / 1e6);
    std::printf("%s\n", shared.latency.summary("delivery latency").c_str());
    if (!csv_path.empty()) {
        // clients_connected,clients,msgs_per_s,mb_per_s,p50_us,p90_us,p99_us,p999_us,max_us
        if (std::FILE* f = std::fopen(csv_path.c_str(), "a")) {
            const auto& h = shared.latency;
            std::fprintf(f, "%d,%d,%.0f,%.2f,%.1f,%.1f,%.1f,%.1f,%.1f\n", shared.ready.load(), clients,
                         static_cast<double>(msgs) / elapsed, static_cast<double>(shared.bytes.load()) / elapsed / 1e6,
                         static_cast<double>(h.percentile(50)) / 1e3, static_cast<double>(h.percentile(90)) / 1e3,
                         static_cast<double>(h.percentile(99)) / 1e3, static_cast<double>(h.percentile(99.9)) / 1e3,
                         static_cast<double>(h.max()) / 1e3);
            std::fclose(f);
        }
    }
    return msgs > 0 ? 0 : 1;
}
