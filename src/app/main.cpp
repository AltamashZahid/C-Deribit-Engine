#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <iostream>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "app/app.hpp"
#include "common/logger.hpp"
#include "common/time.hpp"
#include "server/book_message.hpp"

using namespace de;

namespace {

std::atomic<bool> g_interrupted{false};
void on_signal(int) { g_interrupted.store(true); }

struct Args {
    std::string env_path = ".env";
    bool cli = true;
    double synthetic_rate = -1;  // < 0: off; 0: as fast as possible
    int duration_s = 0;          // --no-cli: 0 = until Ctrl+C
    std::size_t depth = 10;
    int port = -1;
    std::vector<std::string> subscribe;
};

void usage() {
    std::puts(
        "usage: deribit_engine [options]\n"
        "  --env PATH          config file (default .env)\n"
        "  --no-cli            run headless (server + streaming only)\n"
        "  --duration SEC      with --no-cli: exit after SEC seconds (default: until Ctrl+C)\n"
        "  --subscribe INST    stream INST's book from startup (repeatable)\n"
        "  --depth N           levels per side in published book messages (default 10)\n"
        "  --port N            market data server port (overrides MD_SERVER_PORT)\n"
        "  --synthetic RATE    no Deribit: publish a synthetic SYN-PERP book at RATE msg/s\n"
        "                      (0 = as fast as possible) to load-test the server");
}

Args parse_args(int argc, char** argv) {
    Args a;
    auto need = [&](int& i) -> std::string {
        if (i + 1 >= argc) throw std::runtime_error(std::string("missing value for ") + argv[i]);
        return argv[++i];
    };
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--env") a.env_path = need(i);
        else if (arg == "--no-cli") a.cli = false;
        else if (arg == "--duration") a.duration_s = std::stoi(need(i));
        else if (arg == "--subscribe") a.subscribe.push_back(need(i));
        else if (arg == "--depth") a.depth = static_cast<std::size_t>(std::stoul(need(i)));
        else if (arg == "--port") a.port = std::stoi(need(i));
        else if (arg == "--synthetic") a.synthetic_rate = std::stod(need(i));
        else if (arg == "--help" || arg == "-h") {
            usage();
            std::exit(0);
        } else {
            throw std::runtime_error("unknown option " + arg);
        }
    }
    if (a.depth == 0) a.depth = 1;
    return a;
}

// Generates a random-walk book for "SYN-PERP" and publishes it through the same path as
// real Deribit data. Used to measure server fan-out without network noise.
void run_synthetic(App& app, double rate, const std::atomic<bool>& stop) {
    OrderBook book("SYN-PERP");
    std::vector<PriceLevel> bids, asks;
    const double tick = 0.5, mid = 50'000;
    for (int i = 1; i <= 200; ++i) {
        bids.push_back({mid - tick * i, 1000.0 * i});
        asks.push_back({mid + tick * i, 1000.0 * i});
    }
    book.load_snapshot(1, bids, asks);
    std::mt19937 rng(42);
    std::geometric_distribution<int> distance(0.3);
    std::uniform_int_distribution<int> amount(1, 500);
    std::uint64_t sent = 0;
    const auto start = now_ns();
    while (!stop.load(std::memory_order_relaxed)) {
        if (rate > 0) {
            // Pace by spinning: sleep_for can oversleep by up to ~15 ms on Windows, which
            // would turn a steady rate into bursts.
            const auto due = start + static_cast<std::int64_t>(static_cast<double>(sent) * 1e9 / rate);
            const auto now = now_ns();
            if (now < due) {
                if (due - now > 2'000'000) std::this_thread::sleep_for(std::chrono::milliseconds(1));
                else std::this_thread::yield();
                continue;
            }
        }
        const bool bid = rng() & 1;
        const double offset = tick * (1 + distance(rng));
        book.set_level(bid ? BookSide::Bid : BookSide::Ask, bid ? mid - offset : mid + offset, 10.0 * amount(rng));
        book.set_change_id(book.change_id() + 1);
        const auto recv = now_ns();
        if (app.server->subscriber_count(book.instrument()) > 0) {
            auto msg = std::make_shared<const std::string>(build_book_message(book, app.depth, recv));
            app.server->publish(book.instrument(), std::move(msg), recv);
        }
        ++sent;
    }
}

}  // namespace

int main(int argc, char** argv) {
    Args args;
    App app;
    try {
        args = parse_args(argc, argv);
        app.cfg = Config::load(args.env_path);
        if (args.port >= 0) app.cfg.server_port = static_cast<std::uint16_t>(args.port);
    } catch (const std::exception& e) {
        std::fprintf(stderr, "error: %s\n", e.what());
        usage();
        return 2;
    }
    app.depth = args.depth;
    const auto& cfg = app.cfg;
    Logger::instance().start(cfg.log_file, cfg.log_level, cfg.console_log_level);
    LOG_INFO("deribit_engine starting");

    const bool synthetic = args.synthetic_rate >= 0;
    if (!synthetic) {
        deribit::ClientOptions o;
        o.host = cfg.deribit_host;
        o.port = cfg.deribit_port;
        o.path = cfg.deribit_path;
        o.client_id = cfg.client_id;
        o.client_secret = cfg.client_secret;
        o.ca_file = cfg.ca_file;
        o.book_interval = cfg.book_interval;
        o.heartbeat_seconds = cfg.heartbeat_seconds;
        o.request_timeout_ms = cfg.request_timeout_ms;
        o.order_rate_per_sec = cfg.order_rate_per_sec;
        o.order_burst = cfg.order_burst;
        o.rpc_rate_per_sec = cfg.rpc_rate_per_sec;
        o.rpc_burst = cfg.rpc_burst;
        app.client = std::make_unique<deribit::Client>(std::move(o));
        app.client->set_book_handler([&app](const OrderBook& b, std::int64_t recv_ns) { app.on_book(b, recv_ns); });
        app.client->set_state_handler([](deribit::ConnectionState s, bool authed) {
            LOG_INFO("deribit state: {}{}", deribit::to_string(s), authed ? " (authenticated)" : "");
        });
    }

    ServerOptions so;
    so.bind = cfg.server_bind;
    so.port = cfg.server_port;
    so.threads = cfg.server_threads;
    so.max_queue = static_cast<std::size_t>(cfg.server_max_queue);
    app.server = std::make_unique<MarketDataServer>(so, [&app](const std::string& inst, bool wanted) {
        if (!app.client) return;
        if (wanted) app.acquire_book(inst);
        else app.release_book(inst);
    });

    try {
        app.server->start();
    } catch (const std::exception& e) {
        std::fprintf(stderr, "error: cannot start market data server on %s:%u: %s\n", cfg.server_bind.c_str(),
                     static_cast<unsigned>(cfg.server_port), e.what());
        return 1;
    }
    std::printf("market data server: ws://%s:%u\n", cfg.server_bind.c_str(), static_cast<unsigned>(app.server->port()));

    std::atomic<bool> stop_synthetic{false};
    std::thread synthetic_thread;
    if (synthetic) {
        std::printf("synthetic mode: publishing SYN-PERP at %s\n",
                    args.synthetic_rate > 0 ? (std::to_string(static_cast<long long>(args.synthetic_rate)) + " msg/s").c_str()
                                            : "max rate");
        synthetic_thread = std::thread([&] { run_synthetic(app, args.synthetic_rate, stop_synthetic); });
    } else {
        std::printf("deribit: wss://%s%s (%s)\n", cfg.deribit_host.c_str(), cfg.deribit_path.c_str(),
                    cfg.has_credentials() ? "credentials loaded" : "no credentials: public data only");
        app.client->start();
        for (const auto& inst : args.subscribe) {
            std::lock_guard lock(app.sub_mu);
            app.cli_subs.insert(inst);
        }
        for (const auto& inst : args.subscribe) app.acquire_book(inst);
    }

    if (args.cli) {
        run_cli(app);
    } else {
        std::signal(SIGINT, on_signal);
        std::signal(SIGTERM, on_signal);
        const auto start = std::chrono::steady_clock::now();
        while (!g_interrupted.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            if (args.duration_s > 0 && std::chrono::steady_clock::now() - start >= std::chrono::seconds(args.duration_s)) break;
        }
        app.print_stats();
    }

    stop_synthetic = true;
    if (synthetic_thread.joinable()) synthetic_thread.join();
    app.server->stop();
    if (app.client) app.client->stop();
    LOG_INFO("deribit_engine stopped");
    Logger::instance().stop();
    return 0;
}
