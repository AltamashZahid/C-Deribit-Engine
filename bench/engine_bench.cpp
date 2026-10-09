// Offline micro-benchmarks of the engine's hot path. No network: every number here is
// pure CPU time on this machine, which is what "microsecond" latency claims are about.
//
//   engine_bench [iterations]

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <cstdio>
#include <format>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <boost/json.hpp>

#include "common/latency_histogram.hpp"
#include "common/logger.hpp"
#include "common/mpmc_queue.hpp"
#include "common/time.hpp"
#include "deribit/book_feed.hpp"
#include "deribit/order_book.hpp"
#include "server/book_message.hpp"

using namespace de;
namespace json = boost::json;

namespace {

// Feeds results into a volatile so the optimiser can't delete the benchmarked work.
volatile double g_sink = 0;
void keep(double x) { g_sink = g_sink + x; }

// Runs op(i) for i in [0, n) twice: once untimed, for throughput (ns/op), and once with a
// clock read around every call, for the latency distribution. Percentiles therefore include
// one clock read (~40 ns) and, on Windows, steady_clock's 100 ns tick granularity.
template <class Reset, class Op>
void measure(const char* name, int n, Reset reset, Op op) {
    reset();
    const auto t0 = now_ns();
    for (int i = 0; i < n; ++i) op(i);
    const auto total = now_ns() - t0;

    reset();
    LatencyHistogram h;
    for (int i = 0; i < n; ++i) {
        const auto t = now_ns();
        op(i);
        h.record(now_ns() - t);
    }
    std::printf("%-36s %9.1f ns/op %11.0f ops/s   p50=%-8s p99=%-8s p99.9=%-8s\n", name,
                static_cast<double>(total) / n, n * 1e9 / static_cast<double>(total), format_ns(h.percentile(50)).c_str(),
                format_ns(h.percentile(99)).c_str(), format_ns(h.percentile(99.9)).c_str());
}

void no_reset() {}

// Builds a Deribit-style snapshot with `levels` per side around 80000 (tick 0.5).
OrderBook make_book(int levels) {
    OrderBook book("BTC-PERPETUAL");
    std::vector<PriceLevel> bids, asks;
    for (int i = 0; i < levels; ++i) {
        bids.push_back({80000.0 - 0.5 * i, 10.0 * (i + 1)});
        asks.push_back({80000.5 + 0.5 * i, 10.0 * (i + 1)});
    }
    book.load_snapshot(1, bids, asks);
    return book;
}

// Pre-generates `n` sequential Deribit change notifications, each touching `k` levels near
// the top of the book (the same shape as book.BTC-PERPETUAL.100ms traffic).
std::vector<std::string> make_change_frames(int n, int k) {
    std::mt19937 rng(3);
    std::geometric_distribution<int> distance(0.25);
    std::uniform_int_distribution<int> amount(0, 2000);
    std::vector<std::string> frames;
    frames.reserve(static_cast<std::size_t>(n));
    for (int i = 0; i < n; ++i) {
        std::string bids, asks;
        for (int j = 0; j < k; ++j) {
            const bool bid = rng() & 1;
            const int d = distance(rng);
            const int a = amount(rng) * 10;
            const double price = bid ? 80000.0 - 0.5 * d : 80000.5 + 0.5 * d;
            auto& side = bid ? bids : asks;
            if (!side.empty()) side += ',';
            side += std::format(R"(["{}",{},{}])", a == 0 ? "delete" : "change", price, a);
        }
        frames.push_back(std::format(
            R"({{"jsonrpc":"2.0","method":"subscription","params":{{"channel":"book.BTC-PERPETUAL.100ms","data":{{"type":"change","timestamp":1791497024149,"prev_change_id":{},"instrument_name":"BTC-PERPETUAL","change_id":{},"bids":[{}],"asks":[{}]}}}}}})",
            i + 1, i + 2, bids, asks));
    }
    return frames;
}

void bench_order_book(int n) {
    std::mt19937 rng(1);
    std::geometric_distribution<int> distance(0.25);  // most updates within a few ticks of the top
    std::uniform_int_distribution<int> amount(0, 50);  // 0 = delete the level
    struct Op {
        BookSide side;
        double price, amount;
    };
    std::vector<Op> ops(static_cast<std::size_t>(n));
    for (auto& op : ops) {
        const bool bid = rng() & 1;
        const int d = distance(rng);
        op = {bid ? BookSide::Bid : BookSide::Ask, bid ? 80000.0 - 0.5 * d : 80000.5 + 0.5 * d, 10.0 * amount(rng)};
    }
    OrderBook book;
    measure("order book set_level (1000 levels)", n, [&] { book = make_book(1000); },
            [&](int i) { book.set_level(ops[i].side, ops[i].price, ops[i].amount); });
    keep(book.best_bid()->price);
}

void bench_parse_and_apply(int n) {
    const auto frames = make_change_frames(n, 6);
    std::vector<unsigned char> arena(1 << 20);
    measure("json parse, arena allocator", n, no_reset, [&](int i) {
        boost::json::monotonic_resource mr(arena.data(), arena.size());
        json::value v = json::parse(frames[i], &mr);
        keep(static_cast<double>(v.as_object().size()));
    });
    measure("json parse, default allocator", n, no_reset, [&](int i) {
        json::value v = json::parse(frames[i]);
        keep(static_cast<double>(v.as_object().size()));
    });
    OrderBook book;
    int bad = 0;
    measure("parse + apply Deribit book frame", n, [&] { book = make_book(1000); },
            [&](int i) {
                boost::json::monotonic_resource mr(arena.data(), arena.size());
                json::value v = json::parse(frames[i], &mr);
                const auto& data = v.as_object().at("params").as_object().at("data").as_object();
                bad += deribit::apply_book_message(book, data) != deribit::BookApply::Applied;
            });
    if (bad) std::printf("  !! %d frames did not apply in sequence\n", bad);
}

void bench_build_message(int n) {
    const auto book = make_book(1000);
    measure("build top-10 client message", n, no_reset, [&](int i) {
        auto msg = build_book_message(book, 10, i);
        keep(static_cast<double>(msg.size()));
    });
}

void bench_queue(int n) {
    MpmcQueue<std::uint64_t> q(1024);
    std::uint64_t v = 0;
    measure("mpmc queue push+pop (1 thread)", n, no_reset, [&](int i) {
        q.try_push(static_cast<std::uint64_t>(i));
        q.try_pop(v);
    });
    keep(static_cast<double>(v));

    // 2 producers -> 2 consumers, all contending on the same queue.
    constexpr int kThreads = 2;
    const auto per = static_cast<std::uint64_t>(n);
    MpmcQueue<std::uint64_t> q2(4096);
    std::atomic<std::uint64_t> popped{0};
    std::vector<std::thread> ts;
    const auto t0 = now_ns();
    for (int p = 0; p < kThreads; ++p)
        ts.emplace_back([&] {
            for (std::uint64_t i = 0; i < per; ++i)
                while (!q2.try_push(i)) std::this_thread::yield();
        });
    for (int c = 0; c < kThreads; ++c)
        ts.emplace_back([&] {
            std::uint64_t x;
            while (popped.load(std::memory_order_relaxed) < per * kThreads) {
                if (q2.try_pop(x)) popped.fetch_add(1, std::memory_order_relaxed);
                else std::this_thread::yield();
            }
        });
    for (auto& t : ts) t.join();
    const auto dt = now_ns() - t0;
    std::printf("%-36s %9.1f ns/op %11.0f ops/s\n", "mpmc queue 2 producers / 2 consumers",
                static_cast<double>(dt) / static_cast<double>(per * kThreads),
                static_cast<double>(per * kThreads) * 1e9 / static_cast<double>(dt));
}

void bench_histogram(int n) {
    LatencyHistogram target;
    measure("latency histogram record", n, no_reset, [&](int i) { target.record(i); });
    keep(static_cast<double>(target.count()));
}

void bench_logger(const std::string& path) {
    Logger::instance().start(path, LogLevel::Info, LogLevel::Off);
    const int n = 10000;  // below the queue capacity: this measures the caller, not drops
    measure("LOG_INFO call (async logger)", n,
            [] { std::this_thread::sleep_for(std::chrono::milliseconds(300)); },  // let the writer drain
            [](int i) { LOG_INFO("order {} acked in {} ns on {}", i, 12345, "BTC-PERPETUAL"); });
    Logger::instance().stop();
    std::printf("%-36s dropped=%llu\n", "", static_cast<unsigned long long>(Logger::instance().dropped()));
}

void bench_clock(int n) {
    std::int64_t sum = 0;
    const auto start = now_ns();
    for (int i = 0; i < n; ++i) sum += now_ns();
    const auto total = now_ns() - start;
    keep(static_cast<double>(sum));
    std::printf("%-36s %9.1f ns/op\n", "steady_clock::now() (timer cost)", static_cast<double>(total) / n);
}

}  // namespace

int main(int argc, char** argv) {
    const int iterations = argc > 1 ? std::max(1000, std::atoi(argv[1])) : 200000;
    std::printf("engine_bench: %d iterations per benchmark\n\n", iterations);
    bench_clock(iterations);
    bench_order_book(iterations);
    bench_parse_and_apply(iterations);
    bench_build_message(iterations);
    bench_queue(iterations);
    bench_histogram(iterations);
    bench_logger("engine_bench.log");
    return 0;
}
