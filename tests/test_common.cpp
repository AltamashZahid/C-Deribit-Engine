#include <atomic>
#include <cstdint>
#include <random>
#include <thread>
#include <vector>

#include "common/config.hpp"
#include "common/latency_histogram.hpp"
#include "common/mpmc_queue.hpp"
#include "common/rate_limiter.hpp"
#include "test.hpp"

using namespace de;

TEST(histogram_small_values_are_exact) {
    LatencyHistogram h;
    for (int v = 0; v < 32; ++v) CHECK_EQ(LatencyHistogram::bucket_value(LatencyHistogram::bucket_of(v)), static_cast<std::uint64_t>(v));
    h.record(7);
    CHECK_EQ(h.percentile(50), 7);
    CHECK_EQ(h.min(), 7);
    CHECK_EQ(h.max(), 7);
}

TEST(histogram_buckets_are_contiguous_and_bounded_error) {
    // Every value maps to a bucket whose range contains it, and buckets tile the number line.
    std::mt19937_64 rng(1);
    for (int i = 0; i < 200000; ++i) {
        const std::uint64_t v = rng() >> (rng() % 40 + 22);  // spread over many magnitudes
        const int b = LatencyHistogram::bucket_of(v);
        REQUIRE(b >= 0 && b < LatencyHistogram::kBuckets);
        CHECK(LatencyHistogram::bucket_lower(b) <= v);
        if (b + 1 < LatencyHistogram::kBuckets) CHECK(v < LatencyHistogram::bucket_lower(b + 1));
        const double rep = static_cast<double>(LatencyHistogram::bucket_value(b));
        if (v > 0) CHECK(std::fabs(rep - static_cast<double>(v)) / static_cast<double>(v) <= 1.0 / 32);
    }
}

TEST(histogram_percentiles_on_uniform_data) {
    LatencyHistogram h;
    for (int v = 1; v <= 100000; ++v) h.record(v);
    CHECK_EQ(h.count(), 100000u);
    CHECK_NEAR(h.percentile(50), 50000, 50000 * 0.035);
    CHECK_NEAR(h.percentile(99), 99000, 99000 * 0.035);
    CHECK_EQ(h.percentile(100), 100000);
    CHECK_NEAR(h.mean(), 50000.5, 0.01);
    h.reset();
    CHECK_EQ(h.count(), 0u);
    CHECK_EQ(h.percentile(50), 0);
}

TEST(histogram_concurrent_record) {
    LatencyHistogram h;
    std::vector<std::thread> ts;
    for (int t = 0; t < 4; ++t)
        ts.emplace_back([&h, t] {
            for (int i = 0; i < 50000; ++i) h.record(1000 * (t + 1));
        });
    for (auto& t : ts) t.join();
    CHECK_EQ(h.count(), 200000u);
    CHECK_EQ(h.min(), 1000);
    CHECK_EQ(h.max(), 4000);
}

TEST(mpmc_queue_fifo_and_capacity) {
    MpmcQueue<int> q(4);
    int out = 0;
    CHECK(!q.try_pop(out));
    for (int i = 0; i < 4; ++i) CHECK(q.try_push(i));
    CHECK(!q.try_push(99));  // full
    for (int i = 0; i < 4; ++i) {
        REQUIRE(q.try_pop(out));
        CHECK_EQ(out, i);
    }
    CHECK(!q.try_pop(out));
    // wrap around many times
    for (int i = 0; i < 1000; ++i) {
        CHECK(q.try_push(i));
        REQUIRE(q.try_pop(out));
        CHECK_EQ(out, i);
    }
}

TEST(mpmc_queue_rejects_bad_capacity) {
    bool threw = false;
    try {
        MpmcQueue<int> q(6);
    } catch (const std::invalid_argument&) {
        threw = true;
    }
    CHECK(threw);
}

TEST(mpmc_queue_many_producers_many_consumers) {
    // 4 producers push disjoint ranges; 4 consumers pop. Every value must arrive exactly once.
    constexpr int kProducers = 4, kConsumers = 4, kPer = 100000;
    MpmcQueue<std::uint32_t> q(1024);
    std::vector<std::atomic<std::uint8_t>> seen(kProducers * kPer);
    std::atomic<int> consumed{0};
    std::vector<std::thread> ts;
    for (int p = 0; p < kProducers; ++p)
        ts.emplace_back([&, p] {
            for (int i = 0; i < kPer; ++i) {
                const auto v = static_cast<std::uint32_t>(p * kPer + i);
                while (!q.try_push(v)) std::this_thread::yield();
            }
        });
    for (int c = 0; c < kConsumers; ++c)
        ts.emplace_back([&] {
            std::uint32_t v;
            while (consumed.load() < kProducers * kPer) {
                if (q.try_pop(v)) {
                    seen[v].fetch_add(1);
                    consumed.fetch_add(1);
                } else {
                    std::this_thread::yield();
                }
            }
        });
    for (auto& t : ts) t.join();
    int bad = 0;
    for (auto& s : seen) bad += s.load() != 1;
    CHECK_EQ(bad, 0);
}

TEST(token_bucket_burst_then_refill) {
    TokenBucket b(10, 5);  // 10/s, burst 5
    const std::int64_t t0 = 1'000'000'000;
    for (int i = 0; i < 5; ++i) CHECK(b.try_acquire(t0));
    CHECK(!b.try_acquire(t0));
    CHECK_EQ(b.wait_ns(t0), 100'000'000);  // one token every 100 ms
    CHECK(!b.try_acquire(t0 + 99'000'000));
    CHECK(b.try_acquire(t0 + 100'000'000));
    // A long idle period refills only up to the burst size.
    const std::int64_t later = t0 + 60'000'000'000;
    for (int i = 0; i < 5; ++i) CHECK(b.try_acquire(later));
    CHECK(!b.try_acquire(later));
}

TEST(token_bucket_zero_rate_is_unlimited) {
    TokenBucket b(0, 1);
    for (int i = 0; i < 1000; ++i) CHECK(b.try_acquire(0));
    CHECK_EQ(b.wait_ns(0), 0);
}

TEST(dotenv_parsing) {
    const auto m = parse_dotenv(
        "# comment\n"
        "\n"
        "DERIBIT_CLIENT_ID=abc\r\n"
        "export DERIBIT_CLIENT_SECRET = \"s3 cr#et\"\n"
        "LOG_LEVEL=debug # trailing comment\n"
        "EMPTY=\n"
        "QUOTED='single'\n"
        "no_equals_line\n"
        "DERIBIT_CLIENT_ID=override");
    CHECK_EQ(m.at("DERIBIT_CLIENT_ID"), std::string("override"));
    CHECK_EQ(m.at("DERIBIT_CLIENT_SECRET"), std::string("s3 cr#et"));
    CHECK_EQ(m.at("LOG_LEVEL"), std::string("debug"));
    CHECK_EQ(m.at("EMPTY"), std::string(""));
    CHECK_EQ(m.at("QUOTED"), std::string("single"));
    CHECK(!m.contains("no_equals_line"));
}

TEST(config_from_map_and_validation) {
    auto c = Config::from_map({{"MD_SERVER_PORT", "9001"}, {"LOG_LEVEL", "warn"}, {"ORDER_RATE_PER_SEC", "2.5"},
                               {"HEARTBEAT_SECONDS", "3"}, {"DERIBIT_CLIENT_ID", "id"}});
    CHECK_EQ(c.server_port, 9001);
    CHECK(c.log_level == LogLevel::Warn);
    CHECK_NEAR(c.order_rate_per_sec, 2.5, 1e-12);
    CHECK_EQ(c.heartbeat_seconds, 10);  // clamped to Deribit's minimum
    CHECK(!c.has_credentials());        // secret missing

    bool threw = false;
    try {
        Config::from_map({{"MD_SERVER_PORT", "80x"}});
    } catch (const std::runtime_error&) {
        threw = true;
    }
    CHECK(threw);
    threw = false;
    try {
        Config::from_map({{"LOG_LEVEL", "loud"}});
    } catch (const std::runtime_error&) {
        threw = true;
    }
    CHECK(threw);
}
