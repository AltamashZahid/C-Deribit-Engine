#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cmath>
#include <cstdint>
#include <format>
#include <limits>
#include <string>
#include <string_view>

namespace de {

// Formats a nanosecond duration as "850ns", "12.4us", "3.10ms" or "1.25s".
inline std::string format_ns(std::int64_t ns) {
    if (ns < 1'000) return std::format("{}ns", ns);
    if (ns < 1'000'000) return std::format("{:.1f}us", static_cast<double>(ns) / 1e3);
    if (ns < 1'000'000'000) return std::format("{:.2f}ms", static_cast<double>(ns) / 1e6);
    return std::format("{:.2f}s", static_cast<double>(ns) / 1e9);
}

// Log-linear latency histogram in the style of HdrHistogram. Values below 32 are kept
// exactly; above that, every power-of-two range is split into 32 equal buckets, so a
// reported percentile is within ~3% of the true value. Memory is fixed (~10 KB) no
// matter how many samples are recorded.
//
// record() is wait-free (relaxed atomic increments) and may be called from any thread.
// Readers see a slightly stale but internally reasonable view while writers are active.
class LatencyHistogram {
public:
    static constexpr int kSubBits = 5;
    static constexpr int kSub = 1 << kSubBits;  // buckets per power of two
    static constexpr int kMaxBits = 42;         // ~73 minutes in nanoseconds
    static constexpr int kBuckets = (kMaxBits - kSubBits + 1) * kSub;

    static constexpr int bucket_of(std::uint64_t v) noexcept {
        constexpr std::uint64_t kMax = (std::uint64_t{1} << kMaxBits) - 1;
        if (v > kMax) v = kMax;
        if (v < static_cast<std::uint64_t>(kSub)) return static_cast<int>(v);
        const int top = std::bit_width(v) - 1;  // >= kSubBits
        const int shift = top - kSubBits;
        const int sub = static_cast<int>((v >> shift) & (kSub - 1));
        return (shift + 1) * kSub + sub;
    }

    // Smallest value that maps to `bucket`.
    static constexpr std::uint64_t bucket_lower(int bucket) noexcept {
        if (bucket < kSub) return static_cast<std::uint64_t>(bucket);
        const int shift = bucket / kSub - 1;
        const int sub = bucket % kSub;
        return static_cast<std::uint64_t>(kSub + sub) << shift;
    }

    // Value reported for samples in `bucket` (its midpoint).
    static constexpr std::uint64_t bucket_value(int bucket) noexcept {
        if (bucket < kSub) return static_cast<std::uint64_t>(bucket);
        const int shift = bucket / kSub - 1;
        return bucket_lower(bucket) + ((std::uint64_t{1} << shift) - 1) / 2;
    }

    void record(std::int64_t ns) noexcept {
        const auto v = static_cast<std::uint64_t>(ns < 0 ? 0 : ns);
        counts_[static_cast<std::size_t>(bucket_of(v))].fetch_add(1, std::memory_order_relaxed);
        count_.fetch_add(1, std::memory_order_relaxed);
        sum_.fetch_add(v, std::memory_order_relaxed);
        auto cur = min_.load(std::memory_order_relaxed);
        while (v < cur && !min_.compare_exchange_weak(cur, v, std::memory_order_relaxed)) {
        }
        cur = max_.load(std::memory_order_relaxed);
        while (v > cur && !max_.compare_exchange_weak(cur, v, std::memory_order_relaxed)) {
        }
    }

    std::uint64_t count() const noexcept { return count_.load(std::memory_order_relaxed); }

    std::int64_t min() const noexcept {
        return count() ? static_cast<std::int64_t>(min_.load(std::memory_order_relaxed)) : 0;
    }
    std::int64_t max() const noexcept {
        return static_cast<std::int64_t>(max_.load(std::memory_order_relaxed));
    }
    double mean() const noexcept {
        const auto n = count();
        return n ? static_cast<double>(sum_.load(std::memory_order_relaxed)) / static_cast<double>(n) : 0.0;
    }

    // p in [0, 100]. Returns 0 when empty.
    std::int64_t percentile(double p) const noexcept {
        std::uint64_t total = 0;
        for (const auto& c : counts_) total += c.load(std::memory_order_relaxed);
        if (total == 0) return 0;
        if (p >= 100.0) return max();  // tracked exactly, no bucket rounding
        const double clamped = std::clamp(p, 0.0, 100.0);
        auto rank = static_cast<std::uint64_t>(std::ceil(clamped / 100.0 * static_cast<double>(total)));
        rank = std::clamp<std::uint64_t>(rank, 1, total);
        std::uint64_t seen = 0;
        for (int b = 0; b < kBuckets; ++b) {
            seen += counts_[static_cast<std::size_t>(b)].load(std::memory_order_relaxed);
            if (seen >= rank) {
                const auto v = static_cast<std::int64_t>(bucket_value(b));
                return std::clamp(v, min(), max());
            }
        }
        return max();
    }

    void reset() noexcept {
        for (auto& c : counts_) c.store(0, std::memory_order_relaxed);
        count_.store(0, std::memory_order_relaxed);
        sum_.store(0, std::memory_order_relaxed);
        min_.store(std::numeric_limits<std::uint64_t>::max(), std::memory_order_relaxed);
        max_.store(0, std::memory_order_relaxed);
    }

    // One-line report, e.g. "order rtt   n=20  mean=152ms p50=... p99=... max=...".
    std::string summary(std::string_view name) const {
        if (count() == 0) return std::format("{:<22} n=0", name);
        return std::format("{:<22} n={:<8} mean={:<9} p50={:<9} p90={:<9} p99={:<9} p99.9={:<9} max={}",
                           name, count(), format_ns(static_cast<std::int64_t>(mean())),
                           format_ns(percentile(50)), format_ns(percentile(90)),
                           format_ns(percentile(99)), format_ns(percentile(99.9)), format_ns(max()));
    }

private:
    std::array<std::atomic<std::uint64_t>, kBuckets> counts_{};
    std::atomic<std::uint64_t> count_{0};
    std::atomic<std::uint64_t> sum_{0};
    std::atomic<std::uint64_t> min_{std::numeric_limits<std::uint64_t>::max()};
    std::atomic<std::uint64_t> max_{0};
};

}  // namespace de
