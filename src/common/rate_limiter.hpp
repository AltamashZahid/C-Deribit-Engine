#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>

namespace de {

// Token bucket: refills at `rate_per_sec` tokens per second up to `burst` tokens; each
// request spends one. The caller passes the current time so the logic is deterministic
// in tests. Not thread-safe - each bucket is owned by the Deribit client's I/O thread.
// A rate <= 0 disables limiting.
class TokenBucket {
public:
    TokenBucket(double rate_per_sec, double burst)
        : rate_(rate_per_sec), burst_(std::max(1.0, burst)), tokens_(burst_) {}

    bool try_acquire(std::int64_t now_ns, double cost = 1.0) noexcept {
        if (rate_ <= 0) return true;
        refill(now_ns);
        if (tokens_ < cost) return false;
        tokens_ -= cost;
        return true;
    }

    // Nanoseconds until `cost` tokens will be available (0 if they are available now).
    std::int64_t wait_ns(std::int64_t now_ns, double cost = 1.0) noexcept {
        if (rate_ <= 0) return 0;
        refill(now_ns);
        if (tokens_ >= cost) return 0;
        return static_cast<std::int64_t>(std::ceil((cost - tokens_) / rate_ * 1e9));
    }

    double tokens() const noexcept { return tokens_; }

private:
    void refill(std::int64_t now_ns) noexcept {
        if (last_ns_ == kNever) {
            last_ns_ = now_ns;
            return;
        }
        if (now_ns <= last_ns_) return;
        tokens_ = std::min(burst_, tokens_ + static_cast<double>(now_ns - last_ns_) * rate_ / 1e9);
        last_ns_ = now_ns;
    }

    static constexpr std::int64_t kNever = std::numeric_limits<std::int64_t>::min();

    double rate_;
    double burst_;
    double tokens_;
    std::int64_t last_ns_ = kNever;
};

}  // namespace de
