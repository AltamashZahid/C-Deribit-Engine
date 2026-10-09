#pragma once

#include <chrono>
#include <cstdint>

namespace de {

// Monotonic nanoseconds. steady_clock is system-wide on both Windows (QPC) and Linux
// (CLOCK_MONOTONIC), so timestamps taken in two processes on one machine can be
// subtracted - md_client_bench relies on this to measure delivery latency.
inline std::int64_t now_ns() noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

// Wall-clock nanoseconds since the Unix epoch. Only used for log timestamps.
inline std::int64_t wall_ns() noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

constexpr std::int64_t ms_to_ns(std::int64_t ms) noexcept { return ms * 1'000'000; }

}  // namespace de
