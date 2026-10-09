#pragma once

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <format>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

#include "common/mpmc_queue.hpp"
#include "common/time.hpp"

namespace de {

enum class LogLevel : int { Debug = 0, Info = 1, Warn = 2, Error = 3, Off = 4 };

std::string_view to_string(LogLevel level) noexcept;
std::optional<LogLevel> parse_log_level(std::string_view text) noexcept;

// Small id for the calling thread (1, 2, 3... in order of first log call).
std::uint32_t log_thread_id() noexcept;

struct LogRecord {
    std::int64_t wall_ns = 0;
    LogLevel level = LogLevel::Info;
    std::uint32_t thread_id = 0;
    std::uint16_t length = 0;
    bool truncated = false;
    char text[232];
};

// Asynchronous logger. The caller formats its message into a fixed-size record on its
// own stack (std::format_to_n, no heap allocation) and pushes the record into a
// lock-free queue. A background thread does all file and console I/O, so the trading
// and market-data threads never block on disk. If the queue is full the record is
// dropped and counted rather than stalling the caller.
class Logger {
public:
    static Logger& instance();

    // Opens `path` for appending (empty = console only) and starts the writer thread.
    // Records at `console_level` or above are also written to stderr.
    void start(const std::string& path, LogLevel level, LogLevel console_level);
    // Drains everything already queued, then stops the writer thread.
    void stop();

    void set_level(LogLevel level) noexcept {
        level_.store(static_cast<int>(level), std::memory_order_relaxed);
    }
    bool enabled(LogLevel level) const noexcept {
        return static_cast<int>(level) >= level_.load(std::memory_order_relaxed);
    }

    template <class... Args>
    void log(LogLevel level, std::format_string<Args...> fmt, Args&&... args) {
        if (!enabled(level)) return;
        LogRecord rec;
        rec.wall_ns = wall_ns();
        rec.level = level;
        rec.thread_id = log_thread_id();
        const auto res = std::format_to_n(rec.text, sizeof(rec.text), fmt, std::forward<Args>(args)...);
        const auto cap = static_cast<std::ptrdiff_t>(sizeof(rec.text));
        rec.length = static_cast<std::uint16_t>(std::min(res.size, cap));
        rec.truncated = res.size > cap;
        push(rec);
    }

    std::uint64_t dropped() const noexcept { return dropped_.load(std::memory_order_relaxed); }
    std::uint64_t written() const noexcept { return written_.load(std::memory_order_relaxed); }

private:
    Logger() = default;
    ~Logger();

    void push(const LogRecord& rec) noexcept;
    void run();
    void write(const LogRecord& rec);

    MpmcQueue<LogRecord> queue_{1 << 14};
    std::atomic<int> level_{static_cast<int>(LogLevel::Info)};
    std::atomic<bool> running_{false};
    std::atomic<bool> stop_requested_{false};
    std::mutex lifecycle_mu_;
    std::thread worker_;
    std::FILE* file_ = nullptr;
    LogLevel console_level_ = LogLevel::Warn;
    std::atomic<std::uint64_t> dropped_{0};
    std::atomic<std::uint64_t> written_{0};
};

}  // namespace de

#define LOG_DEBUG(...) ::de::Logger::instance().log(::de::LogLevel::Debug, __VA_ARGS__)
#define LOG_INFO(...) ::de::Logger::instance().log(::de::LogLevel::Info, __VA_ARGS__)
#define LOG_WARN(...) ::de::Logger::instance().log(::de::LogLevel::Warn, __VA_ARGS__)
#define LOG_ERROR(...) ::de::Logger::instance().log(::de::LogLevel::Error, __VA_ARGS__)
