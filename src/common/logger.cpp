#include "common/logger.hpp"

#include <cctype>
#include <chrono>
#include <ctime>

namespace de {

std::string_view to_string(LogLevel level) noexcept {
    switch (level) {
        case LogLevel::Debug: return "DEBUG";
        case LogLevel::Info: return "INFO";
        case LogLevel::Warn: return "WARN";
        case LogLevel::Error: return "ERROR";
        case LogLevel::Off: return "OFF";
    }
    return "?";
}

std::optional<LogLevel> parse_log_level(std::string_view text) noexcept {
    std::string lower;
    for (char c : text) lower.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
    if (lower == "debug") return LogLevel::Debug;
    if (lower == "info") return LogLevel::Info;
    if (lower == "warn" || lower == "warning") return LogLevel::Warn;
    if (lower == "error") return LogLevel::Error;
    if (lower == "off" || lower == "none") return LogLevel::Off;
    return std::nullopt;
}

std::uint32_t log_thread_id() noexcept {
    static std::atomic<std::uint32_t> next{1};
    thread_local const std::uint32_t id = next.fetch_add(1, std::memory_order_relaxed);
    return id;
}

Logger& Logger::instance() {
    static Logger logger;
    return logger;
}

Logger::~Logger() { stop(); }

void Logger::start(const std::string& path, LogLevel level, LogLevel console_level) {
    std::lock_guard lock(lifecycle_mu_);
    if (running_.load()) return;
    set_level(level);
    console_level_ = console_level;
    if (!path.empty()) {
        file_ = std::fopen(path.c_str(), "a");
        if (!file_) std::fprintf(stderr, "logger: cannot open %s, logging to console only\n", path.c_str());
    }
    stop_requested_.store(false);
    running_.store(true, std::memory_order_release);
    worker_ = std::thread([this] { run(); });
}

void Logger::stop() {
    std::lock_guard lock(lifecycle_mu_);
    if (!running_.load()) return;
    // From here on, new records fall back to synchronous stderr (warnings and errors only).
    running_.store(false, std::memory_order_release);
    stop_requested_.store(true, std::memory_order_release);
    worker_.join();
    if (file_) {
        std::fclose(file_);
        file_ = nullptr;
    }
}

void Logger::push(const LogRecord& rec) noexcept {
    if (!running_.load(std::memory_order_acquire)) {
        if (rec.level >= LogLevel::Warn) {
            std::fprintf(stderr, "%s %.*s\n", to_string(rec.level).data(), static_cast<int>(rec.length), rec.text);
        }
        return;
    }
    if (!queue_.try_push(rec)) dropped_.fetch_add(1, std::memory_order_relaxed);
}

void Logger::run() {
    LogRecord rec;
    for (;;) {
        // Read the flag before draining so anything pushed before stop() is written.
        const bool stopping = stop_requested_.load(std::memory_order_acquire);
        std::size_t n = 0;
        while (queue_.try_pop(rec)) {
            write(rec);
            ++n;
        }
        if (n && file_) std::fflush(file_);
        if (stopping) break;
        if (n == 0) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

void Logger::write(const LogRecord& rec) {
    const std::time_t secs = static_cast<std::time_t>(rec.wall_ns / 1'000'000'000);
    const auto micros = (rec.wall_ns % 1'000'000'000) / 1'000;
    std::tm tm{};
#ifdef _WIN32
    gmtime_s(&tm, &secs);
#else
    gmtime_r(&secs, &tm);
#endif
    char line[320];
    const auto res = std::format_to_n(line, sizeof(line), "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}Z {:<5} [t{}] {}{}\n",
                                      tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min,
                                      tm.tm_sec, micros, to_string(rec.level), rec.thread_id,
                                      std::string_view(rec.text, rec.length), rec.truncated ? "..." : "");
    auto len = static_cast<std::size_t>(std::min<std::ptrdiff_t>(res.size, sizeof(line)));
    if (static_cast<std::size_t>(res.size) > sizeof(line)) line[len - 1] = '\n';
    if (file_) std::fwrite(line, 1, len, file_);
    if (rec.level >= console_level_) std::fwrite(line, 1, len, stderr);
    written_.fetch_add(1, std::memory_order_relaxed);
}

}  // namespace de
