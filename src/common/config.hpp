#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_map>

#include "common/logger.hpp"

namespace de {

// Parses KEY=VALUE lines. Supports blank lines, '#' comments, an optional leading
// "export ", single or double quotes around the value, and trailing " # comment" on
// unquoted values. Later keys override earlier ones.
std::unordered_map<std::string, std::string> parse_dotenv(std::string_view text);

struct Config {
    // Deribit connection
    std::string deribit_host = "test.deribit.com";
    std::string deribit_port = "443";
    std::string deribit_path = "/ws/api/v2";
    std::string client_id;
    std::string client_secret;
    std::string ca_file;             // optional PEM bundle; default = system store
    std::string book_interval = "100ms";

    // Local market-data WebSocket server
    std::string server_bind = "127.0.0.1";
    std::uint16_t server_port = 8080;
    int server_threads = 2;
    int server_max_queue = 256;      // per-client cap on queued replies before it's dropped

    // Logging
    std::string log_file = "deribit_engine.log";
    LogLevel log_level = LogLevel::Info;
    LogLevel console_log_level = LogLevel::Warn;

    // Reliability
    int heartbeat_seconds = 10;
    int request_timeout_ms = 5000;
    double order_rate_per_sec = 5;   // matching-engine requests (buy/sell/edit/cancel)
    double order_burst = 10;
    double rpc_rate_per_sec = 20;    // everything else
    double rpc_burst = 50;

    bool has_credentials() const { return !client_id.empty() && !client_secret.empty(); }

    // Reads `env_path` if it exists, then lets real environment variables override it.
    // Throws std::runtime_error on a malformed value.
    static Config load(const std::string& env_path);
    static Config from_map(const std::unordered_map<std::string, std::string>& values);
};

}  // namespace de
