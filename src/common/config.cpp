#include "common/config.hpp"

#include <charconv>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <stdexcept>

namespace de {
namespace {

std::string_view trim(std::string_view s) {
    const auto ws = " \t\r\n";
    const auto b = s.find_first_not_of(ws);
    if (b == std::string_view::npos) return {};
    const auto e = s.find_last_not_of(ws);
    return s.substr(b, e - b + 1);
}

template <class T>
T parse_number(const std::string& key, const std::string& value) {
    T out{};
    const auto* first = value.data();
    const auto* last = value.data() + value.size();
    const auto [ptr, ec] = std::from_chars(first, last, out);
    if (ec != std::errc{} || ptr != last) throw std::runtime_error("invalid value for " + key + ": '" + value + "'");
    return out;
}

const char* kKeys[] = {
    "DERIBIT_HOST", "DERIBIT_PORT", "DERIBIT_PATH", "DERIBIT_CLIENT_ID", "DERIBIT_CLIENT_SECRET",
    "DERIBIT_CA_FILE", "DERIBIT_BOOK_INTERVAL", "MD_SERVER_BIND", "MD_SERVER_PORT", "MD_SERVER_THREADS",
    "MD_SERVER_MAX_QUEUE", "LOG_FILE", "LOG_LEVEL", "CONSOLE_LOG_LEVEL", "HEARTBEAT_SECONDS",
    "REQUEST_TIMEOUT_MS", "ORDER_RATE_PER_SEC", "ORDER_BURST", "RPC_RATE_PER_SEC", "RPC_BURST",
};

}  // namespace

std::unordered_map<std::string, std::string> parse_dotenv(std::string_view text) {
    std::unordered_map<std::string, std::string> out;
    std::size_t pos = 0;
    while (pos <= text.size()) {
        auto nl = text.find('\n', pos);
        if (nl == std::string_view::npos) nl = text.size();
        auto line = trim(text.substr(pos, nl - pos));
        pos = nl + 1;
        if (line.empty() || line.front() == '#') continue;
        if (line.starts_with("export ")) line = trim(line.substr(7));
        const auto eq = line.find('=');
        if (eq == std::string_view::npos) continue;
        const auto key = trim(line.substr(0, eq));
        auto value = trim(line.substr(eq + 1));
        if (key.empty()) continue;
        if (value.size() >= 2 && (value.front() == '"' || value.front() == '\'') && value.back() == value.front()) {
            value = value.substr(1, value.size() - 2);
        } else if (const auto hash = value.find(" #"); hash != std::string_view::npos) {
            value = trim(value.substr(0, hash));
        }
        out[std::string(key)] = std::string(value);
    }
    return out;
}

Config Config::from_map(const std::unordered_map<std::string, std::string>& values) {
    Config c;
    auto get = [&](const char* key, auto apply) {
        if (auto it = values.find(key); it != values.end() && !it->second.empty()) apply(it->second);
    };
    auto level = [](const std::string& key, const std::string& v) {
        auto l = parse_log_level(v);
        if (!l) throw std::runtime_error("invalid value for " + key + ": '" + v + "'");
        return *l;
    };
    get("DERIBIT_HOST", [&](const std::string& v) { c.deribit_host = v; });
    get("DERIBIT_PORT", [&](const std::string& v) { c.deribit_port = v; });
    get("DERIBIT_PATH", [&](const std::string& v) { c.deribit_path = v; });
    get("DERIBIT_CLIENT_ID", [&](const std::string& v) { c.client_id = v; });
    get("DERIBIT_CLIENT_SECRET", [&](const std::string& v) { c.client_secret = v; });
    get("DERIBIT_CA_FILE", [&](const std::string& v) { c.ca_file = v; });
    get("DERIBIT_BOOK_INTERVAL", [&](const std::string& v) { c.book_interval = v; });
    get("MD_SERVER_BIND", [&](const std::string& v) { c.server_bind = v; });
    get("MD_SERVER_PORT", [&](const std::string& v) { c.server_port = parse_number<std::uint16_t>("MD_SERVER_PORT", v); });
    get("MD_SERVER_THREADS", [&](const std::string& v) { c.server_threads = parse_number<int>("MD_SERVER_THREADS", v); });
    get("MD_SERVER_MAX_QUEUE", [&](const std::string& v) { c.server_max_queue = parse_number<int>("MD_SERVER_MAX_QUEUE", v); });
    get("LOG_FILE", [&](const std::string& v) { c.log_file = v; });
    get("LOG_LEVEL", [&](const std::string& v) { c.log_level = level("LOG_LEVEL", v); });
    get("CONSOLE_LOG_LEVEL", [&](const std::string& v) { c.console_log_level = level("CONSOLE_LOG_LEVEL", v); });
    get("HEARTBEAT_SECONDS", [&](const std::string& v) { c.heartbeat_seconds = parse_number<int>("HEARTBEAT_SECONDS", v); });
    get("REQUEST_TIMEOUT_MS", [&](const std::string& v) { c.request_timeout_ms = parse_number<int>("REQUEST_TIMEOUT_MS", v); });
    get("ORDER_RATE_PER_SEC", [&](const std::string& v) { c.order_rate_per_sec = parse_number<double>("ORDER_RATE_PER_SEC", v); });
    get("ORDER_BURST", [&](const std::string& v) { c.order_burst = parse_number<double>("ORDER_BURST", v); });
    get("RPC_RATE_PER_SEC", [&](const std::string& v) { c.rpc_rate_per_sec = parse_number<double>("RPC_RATE_PER_SEC", v); });
    get("RPC_BURST", [&](const std::string& v) { c.rpc_burst = parse_number<double>("RPC_BURST", v); });

    if (c.server_threads < 1) throw std::runtime_error("MD_SERVER_THREADS must be >= 1");
    if (c.server_max_queue < 1) throw std::runtime_error("MD_SERVER_MAX_QUEUE must be >= 1");
    if (c.heartbeat_seconds < 10) c.heartbeat_seconds = 10;  // Deribit's minimum
    if (c.request_timeout_ms < 100) throw std::runtime_error("REQUEST_TIMEOUT_MS must be >= 100");
    return c;
}

Config Config::load(const std::string& env_path) {
    std::unordered_map<std::string, std::string> values;
    if (std::ifstream in(env_path); in) {
        std::stringstream ss;
        ss << in.rdbuf();
        values = parse_dotenv(ss.str());
    }
    for (const char* key : kKeys) {
        if (const char* v = std::getenv(key); v && *v) values[key] = v;
    }
    return from_map(values);
}

}  // namespace de
