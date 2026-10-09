#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <format>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include <boost/json.hpp>

#include "app/app.hpp"
#include "common/logger.hpp"
#include "common/time.hpp"
#include "server/book_message.hpp"

namespace de {

namespace json = boost::json;
using deribit::RpcResult;

// ---- App ------------------------------------------------------------------------------------

void App::acquire_book(const std::string& instrument) {
    std::lock_guard lock(sub_mu);
    if (sub_refs[instrument]++ == 0 && client) client->subscribe_book(instrument);
}

void App::release_book(const std::string& instrument) {
    std::lock_guard lock(sub_mu);
    auto it = sub_refs.find(instrument);
    if (it == sub_refs.end()) return;
    if (--it->second <= 0) {
        sub_refs.erase(it);
        if (client) client->unsubscribe_book(instrument);
    }
}

void App::on_book(const OrderBook& book, std::int64_t recv_ns) {
    // 1. Strategy hook (bench loop): fire the order before doing anything else.
    if (loop.armed.load(std::memory_order_relaxed) && book.instrument() == loop.instrument &&
        loop.armed.exchange(false)) {
        std::shared_ptr<std::promise<RpcResult>> promise;
        {
            std::lock_guard lock(loop.mu);
            promise = loop.current;
        }
        client->place_order(
            loop.order,
            [this, promise, recv_ns](RpcResult r) {
                loop.tick_to_ack.record(now_ns() - recv_ns);
                if (promise) promise->set_value(std::move(r));
            },
            recv_ns);
    }

    // 2. Fan out to local WebSocket clients (serialised once, shared by all of them).
    if (server->subscriber_count(book.instrument()) > 0) {
        auto msg = std::make_shared<const std::string>(build_book_message(book, depth, recv_ns));
        server->publish(book.instrument(), std::move(msg), recv_ns);
    }

    // 3. Keep a small copy of the top of book for the CLI's "top" command.
    TopOfBook t;
    t.change_id = book.change_id();
    t.exch_ts_ms = book.timestamp_ms();
    book.top(BookSide::Bid, 10, t.bids);
    book.top(BookSide::Ask, 10, t.asks);
    std::lock_guard lock(top_mu);
    tops[book.instrument()] = std::move(t);
}

void App::print_stats() {
    std::puts("---- latency ----");
    if (client) {
        std::puts(client->md_latency().summary("md parse+apply").c_str());
        std::puts(client->tick_to_send_latency().summary("tick-to-send").c_str());
        std::puts(client->socket_write_latency().summary("socket write").c_str());
        std::puts(client->order_latency().summary("order rtt").c_str());
        std::puts(client->rpc_latency().summary("rpc rtt").c_str());
    }
    std::puts(server->fanout_latency().summary("ws fan-out").c_str());
    if (loop.tick_to_ack.count()) std::puts(loop.tick_to_ack.summary("tick-to-ack (loop)").c_str());
    std::puts("---- counters ----");
    if (client) {
        const auto& s = client->stats();
        std::printf("deribit: state=%s auth=%s msgs_in=%llu bytes_in=%llu book_updates=%llu resyncs=%llu connects=%llu "
                    "disconnects=%llu timeouts=%llu throttled=%llu rate_limit_retries=%llu\n",
                    std::string(deribit::to_string(client->state())).c_str(), client->authenticated() ? "yes" : "no",
                    static_cast<unsigned long long>(s.messages_in.load()), static_cast<unsigned long long>(s.bytes_in.load()),
                    static_cast<unsigned long long>(s.book_updates.load()), static_cast<unsigned long long>(s.book_resyncs.load()),
                    static_cast<unsigned long long>(s.connects.load()), static_cast<unsigned long long>(s.disconnects.load()),
                    static_cast<unsigned long long>(s.timeouts.load()), static_cast<unsigned long long>(s.throttled.load()),
                    static_cast<unsigned long long>(s.rate_limit_retries.load()));
    }
    const auto& ss = server->stats();
    std::printf("server: sessions_active=%lld sessions_total=%llu publishes=%llu msgs_out=%llu bytes_out=%llu conflated=%llu "
                "slow_drops=%llu\n",
                static_cast<long long>(ss.sessions_active.load()), static_cast<unsigned long long>(ss.sessions_opened.load()),
                static_cast<unsigned long long>(ss.publishes.load()), static_cast<unsigned long long>(ss.messages_out.load()),
                static_cast<unsigned long long>(ss.bytes_out.load()), static_cast<unsigned long long>(ss.conflated.load()),
                static_cast<unsigned long long>(ss.slow_consumer_drops.load()));
    std::printf("logger: written=%llu dropped=%llu\n", static_cast<unsigned long long>(Logger::instance().written()),
                static_cast<unsigned long long>(Logger::instance().dropped()));
}

// ---- formatting helpers -------------------------------------------------------------------

namespace {

// Boost.JSON prints doubles as "8.1899E4"; show them the way a trader expects ("81899").
std::string text(const json::value& v) {
    if (v.is_double()) return std::format("{}", v.get_double());
    if (v.is_string()) return std::string(v.get_string());
    return json::serialize(v);
}

void pretty(std::string& out, const json::value& v, int indent) {
    const std::string pad(static_cast<std::size_t>(indent) * 2, ' ');
    const std::string pad_in(static_cast<std::size_t>(indent + 1) * 2, ' ');
    if (v.is_object()) {
        const auto& o = v.get_object();
        if (o.empty()) {
            out += "{}";
            return;
        }
        out += "{\n";
        bool first = true;
        for (const auto& kv : o) {
            if (!first) out += ",\n";
            first = false;
            out += pad_in + json::serialize(json::value(kv.key())) + ": ";
            pretty(out, kv.value(), indent + 1);
        }
        out += "\n" + pad + "}";
    } else if (v.is_array()) {
        const auto& a = v.get_array();
        const bool scalar = std::all_of(a.begin(), a.end(), [](const json::value& e) { return !e.is_structured(); });
        if (a.empty() || scalar) {
            out += "[";
            for (std::size_t i = 0; i < a.size(); ++i) {
                if (i) out += ",";
                out += a[i].is_string() ? json::serialize(a[i]) : text(a[i]);
            }
            out += "]";
            return;
        }
        out += "[\n";
        for (std::size_t i = 0; i < a.size(); ++i) {
            if (i) out += ",\n";
            out += pad_in;
            pretty(out, a[i], indent + 1);
        }
        out += "\n" + pad + "]";
    } else {
        out += v.is_string() ? json::serialize(v) : text(v);
    }
}

void print_json(const json::value& v) {
    std::string s;
    pretty(s, v, 0);
    std::puts(s.c_str());
}

std::string field(const json::object& o, std::string_view key) {
    const auto* v = o.if_contains(key);
    if (!v || v->is_null()) return "-";
    return text(*v);
}

bool report_error(const RpcResult& r) {
    if (r.ok()) return false;
    std::printf("error %d: %s\n", r.error->code, r.error->message.c_str());
    return true;
}

void print_latency(const RpcResult& r) { std::printf("(round trip %s)\n", format_ns(r.latency_ns).c_str()); }

void print_order(const json::object& o) {
    std::printf("  %-14s %-12s %-5s %-24s price=%-10s amount=%-8s filled=%-8s avg=%-10s label=%s\n",
                field(o, "order_id").c_str(), field(o, "order_state").c_str(), field(o, "direction").c_str(),
                field(o, "instrument_name").c_str(), field(o, "price").c_str(), field(o, "amount").c_str(),
                field(o, "filled_amount").c_str(), field(o, "average_price").c_str(), field(o, "label").c_str());
}

std::vector<std::string> split(const std::string& line) {
    std::istringstream in(line);
    std::vector<std::string> out;
    for (std::string w; in >> w;) out.push_back(w);
    return out;
}

double to_double(const std::string& s) {
    std::size_t used = 0;
    const double d = std::stod(s, &used);
    if (used != s.size()) throw std::invalid_argument("not a number: " + s);
    return d;
}

constexpr const char* kHelp = R"(commands:
  instruments <currency> [kind]            list instruments (kind: future, option, spot, ...)
  book <instrument> [depth]                order book snapshot from Deribit
  buy  <instrument> <amount> <price|market> [post_only] [label=NAME]
  sell <instrument> <amount> <price|market> [post_only] [label=NAME]
  modify <order_id> <amount> <price>       edit an open order
  cancel <order_id>                        cancel one order
  cancel_all [instrument]                  cancel all orders (optionally one instrument)
  orders [instrument]                      open orders
  positions <currency> [kind]              current positions
  account <currency>                       account summary
  sub <instrument> / unsub <instrument>    stream an instrument's book into the engine
  top <instrument>                         locally maintained top of book
  call <method> [json-params]              raw JSON-RPC call, e.g. call public/ticker {"instrument_name":"BTC-PERPETUAL"}
  bench orders <instrument> [count]        place + cancel post-only orders, report round-trip latency
  bench loop <instrument> [count]          on a book update: send an order, measure tick-to-send and tick-to-ack
  stats                                    latency histograms and counters
  reset                                    clear latency histograms
  log <debug|info|warn|error>              change log level
  help, quit)";

// ---- benchmarks -------------------------------------------------------------------------

struct BenchSetup {
    double tick = 0;
    double amount = 0;
    double price = 0;  // passive buy price that won't fill but is inside Deribit's price band
};

bool prepare_bench(App& app, const std::string& inst, BenchSetup& s) {
    auto info = app.client->get_instrument(inst).get();
    if (report_error(info)) return false;
    auto book = app.client->get_order_book(inst, 1).get();
    if (report_error(book)) return false;
    const auto& i = info.result.as_object();
    const auto& b = book.result.as_object();
    s.tick = i.at("tick_size").to_number<double>();
    s.amount = i.at("min_trade_amount").to_number<double>();
    const auto* best_bid = b.if_contains("best_bid_price");
    const auto* min_price = b.if_contains("min_price");
    if (!best_bid || best_bid->is_null() || best_bid->to_number<double>() <= 0) {
        std::puts("instrument has no bids; pick a liquid instrument such as BTC-PERPETUAL");
        return false;
    }
    const double bid = best_bid->to_number<double>();
    double target = bid * 0.99;  // ~1% below the market: rests on the book, won't fill
    if (min_price && !min_price->is_null()) target = std::max(target, min_price->to_number<double>() + s.tick);
    s.price = std::floor(target / s.tick) * s.tick;
    if (s.price >= bid) s.price = bid - s.tick;
    std::printf("bench setup: %s tick=%g amount=%g passive buy price=%g (best bid %g)\n", inst.c_str(), s.tick, s.amount,
                s.price, bid);
    return true;
}

void bench_orders(App& app, const std::string& inst, int count) {
    BenchSetup s;
    if (!prepare_bench(app, inst, s)) return;
    LatencyHistogram place, cancel;
    for (int n = 0; n < count; ++n) {
        deribit::OrderRequest o;
        o.instrument = inst;
        o.side = deribit::OrderSide::Buy;
        o.amount = s.amount;
        o.price = s.price;
        o.post_only = true;
        o.label = "de-bench";
        auto r = app.client->place_order(o).get();
        if (report_error(r)) break;
        place.record(r.latency_ns);
        const auto id = std::string(r.result.at("order").at("order_id").as_string());
        auto c = app.client->cancel(id).get();
        if (report_error(c)) break;
        cancel.record(c.latency_ns);
    }
    auto cleanup = app.client->cancel_by_label("de-bench").get();
    report_error(cleanup);
    std::puts(place.summary("place (buy) rtt").c_str());
    std::puts(cancel.summary("cancel rtt").c_str());
}

void bench_loop(App& app, const std::string& inst, int count) {
    BenchSetup s;
    if (!prepare_bench(app, inst, s)) return;
    app.acquire_book(inst);
    app.client->tick_to_send_latency().reset();
    app.client->socket_write_latency().reset();
    app.loop.tick_to_ack.reset();
    app.loop.instrument = inst;
    app.loop.order.instrument = inst;
    app.loop.order.side = deribit::OrderSide::Buy;
    app.loop.order.amount = s.amount;
    app.loop.order.price = s.price;
    app.loop.order.post_only = true;
    app.loop.order.label = "de-loop";
    int done = 0;
    for (int n = 0; n < count; ++n) {
        auto promise = std::make_shared<std::promise<RpcResult>>();
        auto future = promise->get_future();
        {
            std::lock_guard lock(app.loop.mu);
            app.loop.current = promise;
        }
        app.loop.armed = true;  // the next book update on the I/O thread fires the order
        if (future.wait_for(std::chrono::seconds(15)) != std::future_status::ready) {
            app.loop.armed = false;
            std::puts("no book update / ack within 15 s, stopping");
            break;
        }
        auto r = future.get();
        if (report_error(r)) break;
        ++done;
        const auto id = std::string(r.result.at("order").at("order_id").as_string());
        report_error(app.client->cancel(id).get());
    }
    {
        std::lock_guard lock(app.loop.mu);
        app.loop.current.reset();
    }
    report_error(app.client->cancel_by_label("de-loop").get());
    app.release_book(inst);
    std::printf("%d/%d iterations\n", done, count);
    std::puts(app.client->tick_to_send_latency().summary("tick-to-send (engine)").c_str());
    std::puts(app.client->socket_write_latency().summary("socket write (OS)").c_str());
    std::puts(app.loop.tick_to_ack.summary("tick-to-ack").c_str());
}

// ---- command dispatch -------------------------------------------------------------------

bool needs_client(App& app) {
    if (app.client) return true;
    std::puts("not available in --synthetic mode");
    return false;
}

void handle(App& app, const std::vector<std::string>& w, const std::string& line) {
    const auto& cmd = w[0];
    const auto arg = [&](std::size_t i, const std::string& def = {}) { return i < w.size() ? w[i] : def; };

    if (cmd == "help") {
        std::puts(kHelp);
    } else if (cmd == "stats") {
        app.print_stats();
    } else if (cmd == "reset") {
        if (app.client) {
            app.client->md_latency().reset();
            app.client->order_latency().reset();
            app.client->rpc_latency().reset();
            app.client->tick_to_send_latency().reset();
            app.client->socket_write_latency().reset();
        }
        app.server->fanout_latency().reset();
        app.loop.tick_to_ack.reset();
        std::puts("histograms cleared");
    } else if (cmd == "log" && w.size() >= 2) {
        if (auto l = parse_log_level(w[1])) {
            Logger::instance().set_level(*l);
            std::printf("log level %s\n", std::string(to_string(*l)).c_str());
        } else {
            std::puts("unknown level");
        }
    } else if (!needs_client(app)) {
        return;
    } else if (cmd == "instruments" && w.size() >= 2) {
        auto r = app.client->get_instruments(w[1], arg(2)).get();
        if (report_error(r)) return;
        for (const auto& v : r.result.as_array()) {
            const auto& o = v.as_object();
            std::printf("  %-28s kind=%-7s tick=%-8s min_amount=%-6s contract=%s\n", field(o, "instrument_name").c_str(),
                        field(o, "kind").c_str(), field(o, "tick_size").c_str(), field(o, "min_trade_amount").c_str(),
                        field(o, "contract_size").c_str());
        }
        std::printf("%zu instruments ", r.result.as_array().size());
        print_latency(r);
    } else if (cmd == "book" && w.size() >= 2) {
        const int depth = std::stoi(arg(2, "10"));
        auto r = app.client->get_order_book(w[1], depth).get();
        if (report_error(r)) return;
        const auto& o = r.result.as_object();
        std::printf("%s  last=%s mark=%s index=%s state=%s\n", w[1].c_str(), field(o, "last_price").c_str(),
                    field(o, "mark_price").c_str(), field(o, "index_price").c_str(), field(o, "state").c_str());
        const auto& bids = o.at("bids").as_array();
        const auto& asks = o.at("asks").as_array();
        std::printf("  %24s | %-24s\n", "bid (amount @ price)", "ask (price x amount)");
        for (std::size_t i = 0; i < std::max(bids.size(), asks.size()); ++i) {
            std::string l = i < bids.size() ? text(bids[i].as_array()[1]) + " @ " + text(bids[i].as_array()[0]) : "";
            std::string rr = i < asks.size() ? text(asks[i].as_array()[0]) + " x " + text(asks[i].as_array()[1]) : "";
            std::printf("  %24s | %-24s\n", l.c_str(), rr.c_str());
        }
        print_latency(r);
    } else if ((cmd == "buy" || cmd == "sell") && w.size() >= 4) {
        deribit::OrderRequest o;
        o.instrument = w[1];
        o.side = cmd == "buy" ? deribit::OrderSide::Buy : deribit::OrderSide::Sell;
        o.amount = to_double(w[2]);
        if (w[3] == "market") o.type = "market";
        else o.price = to_double(w[3]);
        for (std::size_t i = 4; i < w.size(); ++i) {
            if (w[i] == "post_only") o.post_only = true;
            else if (w[i].starts_with("label=")) o.label = w[i].substr(6);
            else return (void)std::printf("unknown option %s\n", w[i].c_str());
        }
        auto r = app.client->place_order(o).get();
        if (report_error(r)) return;
        const auto& res = r.result.as_object();
        print_order(res.at("order").as_object());
        if (const auto* trades = res.if_contains("trades"); trades && trades->is_array() && !trades->get_array().empty())
            std::printf("  %zu trade(s)\n", trades->get_array().size());
        print_latency(r);
    } else if (cmd == "modify" && w.size() >= 4) {
        auto r = app.client->edit(w[1], to_double(w[2]), to_double(w[3])).get();
        if (report_error(r)) return;
        print_order(r.result.at("order").as_object());
        print_latency(r);
    } else if (cmd == "cancel" && w.size() >= 2) {
        auto r = app.client->cancel(w[1]).get();
        if (report_error(r)) return;
        print_order(r.result.as_object());
        print_latency(r);
    } else if (cmd == "cancel_all") {
        auto r = app.client->cancel_all(arg(1)).get();
        if (report_error(r)) return;
        std::printf("cancelled %s order(s) ", json::serialize(r.result).c_str());
        print_latency(r);
    } else if (cmd == "orders") {
        auto r = app.client->get_open_orders(arg(1)).get();
        if (report_error(r)) return;
        for (const auto& v : r.result.as_array()) print_order(v.as_object());
        std::printf("%zu open order(s) ", r.result.as_array().size());
        print_latency(r);
    } else if (cmd == "positions" && w.size() >= 2) {
        auto r = app.client->get_positions(w[1], arg(2)).get();
        if (report_error(r)) return;
        for (const auto& v : r.result.as_array()) {
            const auto& p = v.as_object();
            std::printf("  %-28s size=%-10s dir=%-5s avg=%-10s mark=%-10s upnl=%-12s total_pnl=%s\n",
                        field(p, "instrument_name").c_str(), field(p, "size").c_str(), field(p, "direction").c_str(),
                        field(p, "average_price").c_str(), field(p, "mark_price").c_str(),
                        field(p, "floating_profit_loss").c_str(), field(p, "total_profit_loss").c_str());
        }
        std::printf("%zu position(s) ", r.result.as_array().size());
        print_latency(r);
    } else if (cmd == "account" && w.size() >= 2) {
        auto r = app.client->get_account_summary(w[1]).get();
        if (report_error(r)) return;
        const auto& a = r.result.as_object();
        std::printf("  %s equity=%s balance=%s available=%s margin_balance=%s\n", field(a, "currency").c_str(),
                    field(a, "equity").c_str(), field(a, "balance").c_str(), field(a, "available_funds").c_str(),
                    field(a, "margin_balance").c_str());
        print_latency(r);
    } else if (cmd == "sub" && w.size() >= 2) {
        bool added;
        {
            std::lock_guard lock(app.sub_mu);
            added = app.cli_subs.insert(w[1]).second;
        }
        if (added) app.acquire_book(w[1]);
        std::printf("streaming %s\n", app.client->book_channel(w[1]).c_str());
    } else if (cmd == "unsub" && w.size() >= 2) {
        bool removed;
        {
            std::lock_guard lock(app.sub_mu);
            removed = app.cli_subs.erase(w[1]) > 0;
        }
        if (removed) app.release_book(w[1]);
        std::printf("%s\n", removed ? "unsubscribed" : "not subscribed from the CLI");
    } else if (cmd == "top" && w.size() >= 2) {
        TopOfBook t;
        {
            std::lock_guard lock(app.top_mu);
            auto it = app.tops.find(w[1]);
            if (it == app.tops.end()) return (void)std::puts("no data yet (use: sub <instrument>)");
            t = it->second;
        }
        std::printf("%s change_id=%lld\n", w[1].c_str(), static_cast<long long>(t.change_id));
        for (std::size_t i = 0; i < std::max(t.bids.size(), t.asks.size()); ++i) {
            std::string l = i < t.bids.size() ? std::format("{} @ {}", t.bids[i].amount, t.bids[i].price) : "";
            std::string r = i < t.asks.size() ? std::format("{} x {}", t.asks[i].price, t.asks[i].amount) : "";
            std::printf("  %24s | %-24s\n", l.c_str(), r.c_str());
        }
    } else if (cmd == "call" && w.size() >= 2) {
        json::object params;
        const auto pos = line.find(w[1]) + w[1].size();
        const auto rest = line.substr(pos);
        if (rest.find_first_not_of(" \t") != std::string::npos) {
            boost::system::error_code ec;
            auto v = json::parse(rest, ec);
            if (ec || !v.is_object()) return (void)std::puts("params must be a JSON object");
            params = std::move(v.get_object());
        }
        auto r = app.client->call(w[1], std::move(params)).get();
        if (report_error(r)) return;
        print_json(r.result);
        print_latency(r);
    } else if (cmd == "bench" && w.size() >= 3 && (w[1] == "orders" || w[1] == "loop")) {
        if (!app.client->has_credentials()) return (void)std::puts("bench needs DERIBIT_CLIENT_ID / DERIBIT_CLIENT_SECRET");
        const int count = std::clamp(std::stoi(arg(3, "20")), 1, 1000);
        if (w[1] == "orders") bench_orders(app, w[2], count);
        else bench_loop(app, w[2], count);
    } else {
        std::puts("unknown command or missing arguments (type: help)");
    }
}

}  // namespace

void run_cli(App& app) {
    std::puts("type 'help' for commands");
    std::string line;
    for (;;) {
        std::fputs("> ", stdout);
        std::fflush(stdout);
        if (!std::getline(std::cin, line)) break;
        const auto words = split(line);
        if (words.empty()) continue;
        if (words[0] == "quit" || words[0] == "exit") break;
        try {
            handle(app, words, line);
        } catch (const std::exception& e) {
            std::printf("error: %s\n", e.what());
        }
    }
}

}  // namespace de
