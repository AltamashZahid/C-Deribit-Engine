#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <vector>

#include "common/config.hpp"
#include "common/latency_histogram.hpp"
#include "deribit/client.hpp"
#include "deribit/order_book.hpp"
#include "server/md_server.hpp"

namespace de {

struct TopOfBook {
    std::vector<PriceLevel> bids, asks;
    std::int64_t change_id = 0;
    std::int64_t exch_ts_ms = 0;
};

// State for "bench loop": on the next book update the I/O thread fires a pre-built order
// and the ack time is measured from the moment that update arrived.
struct LoopBench {
    std::atomic<bool> armed{false};
    std::string instrument;
    deribit::OrderRequest order;
    std::mutex mu;
    std::shared_ptr<std::promise<deribit::RpcResult>> current;
    LatencyHistogram tick_to_ack;
};

struct App {
    Config cfg;
    std::size_t depth = 10;
    std::unique_ptr<deribit::Client> client;  // null in --synthetic mode
    std::unique_ptr<MarketDataServer> server;

    // Book subscriptions are reference-counted: the CLI and every server client can
    // each hold one, and Deribit is only unsubscribed when the last one goes away.
    std::mutex sub_mu;
    std::map<std::string, int> sub_refs;
    std::set<std::string> cli_subs;
    void acquire_book(const std::string& instrument);
    void release_book(const std::string& instrument);

    std::mutex top_mu;
    std::map<std::string, TopOfBook> tops;

    LoopBench loop;

    // Runs on the Deribit I/O thread for every applied book update.
    void on_book(const OrderBook& book, std::int64_t recv_ns);
    void print_stats();
};

// Interactive command loop on stdin. Returns when the user quits or stdin closes.
void run_cli(App& app);

}  // namespace de
