#include <map>
#include <random>
#include <vector>

#include <boost/json.hpp>

#include "deribit/book_feed.hpp"
#include "deribit/order_book.hpp"
#include "server/book_message.hpp"
#include "test.hpp"

using namespace de;
using namespace de::deribit;
namespace json = boost::json;

namespace {
json::object obj(const char* text) { return json::parse(text).as_object(); }
}  // namespace

TEST(book_snapshot_sorts_and_skips_empty_levels) {
    OrderBook b("X");
    b.load_snapshot(5, {{99, 1}, {101, 2}, {100, 0}, {98, 3}}, {{103, 1}, {102, 4}, {104, 2}});
    CHECK(b.valid());
    CHECK_EQ(b.change_id(), 5);
    CHECK_EQ(b.depth(BookSide::Bid), 3u);  // the zero-amount level is skipped
    CHECK_EQ(b.best_bid()->price, 101.0);
    CHECK_EQ(b.best_ask()->price, 102.0);
    std::vector<PriceLevel> top;
    b.top(BookSide::Bid, 10, top);
    REQUIRE(top.size() == 3);
    CHECK_EQ(top[0].price, 101.0);
    CHECK_EQ(top[1].price, 99.0);
    CHECK_EQ(top[2].price, 98.0);
    top.clear();
    b.top(BookSide::Ask, 2, top);
    REQUIRE(top.size() == 2);
    CHECK_EQ(top[0].price, 102.0);
    CHECK_EQ(top[1].price, 103.0);
}

TEST(book_set_level_insert_update_delete) {
    OrderBook b("X");
    b.load_snapshot(1, {{100, 1}}, {{101, 1}});
    b.set_level(BookSide::Bid, 100.5, 7);  // new best bid
    CHECK_EQ(b.best_bid()->price, 100.5);
    b.set_level(BookSide::Bid, 100, 9);  // update
    CHECK_EQ(b.level(BookSide::Bid, 1).amount, 9.0);
    b.set_level(BookSide::Bid, 100.5, 0);  // delete
    CHECK_EQ(b.best_bid()->price, 100.0);
    b.set_level(BookSide::Bid, 42, 0);  // delete of a missing level is a no-op
    CHECK_EQ(b.depth(BookSide::Bid), 1u);
    b.set_level(BookSide::Ask, 100, 1);
    CHECK(b.crossed());
}

TEST(book_matches_reference_map_under_random_updates) {
    // Model check against std::map for 50k random operations.
    OrderBook b("X");
    b.load_snapshot(1, {}, {});
    std::map<double, double, std::greater<>> bids;
    std::map<double, double> asks;
    std::mt19937 rng(7);
    std::uniform_int_distribution<int> tick(1, 400), amt(0, 5);
    for (int i = 0; i < 50000; ++i) {
        const bool bid = rng() & 1;
        const double price = bid ? 1000 - tick(rng) * 0.5 : 1000 + tick(rng) * 0.5;
        const double amount = amt(rng) * 10.0;  // 0 = delete
        b.set_level(bid ? BookSide::Bid : BookSide::Ask, price, amount);
        if (bid) {
            if (amount > 0) bids[price] = amount;
            else bids.erase(price);
        } else {
            if (amount > 0) asks[price] = amount;
            else asks.erase(price);
        }
    }
    REQUIRE(b.depth(BookSide::Bid) == bids.size());
    REQUIRE(b.depth(BookSide::Ask) == asks.size());
    std::size_t i = 0;
    for (const auto& [p, a] : bids) {
        CHECK(b.level(BookSide::Bid, i).price == p && b.level(BookSide::Bid, i).amount == a);
        ++i;
    }
    i = 0;
    for (const auto& [p, a] : asks) {
        CHECK(b.level(BookSide::Ask, i).price == p && b.level(BookSide::Ask, i).amount == a);
        ++i;
    }
}

TEST(feed_snapshot_then_changes_in_sequence) {
    OrderBook b("BTC-PERPETUAL");
    auto r = apply_book_message(b, obj(R"({"type":"snapshot","timestamp":1000,"instrument_name":"BTC-PERPETUAL",
        "change_id":10,"bids":[["new",100.0,5.0],["new",99.5,1e4]],"asks":[["new",100.5,3],["new",101.0,2]]})"));
    CHECK(r == BookApply::Snapshot);
    CHECK_EQ(b.change_id(), 10);
    CHECK_EQ(b.timestamp_ms(), 1000);
    CHECK_EQ(b.level(BookSide::Bid, 1).amount, 10000.0);

    r = apply_book_message(b, obj(R"({"type":"change","timestamp":1100,"change_id":11,"prev_change_id":10,
        "bids":[["delete",100.0,0],["change",99.5,7]],"asks":[["new",100.25,1]]})"));
    CHECK(r == BookApply::Applied);
    CHECK_EQ(b.change_id(), 11);
    CHECK_EQ(b.best_bid()->price, 99.5);
    CHECK_EQ(b.best_bid()->amount, 7.0);
    CHECK_EQ(b.best_ask()->price, 100.25);
}

TEST(feed_detects_sequence_gap) {
    OrderBook b("X");
    apply_book_message(b, obj(R"({"type":"snapshot","change_id":10,"bids":[["new",1,1]],"asks":[["new",2,1]]})"));
    auto r = apply_book_message(b, obj(R"({"type":"change","change_id":13,"prev_change_id":12,"bids":[],"asks":[]})"));
    CHECK(r == BookApply::Gap);
    CHECK(!b.valid());
    // Further changes are rejected until a new snapshot arrives.
    r = apply_book_message(b, obj(R"({"type":"change","change_id":14,"prev_change_id":13,"bids":[],"asks":[]})"));
    CHECK(r == BookApply::Invalid);
    r = apply_book_message(b, obj(R"({"type":"snapshot","change_id":20,"bids":[["new",1,1]],"asks":[["new",2,1]]})"));
    CHECK(r == BookApply::Snapshot);
    CHECK(b.valid());
}

TEST(feed_change_before_snapshot_is_invalid) {
    OrderBook b("X");
    auto r = apply_book_message(b, obj(R"({"type":"change","change_id":2,"prev_change_id":1,"bids":[],"asks":[]})"));
    CHECK(r == BookApply::Invalid);
}

TEST(feed_crossed_book_is_invalid) {
    OrderBook b("X");
    apply_book_message(b, obj(R"({"type":"snapshot","change_id":1,"bids":[["new",100,1]],"asks":[["new",101,1]]})"));
    auto r = apply_book_message(b, obj(R"({"type":"change","change_id":2,"prev_change_id":1,"bids":[["new",102,1]],"asks":[]})"));
    CHECK(r == BookApply::Invalid);
    CHECK(!b.valid());
}

TEST(feed_malformed_message_throws) {
    OrderBook b("X");
    bool threw = false;
    try {
        apply_book_message(b, obj(R"({"type":"snapshot","change_id":1,"bids":"oops","asks":[]})"));
    } catch (const std::exception&) {
        threw = true;
    }
    CHECK(threw);
}

TEST(book_message_round_trips_as_json) {
    OrderBook b("BTC-PERPETUAL");
    b.load_snapshot(42, {{81651.0, 1057520}, {81649.5, 15000}, {81648.5, 19270}}, {{81651.5, 1020200}, {81652.5, 10}});
    b.set_timestamp_ms(1791497024149);
    const auto msg = build_book_message(b, 2, 123456789);
    const auto v = json::parse(msg).as_object();
    CHECK_EQ(std::string(v.at("type").as_string()), std::string("book"));
    CHECK_EQ(std::string(v.at("instrument").as_string()), std::string("BTC-PERPETUAL"));
    CHECK_EQ(v.at("change_id").to_number<std::int64_t>(), 42);
    CHECK_EQ(v.at("exch_ts").to_number<std::int64_t>(), 1791497024149);
    CHECK_EQ(v.at("recv_ns").to_number<std::int64_t>(), 123456789);
    const auto& bids = v.at("bids").as_array();
    REQUIRE(bids.size() == 2);  // depth limit
    CHECK_EQ(bids[0].as_array()[0].to_number<double>(), 81651.0);
    CHECK_EQ(bids[1].as_array()[0].to_number<double>(), 81649.5);
    CHECK_EQ(bids[0].as_array()[1].to_number<double>(), 1057520.0);
    CHECK_EQ(v.at("asks").as_array().size(), 2u);
}
