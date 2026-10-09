#include "deribit/book_feed.hpp"

#include <cstdint>
#include <vector>

#include <boost/json/array.hpp>
#include <boost/json/value.hpp>

namespace de::deribit {
namespace json = boost::json;

namespace {

struct Entry {
    bool remove;
    double price;
    double amount;
};

// Deribit sends [action, price, amount]; grouped books send [price, amount].
Entry parse_entry(const json::value& v) {
    const auto& a = v.as_array();
    if (a.size() == 3) {
        const auto& action = a[0].as_string();
        const bool remove = !action.empty() && action[0] == 'd';  // "delete"
        return {remove, a[1].to_number<double>(), a[2].to_number<double>()};
    }
    return {false, a.at(0).to_number<double>(), a.at(1).to_number<double>()};
}

std::vector<PriceLevel> parse_snapshot_side(const json::array& side) {
    std::vector<PriceLevel> out;
    out.reserve(side.size());
    for (const auto& v : side) {
        const auto e = parse_entry(v);
        if (!e.remove) out.push_back({e.price, e.amount});
    }
    return out;
}

void apply_side(OrderBook& book, BookSide side, const json::array& changes) {
    for (const auto& v : changes) {
        const auto e = parse_entry(v);
        book.set_level(side, e.price, e.remove ? 0.0 : e.amount);
    }
}

}  // namespace

std::string_view to_string(BookApply r) noexcept {
    switch (r) {
        case BookApply::Snapshot: return "snapshot";
        case BookApply::Applied: return "applied";
        case BookApply::Gap: return "gap";
        case BookApply::Invalid: return "invalid";
    }
    return "?";
}

BookApply apply_book_message(OrderBook& book, const json::object& data) {
    const auto& type = data.at("type").as_string();
    const auto change_id = data.at("change_id").to_number<std::int64_t>();
    const auto* ts = data.if_contains("timestamp");

    if (type == "snapshot") {
        book.load_snapshot(change_id, parse_snapshot_side(data.at("bids").as_array()),
                           parse_snapshot_side(data.at("asks").as_array()));
        if (ts) book.set_timestamp_ms(ts->to_number<std::int64_t>());
        if (book.crossed()) {
            book.invalidate();
            return BookApply::Invalid;
        }
        return BookApply::Snapshot;
    }

    if (!book.valid()) return BookApply::Invalid;
    const auto prev = data.at("prev_change_id").to_number<std::int64_t>();
    if (prev != book.change_id()) {
        book.invalidate();
        return BookApply::Gap;
    }
    apply_side(book, BookSide::Bid, data.at("bids").as_array());
    apply_side(book, BookSide::Ask, data.at("asks").as_array());
    book.set_change_id(change_id);
    if (ts) book.set_timestamp_ms(ts->to_number<std::int64_t>());
    if (book.crossed()) {
        book.invalidate();
        return BookApply::Invalid;
    }
    return BookApply::Applied;
}

}  // namespace de::deribit
