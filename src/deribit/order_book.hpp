#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace de {

struct PriceLevel {
    double price = 0;
    double amount = 0;
    bool operator==(const PriceLevel&) const = default;
};

enum class BookSide { Bid, Ask };

// Price-level order book for one instrument, maintained from Deribit's book stream.
//
// Each side is a flat sorted vector ordered from the worst price to the best price, so
// the best bid/ask is at back(). Almost all updates happen near the top of the book,
// which means an insert or erase only shifts the few elements behind it, and lookups
// are a binary search over contiguous memory (cache-friendly, no per-level allocation,
// unlike std::map).
//
// Prices are compared exactly: Deribit sends prices that are exact multiples of the
// tick size, and the same decimal text always parses to the same double.
class OrderBook {
public:
    explicit OrderBook(std::string instrument = {}) : instrument_(std::move(instrument)) {}

    const std::string& instrument() const noexcept { return instrument_; }

    // Replaces the whole book. Levels may be in any order; zero amounts are skipped.
    void load_snapshot(std::int64_t change_id, std::vector<PriceLevel> bids, std::vector<PriceLevel> asks);

    // Sets the amount at `price` (inserting the level if needed); amount <= 0 removes it.
    void set_level(BookSide side, double price, double amount);

    void clear() noexcept;
    void invalidate() noexcept { valid_ = false; }
    bool valid() const noexcept { return valid_; }

    std::int64_t change_id() const noexcept { return change_id_; }
    void set_change_id(std::int64_t id) noexcept { change_id_ = id; }
    std::int64_t timestamp_ms() const noexcept { return timestamp_ms_; }
    void set_timestamp_ms(std::int64_t ts) noexcept { timestamp_ms_ = ts; }

    std::optional<PriceLevel> best_bid() const noexcept;
    std::optional<PriceLevel> best_ask() const noexcept;
    bool crossed() const noexcept;  // best bid >= best ask: the book can't be trusted

    std::size_t depth(BookSide side) const noexcept { return side == BookSide::Bid ? bids_.size() : asks_.size(); }

    // The i-th best level (0 = best). Precondition: i < depth(side).
    const PriceLevel& level(BookSide side, std::size_t i) const noexcept {
        const auto& v = side == BookSide::Bid ? bids_ : asks_;
        return v[v.size() - 1 - i];
    }

    // Appends up to `n` best levels of `side` to `out`, best first.
    void top(BookSide side, std::size_t n, std::vector<PriceLevel>& out) const;

private:
    std::string instrument_;
    std::vector<PriceLevel> bids_;  // ascending price: best bid at back()
    std::vector<PriceLevel> asks_;  // descending price: best ask at back()
    std::int64_t change_id_ = 0;
    std::int64_t timestamp_ms_ = 0;
    bool valid_ = false;
};

}  // namespace de
