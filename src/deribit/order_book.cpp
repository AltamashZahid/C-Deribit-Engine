#include "deribit/order_book.hpp"

#include <algorithm>

namespace de {
namespace {

// "Worse" ordering for a side: for bids a lower price is worse, for asks a higher one.
template <bool IsBid>
constexpr bool worse(double a, double b) noexcept {
    if constexpr (IsBid) return a < b;
    else return a > b;
}

template <bool IsBid>
void set_level_impl(std::vector<PriceLevel>& levels, double price, double amount) {
    auto it = std::lower_bound(levels.begin(), levels.end(), price,
                               [](const PriceLevel& l, double p) { return worse<IsBid>(l.price, p); });
    const bool found = it != levels.end() && it->price == price;
    if (amount <= 0) {
        if (found) levels.erase(it);
        return;
    }
    if (found) it->amount = amount;
    else levels.insert(it, PriceLevel{price, amount});
}

template <bool IsBid>
void load_side(std::vector<PriceLevel>& dst, std::vector<PriceLevel>&& src) {
    std::erase_if(src, [](const PriceLevel& l) { return l.amount <= 0; });
    std::sort(src.begin(), src.end(), [](const PriceLevel& a, const PriceLevel& b) { return worse<IsBid>(a.price, b.price); });
    // A duplicated price keeps its last occurrence.
    auto last = std::unique(src.rbegin(), src.rend(), [](const PriceLevel& a, const PriceLevel& b) { return a.price == b.price; });
    src.erase(src.begin(), last.base());
    dst = std::move(src);
}

}  // namespace

void OrderBook::load_snapshot(std::int64_t change_id, std::vector<PriceLevel> bids, std::vector<PriceLevel> asks) {
    load_side<true>(bids_, std::move(bids));
    load_side<false>(asks_, std::move(asks));
    change_id_ = change_id;
    valid_ = true;
}

void OrderBook::set_level(BookSide side, double price, double amount) {
    if (side == BookSide::Bid) set_level_impl<true>(bids_, price, amount);
    else set_level_impl<false>(asks_, price, amount);
}

void OrderBook::clear() noexcept {
    bids_.clear();
    asks_.clear();
    change_id_ = 0;
    timestamp_ms_ = 0;
    valid_ = false;
}

std::optional<PriceLevel> OrderBook::best_bid() const noexcept {
    if (bids_.empty()) return std::nullopt;
    return bids_.back();
}

std::optional<PriceLevel> OrderBook::best_ask() const noexcept {
    if (asks_.empty()) return std::nullopt;
    return asks_.back();
}

bool OrderBook::crossed() const noexcept {
    return !bids_.empty() && !asks_.empty() && bids_.back().price >= asks_.back().price;
}

void OrderBook::top(BookSide side, std::size_t n, std::vector<PriceLevel>& out) const {
    const auto& v = side == BookSide::Bid ? bids_ : asks_;
    const auto count = std::min(n, v.size());
    for (std::size_t i = 0; i < count; ++i) out.push_back(v[v.size() - 1 - i]);
}

}  // namespace de
