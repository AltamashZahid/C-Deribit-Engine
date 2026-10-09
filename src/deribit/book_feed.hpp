#pragma once

#include <string_view>

#include <boost/json/object.hpp>

#include "deribit/order_book.hpp"

namespace de::deribit {

enum class BookApply {
    Snapshot,  // book replaced by a full snapshot
    Applied,   // incremental change applied in sequence
    Gap,       // prev_change_id didn't match: an update was missed, book invalidated
    Invalid,   // change arrived before any snapshot, or the result was a crossed book
};

std::string_view to_string(BookApply r) noexcept;

// Applies the `data` object of one Deribit `book.<instrument>.<interval>` notification.
//
//   {"type":"snapshot"|"change", "change_id":N, "prev_change_id":N-k, "timestamp":ms,
//    "bids":[["new"|"change"|"delete", price, amount], ...], "asks":[...]}
//
// A "change" is only applied if its prev_change_id equals the book's current change_id;
// otherwise the book is invalidated and the caller must resubscribe to get a fresh
// snapshot. Throws boost::system::system_error on a structurally malformed message.
BookApply apply_book_message(OrderBook& book, const boost::json::object& data);

}  // namespace de::deribit
