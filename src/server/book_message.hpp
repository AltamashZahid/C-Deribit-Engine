#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#include "deribit/order_book.hpp"

namespace de {

// Serialises the best `depth` levels of `book` into the message pushed to local clients:
//
//   {"type":"book","instrument":"BTC-PERPETUAL","change_id":123,"exch_ts":1791497024149,
//    "recv_ns":<engine receive time, steady clock>,"bids":[[price,amount],...],"asks":[...]}
//
// Every message is a complete top-of-book snapshot, so a client can join at any time and
// a slow client loses nothing by skipping messages. The message is built once per update
// and shared by all subscribers.
std::string build_book_message(const OrderBook& book, std::size_t depth, std::int64_t recv_ns);

}  // namespace de
