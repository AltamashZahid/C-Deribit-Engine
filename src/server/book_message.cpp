#include "server/book_message.hpp"

#include <algorithm>
#include <charconv>

namespace de {
namespace {

// std::to_chars writes the shortest text that round-trips exactly, straight into a stack
// buffer: several times faster than std::format for doubles.
template <class T>
void append_number(std::string& out, T value) {
    char buf[32];
    const auto res = std::to_chars(buf, buf + sizeof(buf), value);
    out.append(buf, res.ptr);
}

void append_side(std::string& out, const OrderBook& book, BookSide side, std::size_t depth) {
    const auto n = std::min(depth, book.depth(side));
    out.push_back('[');
    for (std::size_t i = 0; i < n; ++i) {
        const auto& l = book.level(side, i);
        if (i) out.push_back(',');
        out.push_back('[');
        append_number(out, l.price);
        out.push_back(',');
        append_number(out, l.amount);
        out.push_back(']');
    }
    out.push_back(']');
}

}  // namespace

std::string build_book_message(const OrderBook& book, std::size_t depth, std::int64_t recv_ns) {
    std::string out;
    out.reserve(128 + depth * 2 * 32);
    out += R"({"type":"book","instrument":")";
    out += book.instrument();
    out += R"(","change_id":)";
    append_number(out, book.change_id());
    out += R"(,"exch_ts":)";
    append_number(out, book.timestamp_ms());
    out += R"(,"recv_ns":)";
    append_number(out, recv_ns);
    out += R"(,"bids":)";
    append_side(out, book, BookSide::Bid, depth);
    out += R"(,"asks":)";
    append_side(out, book, BookSide::Ask, depth);
    out.push_back('}');
    return out;
}

}  // namespace de
