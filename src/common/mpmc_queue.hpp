#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <utility>

namespace de {

// Bounded lock-free multi-producer / multi-consumer queue (Dmitry Vyukov's design).
//
// Every cell carries a sequence number saying whose turn it is:
//   seq == pos       -> the cell is empty and the producer holding ticket `pos` may fill it
//   seq == pos + 1   -> the cell is full and the consumer holding ticket `pos` may drain it
// A push or pop is one CAS on the shared ticket counter plus one release store on the
// cell, and producers never touch the consumers' counter (separate cache lines).
// try_push fails instead of blocking when the queue is full.
template <class T>
class MpmcQueue {
public:
    explicit MpmcQueue(std::size_t capacity)
        : mask_(checked_capacity(capacity) - 1), cells_(new Cell[capacity]) {
        for (std::size_t i = 0; i < capacity; ++i) cells_[i].seq.store(i, std::memory_order_relaxed);
    }

    MpmcQueue(const MpmcQueue&) = delete;
    MpmcQueue& operator=(const MpmcQueue&) = delete;

    template <class U>
    bool try_push(U&& value) {
        Cell* cell = nullptr;
        std::size_t pos = enqueue_pos_.load(std::memory_order_relaxed);
        for (;;) {
            cell = &cells_[pos & mask_];
            const std::size_t seq = cell->seq.load(std::memory_order_acquire);
            const auto diff = static_cast<std::intptr_t>(seq) - static_cast<std::intptr_t>(pos);
            if (diff == 0) {
                if (enqueue_pos_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) break;
            } else if (diff < 0) {
                return false;  // full: the consumer hasn't freed this cell yet
            } else {
                pos = enqueue_pos_.load(std::memory_order_relaxed);
            }
        }
        cell->value = std::forward<U>(value);
        cell->seq.store(pos + 1, std::memory_order_release);
        return true;
    }

    bool try_pop(T& out) {
        Cell* cell = nullptr;
        std::size_t pos = dequeue_pos_.load(std::memory_order_relaxed);
        for (;;) {
            cell = &cells_[pos & mask_];
            const std::size_t seq = cell->seq.load(std::memory_order_acquire);
            const auto diff = static_cast<std::intptr_t>(seq) - static_cast<std::intptr_t>(pos + 1);
            if (diff == 0) {
                if (dequeue_pos_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) break;
            } else if (diff < 0) {
                return false;  // empty
            } else {
                pos = dequeue_pos_.load(std::memory_order_relaxed);
            }
        }
        out = std::move(cell->value);
        cell->seq.store(pos + mask_ + 1, std::memory_order_release);
        return true;
    }

    std::size_t capacity() const noexcept { return mask_ + 1; }

private:
    struct Cell {
        std::atomic<std::size_t> seq{0};
        T value{};
    };

    static std::size_t checked_capacity(std::size_t capacity) {
        if (capacity < 2 || (capacity & (capacity - 1)) != 0)
            throw std::invalid_argument("MpmcQueue capacity must be a power of two >= 2");
        return capacity;
    }

    const std::size_t mask_;
    std::unique_ptr<Cell[]> cells_;
    alignas(64) std::atomic<std::size_t> enqueue_pos_{0};
    alignas(64) std::atomic<std::size_t> dequeue_pos_{0};
};

}  // namespace de
