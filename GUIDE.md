# Study guide: C++ Deribit Engine

This guide explains how the engine works and *why* each piece is built the way it is, so you can explain
any part of it in an interview. File references point at the code. Read it top to bottom once, then use
the Q&A at the end to test yourself.

---

## 1. The pitch (30 seconds)

> "It's a C++20 trading engine for Deribit's testnet. One TLS WebSocket carries both order management
> (JSON-RPC: buy, sell, edit, cancel, positions) and the incremental order-book feed. The hot path runs on a
> single I/O thread with no locks. Incoming frames are parsed into a preallocated arena and applied to flat,
> sorted order books in about 1.4 µs. The engine then fans the books out to local clients through its own
> WebSocket server, with latest-value conflation so slow clients can't hurt anyone. Around that sit reconnect
> with backoff, request deadlines, client-side rate limiting, sequence-gap recovery, an asynchronous lock-free
> logger, and HdrHistogram-style latency measurement at every stage."

---

## 2. Following one market-data update end to end

1. **Socket → frame.** Deribit sends a WebSocket text frame on `book.BTC-PERPETUAL.100ms`. Beast's
   `async_read` completes on the Deribit I/O thread. The first thing we do is take `recv_ns = now_ns()`.
   Every later latency is measured from this moment ([client.cpp](src/deribit/client.cpp), `read()`).
2. **Parse into an arena.** `handle_frame` parses with Boost.JSON into a `monotonic_resource` laid over a
   preallocated 1 MB buffer. Every JSON node is a pointer bump in that buffer, and the whole thing is "freed"
   by simply reusing the buffer for the next frame. That is 1.7× faster than the default allocator
   (1.05 vs 1.78 µs) and the tail improves even more (p99 1.5 vs 4.7 µs), because there are no `malloc`/`free` calls.
3. **Dispatch.** No `id` field means a notification. `method == "subscription"` goes to `on_subscription`.
   Channels starting with `book.` go to the book logic. Others go to a generic handler.
4. **Apply with sequence checking** ([book_feed.cpp](src/deribit/book_feed.cpp)). Deribit sends one
   `snapshot`, then `change` messages that each carry `change_id` and `prev_change_id`. A change is applied
   only if `prev_change_id == book.change_id()`. Anything else means we missed a message, so the book is
   invalidated and we **resync**. Deribit sends a fresh snapshot on every subscribe, so resync is just
   unsubscribe + subscribe. While resyncing, stale changes are ignored rather than applied to a wrong book.
   A crossed result (best bid ≥ best ask) is also treated as corruption.
5. **Order book update** ([order_book.cpp](src/deribit/order_book.cpp)). Each level change is a binary search
   plus insert/erase in a flat sorted vector: 39 ns. See §4 for why this beats `std::map`.
6. **Book handler** ([cli.cpp](src/app/cli.cpp), `App::on_book`). Still on the I/O thread:
   - first, an optional strategy hook (the `bench loop` order fires here);
   - then, if anyone is subscribed, `build_book_message` serialises the top 10 levels **once**
     with `std::to_chars` (1.4 µs) into a `shared_ptr<const std::string>`;
   - `MarketDataServer::publish` copies the subscriber list under a mutex and posts the shared pointer to each
     client's strand. No per-client copy of the bytes is made.
7. **Per-client delivery** ([md_server.cpp](src/server/md_server.cpp)). Each session queues the message. If an
   older update for the same instrument is still waiting (not yet written), the new one **replaces** it
   (conflation). `async_write` sends it, and the completion records `now - recv_ns` into the fan-out histogram.

Measured end to end (engine receive → client process receive, 10 clients at 10k updates/s): p50 124–186 µs and
p99 231–487 µs, depending on machine state.
Most of that is the Windows loopback TCP stack and thread wake-ups, not our code.

## 3. Following one order

1. `client.place_order(order)` from any thread builds a `Request` and **`asio::dispatch`**es it to the I/O
   thread. If the caller is already on the I/O thread (a strategy reacting to a book update), `dispatch`
   runs it inline: zero thread hops. That's the low-latency path. `post` would always queue.
2. `submit()` checks, in order:
   - Shutting down?
   - Private method without credentials? Fail fast with `not_authenticated`.
   - Not yet authenticated? Park it in `waiting`.
   - Socket down? Park it in `waiting`.
   - Rate limit: the matching-engine token bucket (`buy/sell/edit/cancel`) or the general bucket. If empty,
     queue it in `throttled` and arm a timer for exactly when a token will be available.
3. `send()` assigns a JSON-RPC id and serialises the request. It stores `Pending{request, sent_ns}` in a hash
   map keyed by id, then writes. Beast allows one outstanding write per stream, so there's an `outbox` deque.
   If the outbox was empty, `async_write` frames, encrypts and pushes the bytes into the kernel before returning.
   For a request triggered by a market-data event, two numbers are recorded:
   - **tick-to-send (engine):** frame received → request serialised, just before the write. 38 µs p50 live.
   - **socket write (OS):** the write call itself. 141 µs p50 on Wi-Fi.

   Splitting them showed that the OS send path, not our code, was most of the latency. A raw `send()` of
   300 bytes to Deribit takes 133 µs on this machine, versus 15 µs on loopback.
4. The response with the same `id` arrives. `on_response` erases the pending entry, records
   `now - sent_ns` (order round trip), copies `result` out of the arena (the caller may keep it), and
   completes the callback or future.
5. Failure paths (all tested):
   - **Exchange error** (e.g. `not_enough_funds`): delivered with Deribit's code.
   - **10028 too_many_requests:** retried after 100 ms, then 200 ms, then 400 ms, at most 3 times.
     This is safe because a rejected request did nothing.
   - **Timeout:** the 100 ms sweep timer fails it with `timeout`. For orders the message says the state is
     *unknown*: the exchange may have executed it.
   - **Connection drops mid-flight:** fails with `disconnected`, *not* resent. Resending an order you can't prove
     was rejected risks a duplicate fill. The caller reconciles with `get_open_orders` or the label. This is
     the single most important reliability decision in the client.

## 4. Component deep dives

### Threading model

| Thread | Owns | Talks to others via |
|---|---|---|
| Deribit I/O (1) | socket, pending map, queues, order books, token buckets | `asio::dispatch`/`post` in, callbacks out |
| Server I/O (N) | sessions; each session on its own **strand** | `publish()` (mutex for the subscriber map only) |
| Logger writer (1) | log file | lock-free MPMC queue |
| Main / CLI | stdin | futures |

Why one I/O thread for Deribit? There's one TCP connection. Its bytes are serial anyway, and keeping all
client state on one thread means **no locks on the hot path** and no data races by construction. That's the
classic "single-writer principle". The only shared data are atomics: stats, histograms, state flags.

A **strand** guarantees that a session's handlers never run concurrently, even though the server has N
threads. So per-session state (queue, flags) needs no mutex, and different sessions still run in parallel.

### Order book: flat sorted vectors, best price at the back

- `std::map<double,double>`: O(log n), but every level is a separate heap node. Lookups chase pointers
  through scattered memory, which means cache misses, and inserts call `malloc`.
- A flat `std::vector<PriceLevel>`: lookups are a binary search over contiguous memory. Insert/erase is
  O(n) in theory, because elements behind it shift.

The trick: almost all activity happens at the top of the book. So each side is sorted **worst → best**
(bids ascending, asks descending) and the best price sits at `back()`. An update near the touch only shifts
the few elements after it. Measured: 39 ns per update on a 1000-level book. Correctness is model-checked
against `std::map` over 50k random operations (`book_matches_reference_map_under_random_updates`).

Prices are compared with `==` on doubles. That's safe here: Deribit prices are exact tick multiples, and the
same decimal text always parses to the same double. A production engine would usually convert to integer
ticks; that's a good "what would you change" answer.

### The asynchronous logger

Problem: `fprintf` + `fflush` can take tens of microseconds, and disk stalls can take milliseconds. You can't
do that on the trading thread.

Solution ([logger.hpp](src/common/logger.hpp)):
1. The caller formats into a **fixed 232-byte record on its own stack** (`std::format_to_n`, no heap).
2. It pushes the record into a bounded lock-free queue.
3. A background thread pops records, formats the timestamp, and writes and flushes.
4. If the queue is full, the record is **dropped and counted**. The hot path must never block on logging.

Cost to the caller: ~230 ns (p50 201 ns).

The queue is **Dmitry Vyukov's bounded MPMC queue** ([mpmc_queue.hpp](src/common/mpmc_queue.hpp)). Each cell
has a sequence number:
- `seq == pos` → the cell is free for the producer holding ticket `pos`;
- `seq == pos+1` → the cell is full for the consumer holding ticket `pos`.

A producer CASes `enqueue_pos` forward to claim a ticket, writes the value, then does a *release* store of
`seq = pos+1`. The consumer's *acquire* load of `seq` therefore sees the value fully written. The two counters
live on separate cache lines (`alignas(64)`) to avoid false sharing. Tested with 4 producers and 4 consumers,
checking that every value is delivered exactly once.

### Latency histogram

Storing every sample and sorting is too slow and grows without bound. Instead
([latency_histogram.hpp](src/common/latency_histogram.hpp)):
- values below 32 ns get exact buckets;
- above that, each power-of-two range `[2^k, 2^(k+1))` is split into 32 equal buckets.

The bucket index comes from the position of the top bit (`std::bit_width`) plus the next 5 bits, so it's O(1).
Relative error is at most 1/32 ≈ 3%, memory is a fixed ~10 KB, and `record` is a handful of relaxed atomic
increments (26 ns). So it's wait-free and callable from any thread. This is the same idea as HdrHistogram.

### Rate limiting: token bucket

Deribit uses credit-based limits, with the matching engine stricter than everything else. Two buckets:
`rate` tokens per second refill up to `burst`, and each request costs one. When a bucket is empty, the request
goes into a FIFO, and a timer is armed for exactly `wait_ns = (cost - tokens) / rate` seconds. It's
deterministic: time is passed in, so tests don't sleep. Behind this, a 10028 reply is still handled by
backoff-retry. Defence in depth.

### Reconnect and session recovery

- **Backoff:** `min(30 s, 500 ms · 2^attempt)` + up to 25% random jitter. Jitter stops many clients reconnecting
  in lockstep after an outage (the "thundering herd").
- **Connection object:** each connection is a fresh `shared_ptr<Connection>` with an id. Every async handler
  captures it and returns immediately if `c != conn`. So callbacks from a dead connection can't corrupt the
  new one, and the old stream stays alive until its handlers have drained.
- **On reconnect:** set heartbeat → authenticate → resubscribe public channels → (after auth) private
  channels → flush the `waiting` queue.
- **Liveness:** Deribit heartbeats every 10 s and we answer its `test_request`. If nothing at all arrives for
  3× the interval, the link is declared dead and torn down. A TCP connection can stay "open" through a dead
  NAT for minutes, which is why you need application-level heartbeats.
- **Tokens:** refreshed with the refresh token at 80% of `expires_in`. If that fails, the engine falls back to
  full client-credentials auth.

### WebSocket server: shared payload, strands, conflation, demand

- **Serialise once:** 10 clients means one string and 10 refcount increments, not 10 copies.
- **Latest-value conflation:** every message is a full top-10 snapshot, so an older queued update for the
  same instrument is worthless once a newer one exists. Replacing it in place means:
  - a slow client gets fewer but always-current snapshots instead of a growing, stale backlog;
  - memory per client is bounded (≤ one pending update per instrument).

  Before this design, a lagging client sat on a 256-message backlog: 40 ms of stale data. After it, p50 is
  124 µs under the same load.
- **Requests can't be conflated.** A client that sends requests but never reads the replies would grow its
  queue forever, so after `max_queue` it's disconnected.
- **Demand:** the first subscriber to an instrument triggers a Deribit subscription, and the last one leaving
  cancels it (reference counted in `App`, so the CLI's own `sub` doesn't get cancelled by a server client
  leaving). The demand callback runs under the server lock, so subscribe and unsubscribe events can't be
  reordered.

### TLS

OpenSSL can't read the Windows certificate store, so on Windows the engine enumerates the `ROOT` store with
CryptoAPI and adds each certificate to OpenSSL's `X509_STORE` ([tls_roots.cpp](src/deribit/tls_roots.cpp)).
It sets SNI and verifies the host name (`ssl::host_name_verification`). Skipping verification would make
the API keys interceptable.

## 5. How the numbers were measured (and what they don't mean)

- **Offline numbers** (`engine_bench`) are pure CPU work: two passes per benchmark, untimed for ns/op and
  timed per operation for percentiles. Windows `steady_clock` ticks every 100 ns, so a "p50 = 100 ns" means
  "one tick or less".
- **Fan-out numbers** are cross-process. The engine stamps `recv_ns` into each message, and the client
  subtracts it from its own clock. This works because `steady_clock` is system-wide on Windows (QPC) and
  Linux (`CLOCK_MONOTONIC`).
- **Live round trips (~195 ms)** are network-bound: India ↔ Deribit's data centre. The engine's own
  contribution is the microseconds measured above. Say this clearly in an interview: "internal latency is
  microseconds; total latency is dominated by geography, which is why real HFT co-locates."

## 6. Testing strategy

The hard part is testing the Deribit client without depending on the internet. The answer is
`tests/test_client.cpp`: a **fake Deribit server** that runs real TLS (a P-256 self-signed certificate for
`127.0.0.1`, generated with the OpenSSL API at test start and passed to the client as its CA file) and a real
WebSocket. Each test scripts the server's replies:

| Scenario | What is verified |
|---|---|
| auth then buy | the order waits until authenticated; auth is sent before buy |
| exchange error | Deribit error code and message reach the caller |
| 10028 then success | transparent retry; counter incremented |
| no reply | `timeout` after the deadline |
| snapshot, change, gap | gap detected, unsubscribe + subscribe, new snapshot; gapped update never applied |
| server drops mid-request | `disconnected` error; reconnect; subscription restored on connection #2 |
| heartbeat `test_request` | client replies with `public/test` |
| stop while reconnecting | queued requests fail with `shutting_down`; stop is prompt |

## 7. Interview questions to practise

1. **Why not `std::map` for the book?** Pointer chasing and allocation per level; see §4. Flat vector, best at back.
2. **What happens if you miss a book update?** `prev_change_id` mismatch → invalidate → resubscribe → fresh snapshot.
3. **Your connection drops right after sending an order. What do you do?** Don't resend. Report "unknown",
   then reconcile via open orders or the label. Resending could double the position.
4. **Why one I/O thread?** One socket is serial anyway. Single-writer means no locks or races on the hot path.
5. **`post` vs `dispatch`?** `dispatch` runs inline if you're already on that executor. The strategy-to-order
   path costs no extra queue hop.
6. **How does the lock-free queue avoid the ABA problem?** Tickets only increase (64-bit, effectively never
   wrap), and per-cell sequence numbers encode the "lap", so a stale CAS can't succeed on a reused cell.
7. **Why release/acquire on the cell sequence?** It publishes the written value: the consumer's acquire load
   synchronises with the producer's release store.
8. **What is false sharing and where did you avoid it?** Two threads writing different variables on the same
   cache line ping-pong it between cores. The queue's two counters are `alignas(64)`.
9. **Why might a logger cause latency spikes, and how did you fix it?** Disk I/O and locks on the caller's
   thread. Fixed by formatting into a stack record, pushing lock-free, and doing I/O on a background thread.
   Dropped rather than blocked.
10. **How is p99 computed without storing samples?** Log-linear buckets, cumulative count up to the rank. ≤3% error.
11. **What is conflation and when is it wrong?** Replacing stale snapshots with newer ones. It's right for
    snapshot/state data. It's wrong for event streams like trades or order fills, where every message matters.
12. **How do you handle rate limits?** A token bucket per Deribit limit class, with requests queued until a
    token is available. 10028 is retried with backoff.
13. **What does jitter in backoff solve?** Synchronised reconnect storms after an outage.
14. **How would you go lower-latency?**
    - Integer prices.
    - A hand-written parser for the two hot message shapes (skip the DOM).
    - Busy-poll the I/O thread instead of sleeping in the kernel.
    - Pin threads to cores.
    - A kernel-bypass network stack.
    - Co-location: the network is 99.99% of the 195 ms.
15. **Why is your loopback fan-out ~120 µs if the code is ~1.4 µs?** Windows TCP loopback, IOCP completion
    and thread wake-ups dominate. Measure before optimising.
16. **Your first tick-to-send was 186 µs. How did you find out why?** I instrumented each stage: parse, order
    build, serialise, socket write. The write was 100–220 µs. I first suspected CPU power states, but sending
    1 ms apart didn't change it. Then I timed a raw `send()` with no TLS and no WebSocket: 133 µs to Deribit
    over Wi-Fi versus 15 µs on loopback. So it's the OS and network driver. I split the metric into engine time
    (38 µs live, cold caches) and socket time, and report both. The fix would be wired networking, Linux, or
    kernel bypass, not changing the C++.
17. **You tried a shard-per-thread server. Why didn't you keep it?** One `io_context` per thread means a publish
    posts one task per thread instead of one per client, so I expected it to scale better at 250 clients. An A/B
    test, alternating both builds 3 times, showed +15–20% throughput but ~35% worse latency. Both hit the same
    ceiling, which is Windows loopback TCP: adding threads barely changed anything. So the simpler design stayed.
    The lesson: run alternating A/B tests, because single runs on a laptop varied by more than the difference.
18. **At 1000 clients, latency is 3.9 ms p50 but throughput is still 100k msg/s. Why both?** Throughput is
    fine because every update is serialised once and shared, and the 2 threads keep up: nothing was conflated
    or dropped. Latency grows with clients because one update means 1000 socket writes, and the last client
    waits for the writes before it. 3.9 ms ÷ (1000 / 2 threads) ≈ 8 µs per write. To cut it, add I/O threads.
    For very large audiences, use a multicast or relay tier so one process doesn't write to everyone.
19. **How do you know the books are correct?**
    - Model check against `std::map`.
    - Sequence-gap tests.
    - Crossed-book detection at runtime.

## 8. Ideas for next steps

- Integer tick prices; a specialised zero-copy parser for `book.*` frames
- Subscribe to `user.orders.*` / `user.trades.*` to track order state from the stream instead of polling
- Persist an order journal (label, state) for reconciliation after a crash
- Busy-polling mode for the I/O thread; CPU pinning
- A simple strategy (e.g. a market maker quoting around mid) built on the `bench loop` hook
