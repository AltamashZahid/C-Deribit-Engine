# C++ Deribit Engine

A low-latency trading engine in C++20 for the [Deribit](https://www.deribit.com) **Testnet**. It keeps one
TLS WebSocket session to Deribit for order management and live market data. It maintains local order books
from the incremental feed. It re-broadcasts those books to any number of local clients through its own
WebSocket server.

```
                        ┌──────────────────────── deribit_engine ─────────────────────────┐
 wss://test.deribit.com │  Deribit I/O thread (single-threaded, lock-free hot path)       │
 ◄────── JSON-RPC ─────►│   TLS WebSocket ─► arena JSON parse ─► OrderBook (flat vectors) │
   orders, auth,        │        ▲                                   │ book handler       │
   book.* stream        │        │ requests (dispatch)               ▼                    │
                        │   CLI / strategy                build message once (to_chars)   │
                        │                                            │ shared_ptr         │
                        │  Market data server (N threads, one strand per client)          │
                        │   per-client latest-value queue (conflation) ─► async_write ────┼──► ws://127.0.0.1:8080
                        │  Async logger: lock-free MPMC queue ─► background writer thread │      local clients
                        └─────────────────────────────────────────────────────────────────┘
```

## Features

**Trading (Deribit JSON-RPC over WebSocket)**
- Place, cancel, modify (edit) orders: limit and market, post-only, reduce-only, labels
- Cancel all / by instrument / by label; open orders, positions, account summary
- Order book snapshots, instrument lists, plus any raw JSON-RPC method (`call ...`)
- Supports all Deribit instrument kinds (futures, perpetuals, options, spot)

**Market data**
- Subscribes to `book.<instrument>.100ms` and maintains a local order book per instrument
- Sequence checking (`prev_change_id`): a gap or crossed book triggers an automatic resubscribe for a fresh snapshot
- Built-in WebSocket server: clients subscribe per instrument and get a top-10 snapshot on every update
- Demand-driven: the engine streams an instrument from Deribit only while someone is subscribed (reference-counted)

**Reliability**
- Reconnect with exponential backoff and jitter. Re-authenticates and resubscribes automatically.
- Requests made while disconnected are queued and sent after reconnect. Requests in flight when the link
  dropped fail with `disconnected` and are never blindly resent, because resending an order could duplicate it.
- Per-request deadlines, plus client-side token buckets for Deribit's matching-engine and non-matching limits.
  A `too_many_requests` (10028) reply is retried with backoff.
- Answers Deribit heartbeats (`test_request`) and tears down a silent connection. Refreshes OAuth tokens before they expire.
- Slow server clients get conflated (latest-value) updates. A client that floods requests without reading is dropped,
  so memory stays bounded.
- TLS certificates are verified: Windows root store, OS default paths on Linux, or a custom CA file.

**Measurement**
- Wait-free log-linear latency histograms (HdrHistogram-style, ±3%) on every stage
- `engine_bench`: offline hot-path micro-benchmarks
- `md_client_bench`: multi-client load generator for the WebSocket server
- `bench orders` / `bench loop` CLI commands: live order round-trip and tick-to-trade measurements

## Performance

Headline numbers. Full tables, test setup and analysis are in **[BENCHMARKS.md](BENCHMARKS.md)**.

**Market data server, Linux** (4-vCPU GitHub runner, 2 server threads, 100k msg/s asked of the server in every row,
median of 3 runs, [Load test run](https://github.com/AltamashZahid/C-Deribit-Engine/actions/runs/37922691655)):

| Clients | Delivered msg/s | p50 | p99 | Dropped |
|---:|---:|---:|---:|---:|
| 10 | 98,086 | 110 µs | 283 µs | 0 |
| 100 | 99,969 | 438 µs | 1.10 ms | 0 |
| 500 | 99,999 | 1.95 ms | 5.05 ms | 0 |
| 1000 | 100,000 | 3.90 ms | 8.52 ms | 0 |

**Hot path, offline** (`engine_bench`, i5-13500H laptop): order book update **39 ns**, Deribit book frame
parse + apply **1.4 µs** p50, arena JSON parse 1.7× faster than the default allocator, async log call **228 ns**.

**Live Deribit Testnet** (from India): engine tick-to-send (book update → order ready to send) **38 µs** p50.
Order round trip 229 ms, nearly all of it the India ↔ London network.

Testnet's order books are thin, at about two `book.100ms` updates per second for BTC-PERPETUAL. Use the synthetic feed to load-test.

## Building

Requirements: a C++20 compiler (GCC ≥ 13, Clang ≥ 17, MSVC 19.3x), CMake ≥ 3.20, Boost ≥ 1.83 (Asio, Beast, JSON), OpenSSL 3.

**Windows (MSYS2 UCRT64):**
```sh
pacman -S mingw-w64-ucrt-x86_64-{gcc,cmake,ninja,boost,openssl}
cmake -S . -B build -G Ninja
cmake --build build -j 4        # Beast translation units need ~1 GB RAM each; limit -j on small machines
```
On MinGW the executables are linked statically and run without MSYS2 on `PATH`.

**Linux:**
```sh
sudo apt install build-essential cmake ninja-build libboost-all-dev libssl-dev
cmake -S . -B build -G Ninja && cmake --build build
```
Sanitizers: `-DDE_ENABLE_ASAN=ON` (Address + UB) or `-DDE_ENABLE_TSAN=ON`.

## Running

1. Create Testnet API keys at <https://test.deribit.com> → Account → API, with scopes `trade:read_write` and `account:read`.
2. `cp .env.example .env` and fill in `DERIBIT_CLIENT_ID` / `DERIBIT_CLIENT_SECRET`. `.env` is gitignored.
   Without keys, everything public still works: market data, books, instruments.
3. `./build/deribit_engine`

```
> instruments BTC future
> book BTC-PERPETUAL 5
> buy BTC-PERPETUAL 10 60000 post_only label=test     # amount in USD for inverse perpetuals
> orders
> modify <order_id> 20 60500
> cancel <order_id>
> positions BTC
> sub ETH-PERPETUAL          # stream into the local book
> top ETH-PERPETUAL
> bench orders BTC-PERPETUAL 20
> bench loop BTC-PERPETUAL 20
> stats
```
Run `help` for the full list. Run `deribit_engine --help` for flags: headless `--no-cli`, `--subscribe`,
`--synthetic RATE`, `--port`, `--depth`.

### Market data server protocol

Connect to `ws://127.0.0.1:8080` and send JSON text frames:

```json
{"op":"subscribe","instrument":"BTC-PERPETUAL"}     → {"type":"subscribed","instrument":"BTC-PERPETUAL"}
{"op":"unsubscribe","instrument":"BTC-PERPETUAL"}   → {"type":"unsubscribed","instrument":"BTC-PERPETUAL"}
{"op":"ping"}                                       → {"type":"pong","server_ns":...}
```
Every update of a subscribed instrument then arrives as a full top-of-book snapshot:
```json
{"type":"book","instrument":"BTC-PERPETUAL","change_id":122393390117,"exch_ts":1791497024149,
 "recv_ns":<engine receive time>,"bids":[[81651,1057520],...],"asks":[[81651.5,1020200],...]}
```

### Benchmarks

```sh
./build/engine_bench 500000                                            # offline hot path
./build/deribit_engine --no-cli --synthetic 10000 &                    # synthetic feed
./build/md_client_bench --clients 10 --instrument SYN-PERP --seconds 10
```

**Linux load test.** `scripts/load_test.sh` sweeps client counts (default 10 / 100 / 500 / 1000). Each config runs
3 times and the median is reported, with server CPU, conflation share and drop count. On GitHub:
**Actions → Load test → Run workflow** runs it on an Ubuntu runner and posts the table on the run's summary page.

## Tests

`./build/unit_tests` runs 36 tests in about 2 s. They cover:
- **Order book:** model-checked against `std::map` over 50k random operations. Also sequence-gap, crossed-book and malformed-message handling.
- **Deribit client:** tested against a **local fake Deribit server that speaks real TLS** with a self-signed certificate
  generated at test time. Covers auth ordering, exchange errors, rate-limit retry, request timeout, gap → resubscribe,
  reconnect + resubscribe with in-flight requests failed, heartbeat replies, and shutdown.
- **Market data server:** subscribe/fan-out/demand ref-counting, protocol errors, conflation for a slow consumer,
  dropping a client that never reads, and prompt shutdown.
- **Utilities:** histogram accuracy and concurrency, a 4-producer/4-consumer MPMC exactly-once check, token bucket, and `.env` parsing.

CI runs them on Linux (GCC; Clang with ASan+UBSan; Clang with TSan) and on Windows (MSYS2).

## Layout

```
src/common/   logger (async, lock-free queue), latency histogram, MPMC queue, token bucket, config/.env
src/deribit/  client (TLS WebSocket JSON-RPC), order book, book feed (snapshot/change/gap logic), TLS roots
src/server/   market data WebSocket server, book message serialisation
src/app/      deribit_engine executable: wiring, CLI, live benchmarks
bench/        engine_bench, md_client_bench
tests/        unit + integration tests (fake TLS Deribit server)
```

See [GUIDE.md](GUIDE.md) for a walkthrough of the design and the reasoning behind each decision.
