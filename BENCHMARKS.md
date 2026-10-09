# Benchmark results

All results below were produced by the tools in this repository and can be reproduced with the commands shown.
Numbers are medians of repeated runs unless stated otherwise.

## 1. Market data server at scale (Linux)

Run on GitHub Actions ([run 37922691655](https://github.com/AltamashZahid/C-Deribit-Engine/actions/runs/37922691655)),
workflow **Load test**, script [`scripts/load_test.sh`](scripts/load_test.sh).

```
host: Linux 6.17.0-1022-azure x86_64      (GitHub-hosted Ubuntu 24.04 runner)
cpus: 4  (AMD EPYC 7763)
server threads: 2, client threads: 2, window: 10 s, runs: 3
```

The synthetic feed is sized so that every row asks the server for the same 100,000 msg/s in total
(clients × updates per second):

| Clients | Feed (upd/s) | Wanted msg/s | Delivered msg/s | MB/s | p50 | p99 | p99.9 | Conflated | Dropped |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 10 | 10,000 | 100,000 | 98,086 | 37.9 | 110 µs | 283 µs | 397 µs | 2% | 0 |
| 100 | 1,000 | 100,000 | 99,969 | 38.5 | 438 µs | 1.10 ms | 1.92 ms | 0% | 0 |
| 500 | 200 | 100,000 | 99,999 | 38.5 | 1.95 ms | 5.05 ms | 6.49 ms | 0% | 0 |
| 1000 | 100 | 100,000 | 100,000 | 38.5 | 3.90 ms | 8.52 ms | 9.57 ms | 0% | 0 |

**What it shows**
- **Throughput holds flat from 10 to 1000 clients.** The server delivered essentially all 100k msg/s requested,
  on 2 I/O threads, with no client dropped.
- **Almost nothing was conflated** (0–2%). Clients kept up, so they received every update rather than
  merged snapshots.
- **Latency grows with the number of clients, roughly linearly.** Each update has to be written to every
  subscriber, and the last client in line waits for the writes ahead of it. At 1000 clients and 2 threads,
  p50 3.9 ms implies about 8 µs per WebSocket write per thread. This is the cost of a large fan-out from one
  process, not queueing (nothing was conflated or dropped). More I/O threads would shorten it.
- Server and load generator share the same 4 vCPUs, so these numbers are a lower bound for a dedicated server.
- The engine process used ~170% CPU, but that includes the synthetic feed generator, which spin-waits to keep
  its pacing exact. So it doesn't mean the server threads were saturated.

Latency is measured from the moment the engine received (here: generated) the update to the moment the
client process read it, using the system-wide monotonic clock on the same host.

**Reproduce:** Actions → *Load test* → *Run workflow*, or locally on Linux:
```sh
CONFIGS="10:10000 100:1000 500:200 1000:100" RUNS=3 bash scripts/load_test.sh build load-results
python3 scripts/summarize_load.py load-results/results.csv load-results/machine.txt
```

## 2. Market data server on Windows (laptop)

Intel i5-13500H (16 logical CPUs), Windows 11, 4 server threads, 4 client threads, server and clients on the
same machine. Median of 3 runs each:

| Clients | Feed (upd/s) | Delivered msg/s | p50 | p99 | Dropped |
|---:|---:|---:|---:|---:|---:|
| 10 | 10,000 | 55,000 | 186 µs | 487 µs | 0 |
| 100 | 1,000 | 54,000 | 1.5 ms | 3.2 ms | 0 |
| 250 | 400 | 50,000 | 3.9 ms | 8.2 ms | 0 |

Windows tops out at about 55k msg/s regardless of server thread count (4 → 8 made little difference). The 4-vCPU
Linux runner delivers ~100k msg/s with half the server threads. The limit on Windows is its loopback TCP
stack, not the engine. Past that point the server conflates (slow clients get fewer, current snapshots) instead
of dropping anyone.

## 3. Hot path, offline (`engine_bench 500000`)

Pure CPU time, no network. Same Windows laptop, GCC 16.2 `-O3`. `ns/op` comes from an untimed pass; the
percentiles come from a separately timed pass and include one clock read (~40 ns) at 100 ns timer resolution.

| Operation | ns/op | p50 | p99 |
|---|---:|---:|---:|
| Order book `set_level` (1000-level book, updates near top) | 39 | 100 ns | 100 ns |
| Parse Deribit book frame, arena allocator | 1,055 | 0.9 µs | 1.5 µs |
| Parse Deribit book frame, default allocator | 1,784 | 1.8 µs | 4.7 µs |
| Parse + apply book frame to the order book | 1,990 | 1.4 µs | 2.8 µs |
| Build top-10 client message | 1,403 | 1.4 µs | 3.3 µs |
| MPMC queue push + pop | 13 | — | — |
| Latency histogram `record` | 26 | — | — |
| `LOG_INFO` call (async logger, cost to the caller) | 228 | 201 ns | 299 ns |

## 4. Live Deribit Testnet (from India)

Windows laptop on Wi-Fi, authenticated Testnet account.

| Measurement | p50 | p90 |
|---|---:|---:|
| Order placement round trip (`bench orders`, 20 orders) | 229 ms | 237 ms |
| Cancel round trip | 224 ms | 233 ms |
| **Tick-to-send, engine:** book frame received → order serialised (`bench loop`, 30 iterations) | **38 µs** | 59 µs |
| Socket write: WebSocket framing + TLS + Windows `send()` | 141 µs | 182 µs |
| Tick-to-ack: book frame received → order acknowledged | 216 ms | 258 ms |

The round trips are the India ↔ London network. A raw 300-byte `send()` to Deribit takes 133 µs on this Wi-Fi
link (15 µs on loopback), which is most of the socket-write row. On the live feed the engine's own path runs on
cold caches, because Testnet updates arrive ~0.6 s apart. That's why it shows 38 µs, against ~1.5 µs warm offline.

## Design decisions these measurements drove

| Finding | Change |
|---|---|
| A lagging client sat on a 256-message backlog (40 ms stale) | Latest-value conflation: ≤ 1 pending update per instrument per client → p50 124 µs at the same load |
| `std::format` for doubles cost 3.4 µs per message | `std::to_chars` → 1.4 µs |
| Tick-to-send of 186 µs looked like slow code | Instrumented each stage: the OS send call was 100–220 µs, so the metric was split into engine time and socket time |
| A shard-per-thread server looked better for many clients | Alternating A/B test: +15–20% throughput but ~35% higher latency, so the simpler strand design was kept |
| Low-rate synthetic feeds were bursty on Windows (sleep oversleeps ~15 ms) | Generator spin-waits for gaps < 20 ms; the 250-client result was corrected from 26k to 50k msg/s |
| Parallel builds ran out of RAM | Documented `-j 4` |
