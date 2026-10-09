#!/usr/bin/env bash
# Market data server load test: for each "clients:rate" config, start the engine with a
# synthetic feed, attach md_client_bench, and record throughput, latency, server CPU and
# conflation. Each config runs RUNS times; summarize_load.py reports the median.
#
#   scripts/load_test.sh [build_dir] [out_dir]
#
# Environment (defaults in brackets):
#   CONFIGS          ["10:10000 100:1000 500:200 1000:100"]  clients:updates_per_second
#   SECONDS_PER_RUN  [10]   measurement window per run
#   RUNS             [3]    repetitions per config
#   SERVER_THREADS   [2]    MD_SERVER_THREADS for the engine
#   CLIENT_THREADS   [2]    I/O threads in md_client_bench
set -euo pipefail

BUILD=${1:-build}
OUT=${2:-load-results}
CONFIGS=${CONFIGS:-"10:10000 100:1000 500:200 1000:100"}
DUR=${SECONDS_PER_RUN:-10}
RUNS=${RUNS:-3}
ST=${SERVER_THREADS:-2}
CT=${CLIENT_THREADS:-2}
PORT=${PORT:-18080}

ENGINE="$BUILD/deribit_engine"
BENCH="$BUILD/md_client_bench"
mkdir -p "$OUT"
CSV="$OUT/results.csv"
echo "clients,rate,run,connected,msgs_per_s,mb_per_s,p50_us,p90_us,p99_us,p999_us,max_us,msgs_out,conflated,slow_drops,server_cpu_pct" > "$CSV"

# Every client is a socket: lift the open-file limit as far as we're allowed to.
ulimit -n 65536 2>/dev/null || ulimit -n 8192 2>/dev/null || true

TIME_CMD=()
if [[ -x /usr/bin/time ]]; then TIME_CMD=(/usr/bin/time -f "%P" -o "$OUT/cpu.txt"); fi

{
    echo "host: $(uname -srm)"
    echo "cpus: $(nproc 2>/dev/null || echo '?')"
    if command -v lscpu > /dev/null; then lscpu | sed -n 's/^Model name:[[:space:]]*/cpu model: /p'; fi
    echo "server threads: $ST, client threads: $CT, window: ${DUR}s, runs: $RUNS"
} > "$OUT/machine.txt"
cat "$OUT/machine.txt"

for cfg in $CONFIGS; do
    clients=${cfg%%:*}
    rate=${cfg##*:}
    for run in $(seq 1 "$RUNS"); do
        rm -f "$OUT/run.csv" "$OUT/cpu.txt"
        # The engine must outlive the client: connect time + warm-up + window + margin.
        MD_SERVER_THREADS=$ST LOG_FILE="$OUT/engine.log" CONSOLE_LOG_LEVEL=error \
            "${TIME_CMD[@]}" "$ENGINE" --no-cli --duration $((DUR + 12)) --port "$PORT" \
            --synthetic "$rate" --env /nonexistent.env > "$OUT/engine.out" 2>&1 &
        engine_pid=$!
        sleep 2
        "$BENCH" --port "$PORT" --clients "$clients" --instrument SYN-PERP --seconds "$DUR" \
            --threads "$CT" --csv "$OUT/run.csv" > "$OUT/bench.out" 2>&1 || true
        wait "$engine_pid" || true

        # run.csv: connected,clients,msgs_per_s,mb_per_s,p50,p90,p99,p999,max
        row=$(cat "$OUT/run.csv" 2>/dev/null || echo "0,$clients,0,0,0,0,0,0,0")
        connected=$(echo "$row" | cut -d, -f1)
        metrics=$(echo "$row" | cut -d, -f3-)
        # Engine totals over its lifetime: messages written and updates conflated away.
        counters=$(sed -n 's/.*msgs_out=\([0-9]*\) bytes_out=[0-9]* conflated=\([0-9]*\) slow_drops=\([0-9]*\).*/\1,\2,\3/p' \
            "$OUT/engine.out" | tail -1)
        cpu=""
        if [[ -f "$OUT/cpu.txt" ]]; then cpu=$(tr -d '%' < "$OUT/cpu.txt"); fi
        echo "$clients,$rate,$run,$connected,$metrics,${counters:-,,},${cpu}" >> "$CSV"
        echo "clients=$clients rate=$rate run=$run -> $(tail -1 "$CSV")"
        PORT=$((PORT + 1))  # a fresh port per run avoids TIME_WAIT surprises
    done
done
echo "results: $CSV"
