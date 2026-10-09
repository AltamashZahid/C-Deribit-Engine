#!/usr/bin/env bash
# One-shot benchmark on a fresh Ubuntu 24.04 VM (e.g. a London VM next to Deribit):
# installs dependencies, builds, measures network latency to Deribit Testnet, runs the
# live order benchmarks with your Testnet keys, then the market data load test.
# Everything is written to vm-results/ and printed at the end.
#
#   git clone https://github.com/AltamashZahid/C-Deribit-Engine.git && cd C-Deribit-Engine
#   bash scripts/vm_bench.sh
set -euo pipefail
cd "$(dirname "$0")/.."
OUT=vm-results
mkdir -p "$OUT"

echo "== 1/5 installing build tools (takes ~2 min)"
sudo apt-get update -qq
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq build-essential cmake ninja-build libboost-all-dev libssl-dev time curl > /dev/null

echo "== 2/5 building"
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release > /dev/null
cmake --build build -j "$(nproc)" > "$OUT/build.log" 2>&1 || { tail -30 "$OUT/build.log"; exit 1; }
./build/unit_tests > "$OUT/unit_tests.txt" 2>&1 && tail -1 "$OUT/unit_tests.txt"

if [[ ! -f .env ]]; then
    echo "== Deribit Testnet API key (stored only in ./.env on this VM)"
    read -r -p "   DERIBIT_CLIENT_ID: " cid
    read -r -s -p "   DERIBIT_CLIENT_SECRET (hidden): " secret
    echo
    printf 'DERIBIT_CLIENT_ID=%s\nDERIBIT_CLIENT_SECRET=%s\n' "$cid" "$secret" > .env
    chmod 600 .env
fi

echo "== 3/5 network latency to test.deribit.com (TCP connect, 10 samples)"
{
    echo "host: $(uname -srm), cpus: $(nproc), $(lscpu | sed -n 's/^Model name:[[:space:]]*//p')"
    echo "public ip region: $(curl -s --max-time 5 https://ipinfo.io/city 2>/dev/null || echo '?'), $(curl -s --max-time 5 https://ipinfo.io/country 2>/dev/null || echo '?')"
    for i in $(seq 1 10); do
        curl -s -o /dev/null -w '%{time_connect}\n' https://test.deribit.com/api/v2/public/test
    done | awk '{ms=$1*1000; s+=ms; if(min==""||ms<min)min=ms; if(ms>max)max=ms} END {printf "tcp connect to test.deribit.com: avg %.2f ms, min %.2f ms, max %.2f ms\n", s/NR, min, max}'
} | tee "$OUT/network.txt"

echo "== 4/5 live order benchmarks on Deribit Testnet (~2 min)"
(sleep 5; echo "bench orders BTC-PERPETUAL 50"; echo "bench loop BTC-PERPETUAL 50"; echo "orders"; echo "stats"; echo "quit") \
    | ./build/deribit_engine --port 18200 --env .env > "$OUT/live_bench.txt" 2>&1 || true
grep -E "bench setup|rtt|tick-to|socket write|iterations|open order|md parse|error" "$OUT/live_bench.txt" || cat "$OUT/live_bench.txt"

echo "== 5/5 market data load test (~6 min)"
CONFIGS="10:10000 100:1000 500:200 1000:100" RUNS=3 SERVER_THREADS=2 CLIENT_THREADS=2 \
    bash scripts/load_test.sh build "$OUT/load" > "$OUT/load.log" 2>&1
python3 scripts/summarize_load.py "$OUT/load/results.csv" "$OUT/load/machine.txt" | tee "$OUT/load_summary.md"

echo
echo "== done. Copy everything between the lines below back to the chat:"
echo "------------------------------------------------------------------"
cat "$OUT/network.txt"
grep -E "rtt|tick-to|socket write|iterations|md parse" "$OUT/live_bench.txt" || true
cat "$OUT/load_summary.md"
echo "------------------------------------------------------------------"
echo "Remember to DELETE the VM in the Azure portal when you're finished."
