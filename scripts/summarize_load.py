"""Turns load_test.sh's results.csv into a Markdown table (median of the runs per config).

    python scripts/summarize_load.py load-results/results.csv [machine.txt]
"""
import csv
import statistics
import sys
from collections import OrderedDict


def median(rows, key):
    values = [float(r[key]) for r in rows if r.get(key) not in (None, "")]
    return statistics.median(values) if values else None


def fmt_us(us):
    if us is None:
        return "-"
    return f"{us / 1000:.2f} ms" if us >= 1000 else f"{us:.0f} µs"


def main():
    path = sys.argv[1]
    machine = open(sys.argv[2]).read().strip() if len(sys.argv) > 2 else ""
    groups = OrderedDict()
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            groups.setdefault((int(row["clients"]), int(row["rate"])), []).append(row)

    lines = ["## Market data server load test", ""]
    if machine:
        lines += ["```", machine, "```", ""]
    lines += [
        "| Clients | Feed (upd/s) | Wanted msg/s | Delivered msg/s | MB/s | p50 | p99 | p99.9 | Server CPU | Conflated | Dropped |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for (clients, rate), rows in groups.items():
        delivered = median(rows, "msgs_per_s") or 0
        drops = max(int(r["slow_drops"] or 0) for r in rows)
        connected = min(int(r["connected"] or 0) for r in rows)
        cpu = median(rows, "server_cpu_pct")
        wanted = clients * rate
        # Share of per-client updates superseded by a newer snapshot before they were written,
        # from the engine's lifetime totals (written + conflated = updates offered to clients).
        shares = [
            100 * float(r["conflated"]) / (float(r["conflated"]) + float(r["msgs_out"]))
            for r in rows
            if r.get("conflated") and r.get("msgs_out") and float(r["conflated"]) + float(r["msgs_out"]) > 0
        ]
        conf_pct = f"{statistics.median(shares):.0f}%" if shares else "-"
        clients_text = str(clients) if connected == clients else f"{connected}/{clients}"
        lines.append(
            f"| {clients_text} | {rate:,} | {wanted:,} | {delivered:,.0f} | {median(rows, 'mb_per_s') or 0:.1f} "
            f"| {fmt_us(median(rows, 'p50_us'))} | {fmt_us(median(rows, 'p99_us'))} | {fmt_us(median(rows, 'p999_us'))} "
            f"| {f'{cpu:.0f}%' if cpu is not None else '-'} | {conf_pct} | {drops} |"
        )
    lines += [
        "",
        f"Median of {len(next(iter(groups.values())))} runs per row. Latency is engine receive → client receive "
        "(same host, steady clock). Server CPU is the engine process (100% = one core). "
        "Conflated = updates superseded by a newer snapshot before they could be written to a slow client.",
    ]
    sys.stdout.reconfigure(encoding="utf-8")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
