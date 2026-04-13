#!/usr/bin/env python3
"""
cost_model.py — Converts experiment metrics CSV into a simulated AWS cost
comparison table across all 3 autoscaling strategies.

Pricing basis: AWS m5.xlarge on-demand, us-east-1  = $0.192 / hr
Each worker pod: 1 vCPU / 512 MiB  ≈ 1/4 of m5.xlarge  → $0.048 / hr

Usage:
    python3 experiment/cost_model.py experiment/results/<timestamp>-<mode>.csv
"""

import sys
import csv
from collections import defaultdict

# ---------------------------------------------------------------------------
# Pricing constants
# ---------------------------------------------------------------------------
M5_XLARGE_HOURLY = 0.192          # USD/hr for m5.xlarge on-demand (us-east-1)
VCPUS_PER_NODE   = 4              # m5.xlarge has 4 vCPUs
COST_PER_POD_PER_SECOND = M5_XLARGE_HOURLY / 3600 / VCPUS_PER_NODE

STRATEGY_ORDER = ["dynamic", "keda", "hpa"]
STRATEGY_LABELS = {
    "dynamic": "Dynamic Alloc",
    "keda":    "KEDA",
    "hpa":     "HPA",
}

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
def load_csv(path: str) -> list[dict]:
    rows = []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append({
                "strategy":      row["strategy"],
                "timestamp":     int(row["timestamp"]),
                "executor_count": int(row["executor_count"]),
                "cpu_millicores": int(row["cpu_millicores"]),
                "memory_mib":    int(row["memory_mib"]),
            })
    return rows

# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------
def analyse(rows: list[dict]) -> dict:
    by_strategy = defaultdict(list)
    for row in rows:
        by_strategy[row["strategy"]].append(row)

    results = {}
    for strategy, data in by_strategy.items():
        data.sort(key=lambda r: r["timestamp"])

        duration_s    = data[-1]["timestamp"] - data[0]["timestamp"]
        avg_executors = sum(r["executor_count"] for r in data) / len(data)
        max_executors = max(r["executor_count"] for r in data)
        avg_cpu       = sum(r["cpu_millicores"] for r in data) / len(data)
        peak_mem      = max(r["memory_mib"] for r in data)

        # Simulated cost: integrate (executor_count × cost_per_pod) over time
        cost = 0.0
        for i in range(1, len(data)):
            interval = data[i]["timestamp"] - data[i - 1]["timestamp"]
            cost += data[i - 1]["executor_count"] * COST_PER_POD_PER_SECOND * interval

        results[strategy] = {
            "duration_s":           duration_s,
            "avg_executors":        round(avg_executors, 2),
            "max_executors":        max_executors,
            "avg_cpu_millicores":   round(avg_cpu),
            "peak_memory_mib":      peak_mem,
            "simulated_cost_usd":   round(cost, 6),
        }
    return results

# ---------------------------------------------------------------------------
# Printing
# ---------------------------------------------------------------------------
def print_table(results: dict) -> None:
    metrics = [
        ("duration_s",         "Duration (s)",        "lower"),
        ("avg_executors",      "Avg Executors",        "lower"),
        ("max_executors",      "Max Executors",        "lower"),
        ("avg_cpu_millicores", "Avg CPU (millicores)", "lower"),
        ("peak_memory_mib",    "Peak Memory (MiB)",    "lower"),
        ("simulated_cost_usd", "Simulated Cost ($)",   "lower"),
    ]

    strategies = [s for s in STRATEGY_ORDER if s in results]
    col_w = 18

    # Header
    header = f"{'Metric':<26}" + "".join(f"{STRATEGY_LABELS.get(s, s):>{col_w}}" for s in strategies)
    print()
    print("=" * len(header))
    print(header)
    print("-" * len(header))

    for key, label, direction in metrics:
        values = {s: results[s][key] for s in strategies}
        best   = min(values, key=values.get) if direction == "lower" else max(values, key=values.get)

        def fmt(s: str) -> str:
            v = values[s]
            cell = f"★ {v}" if s == best else str(v)
            return f"{cell:>{col_w}}"

        print(f"{label:<26}" + "".join(fmt(s) for s in strategies))

    print("=" * len(header))
    print()
    print("★ = winner for that metric (lower is better for all metrics above)")
    print()

def print_hypothesis(results: dict) -> None:
    """Print a brief interpretation of the results."""
    strategies = list(results.keys())
    if len(strategies) < 2:
        return

    cheapest = min(strategies, key=lambda s: results[s]["simulated_cost_usd"])
    fastest  = min(strategies, key=lambda s: results[s]["duration_s"])
    leanest  = min(strategies, key=lambda s: results[s]["avg_executors"])

    print("--- Interpretation ---")
    print(f"  Most cost-efficient: {STRATEGY_LABELS.get(cheapest, cheapest)}")
    print(f"  Fastest job:         {STRATEGY_LABELS.get(fastest, fastest)}")
    print(f"  Fewest avg pods:     {STRATEGY_LABELS.get(leanest, leanest)}")
    print()

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 experiment/cost_model.py <results_csv>")
        sys.exit(1)

    csv_path = sys.argv[1]
    rows     = load_csv(csv_path)
    results  = analyse(rows)

    print_table(results)
    print_hypothesis(results)
