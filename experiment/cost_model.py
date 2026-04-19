#!/usr/bin/env python3
"""
cost_model.py — Converts experiment metrics CSV into a simulated AWS cost
comparison table across all 3 autoscaling strategies.

Pricing basis: AWS m5.xlarge on-demand, us-east-1  = $0.192 / hr
  m5.xlarge: 4 vCPU, 16 GiB RAM

Pod sizes (per strategy, post memory fix):
  Strategy 1 — Dynamic Alloc : 1 vCPU / 512 MiB  → 1/4 vCPU share → $0.0480/hr
  Strategy 2 — KEDA          : 1 vCPU / 1 GiB    → 1/4 vCPU share → $0.0480/hr
  Strategy 3 — HPA           : 1 vCPU / 1 GiB    → 1/4 vCPU share → $0.0480/hr

Cost is modelled on vCPU fraction (dominant resource on m5.xlarge for Spark workloads).

Usage:
    python3 experiment/cost_model.py experiment/results/<timestamp>-<mode>.csv
"""

import sys
import csv
from collections import defaultdict

# ---------------------------------------------------------------------------
# Pricing constants (AWS m5.xlarge on-demand, us-east-1, April 2026)
# ---------------------------------------------------------------------------
M5_XLARGE_HOURLY = 0.192   # USD/hr
VCPUS_PER_NODE   = 4       # m5.xlarge has 4 vCPUs
RAM_GIB_PER_NODE = 16      # m5.xlarge has 16 GiB

# Each worker pod = 1 vCPU → 1/4 of the node's vCPUs
COST_PER_POD_HR  = M5_XLARGE_HOURLY / VCPUS_PER_NODE   # $0.0480/hr per pod
COST_PER_POD_SEC = COST_PER_POD_HR / 3600

# Pod memory budget per strategy (GiB) — for reference in output
POD_MEMORY = {
    "dynamic": 0.5,   # 512 MiB (Strategy 1 executors)
    "keda":    1.0,   # 1 GiB   (Strategy 2 workers, post fix)
    "hpa":     1.0,   # 1 GiB   (Strategy 3 workers, post fix)
}

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
                "strategy":       row["strategy"],
                "timestamp":      int(row["timestamp"]),
                "executor_count": int(row["executor_count"]),
                "cpu_millicores": int(row["cpu_millicores"]),
                "memory_mib":     int(row["memory_mib"]),
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
            cost += data[i - 1]["executor_count"] * COST_PER_POD_SEC * interval

        # Theoretical max cost (if always at max executors for full duration)
        theoretical_max_cost = max_executors * COST_PER_POD_SEC * duration_s

        results[strategy] = {
            "duration_s":             duration_s,
            "avg_executors":          round(avg_executors, 2),
            "max_executors":          max_executors,
            "avg_cpu_millicores":     round(avg_cpu),
            "peak_memory_mib":        peak_mem,
            "pod_memory_gib":         POD_MEMORY.get(strategy, "?"),
            "simulated_cost_usd":     round(cost, 6),
            "theoretical_max_cost":   round(theoretical_max_cost, 6),
            "cost_efficiency_pct":    round(
                (1 - cost / theoretical_max_cost) * 100, 1
            ) if theoretical_max_cost > 0 else 0,
        }
    return results

# ---------------------------------------------------------------------------
# Printing
# ---------------------------------------------------------------------------
def print_table(results: dict) -> None:
    strategies = [s for s in STRATEGY_ORDER if s in results]
    col_w = 18

    metrics = [
        ("duration_s",           "Duration (s)",          "lower"),
        ("avg_executors",        "Avg Executors",          "lower"),
        ("max_executors",        "Max Executors",          "lower"),
        ("avg_cpu_millicores",   "Avg CPU (millicores)",   "lower"),
        ("peak_memory_mib",      "Peak Memory (MiB)",      "lower"),
        ("pod_memory_gib",       "Pod Memory (GiB)",       None),
        ("simulated_cost_usd",   "Simulated Cost ($)",     "lower"),
        ("theoretical_max_cost", "Theoretical Max ($)",    None),
        ("cost_efficiency_pct",  "Cost Saving vs Max (%)", "higher"),
    ]

    header = f"{'Metric':<28}" + "".join(f"{STRATEGY_LABELS.get(s, s):>{col_w}}" for s in strategies)
    print()
    print("=" * len(header))
    print(f"  AWS Cost Model  |  m5.xlarge on-demand us-east-1  |  ${M5_XLARGE_HOURLY}/hr per node")
    print(f"  Pod cost: ${COST_PER_POD_HR:.4f}/hr each  |  1 vCPU / node's {VCPUS_PER_NODE} vCPUs")
    print("=" * len(header))
    print(header)
    print("-" * len(header))

    for key, label, direction in metrics:
        values = {s: results[s][key] for s in strategies}

        if direction is None:
            best = None
        elif direction == "lower":
            best = min(values, key=values.get)
        else:
            best = max(values, key=values.get)

        def fmt(s: str, _values: dict = values, _best: str = best) -> str:
            v = _values[s]
            cell = f"★ {v}" if s == _best else str(v)
            return f"{cell:>{col_w}}"

        print(f"{label:<28}" + "".join(fmt(s) for s in strategies))

    print("=" * len(header))
    print()
    print("★ = winner for that metric")
    print()

def print_warnings(rows: list[dict]) -> None:
    """Warn if CPU/memory data is all zeros (metrics-server not installed)."""
    all_cpu_zero = all(r["cpu_millicores"] == 0 for r in rows)
    all_mem_zero = all(r["memory_mib"] == 0 for r in rows)
    if all_cpu_zero or all_mem_zero:
        print()
        print("⚠️  WARNING: CPU/memory columns are all zero.")
        print("   kubectl top had no data — metrics-server may not be installed.")
        print("   Install it with:")
        print("   kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml")
        print()

def print_hypothesis(results: dict) -> None:
    strategies = list(results.keys())
    if len(strategies) < 2:
        return

    cheapest = min(strategies, key=lambda s: results[s]["simulated_cost_usd"])
    fastest  = min(strategies, key=lambda s: results[s]["duration_s"])
    leanest  = min(strategies, key=lambda s: results[s]["avg_executors"])
    most_eff = max(strategies, key=lambda s: results[s]["cost_efficiency_pct"])

    print("--- Interpretation ---")
    print(f"  Most cost-efficient:        {STRATEGY_LABELS.get(cheapest, cheapest)}")
    print(f"  Fastest job:                {STRATEGY_LABELS.get(fastest, fastest)}")
    print(f"  Fewest avg pods (leanest):  {STRATEGY_LABELS.get(leanest, leanest)}")
    print(f"  Best cost saving vs max:    {STRATEGY_LABELS.get(most_eff, most_eff)}")
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

    print_warnings(rows)

    results  = analyse(rows)
    print_table(results)
    print_hypothesis(results)
