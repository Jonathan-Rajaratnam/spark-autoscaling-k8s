#!/usr/bin/env python3
"""
cost_model.py
Converts per-strategy resource time-series into simulated AWS on-demand cost figures.
Pricing baseline: AWS m5.xlarge ($0.192/hr, us-east-1)

Usage:
    python3 experiment/cost_model.py experiment/results/<timestamp>-<mode>.csv
"""

import csv
import os
import sys
from dataclasses import dataclass
from typing import List


# ---------------------------------------------------------------------------
# Pricing constants (AWS m5.xlarge on-demand, us-east-1, April 2026)
# ---------------------------------------------------------------------------
M5_XLARGE_HOURLY = 0.192          # USD per hour
M5_XLARGE_VCPU   = 4              # vCPUs per node
M5_XLARGE_MEMORY = 16384          # MiB per node
SECONDS_PER_HOUR = 3600

STRATEGY_ORDER = ["dynamic", "keda", "hpa", "dra"]
STRATEGY_LABELS = {
    "dynamic": "Dynamic Alloc",
    "dra":     "Dynamic Alloc",
    "keda":    "KEDA",
    "hpa":     "HPA",
}

@dataclass
class MetricRow:
    """Represents one captured metric interval from the experiment harness."""
    timestamp:      int     # Unix epoch seconds
    strategy:       str     # "dra" | "keda" | "hpa" | "dynamic"
    executor_count: int
    cpu_millicores: float   # total across all executors
    memory_mib:     float   # total across all executors


@dataclass
class CostSummary:
    strategy:               str
    duration_seconds:       float
    avg_executor_count:     float
    avg_cpu_millicores:     float
    avg_memory_mib:         float
    peak_executor_count:    int
    peak_cpu_millicores:    float
    simulated_cost_usd:     float
    cpu_efficiency_pct:     float   # fraction of provisioned CPU actually used
    memory_efficiency_pct:  float   # fraction of provisioned memory actually used


# ---------------------------------------------------------------------------
# Core Analysis Functions
# ---------------------------------------------------------------------------

def cost_per_second(cpu_millicores: float, memory_mib: float) -> float:
    """
    Compute instantaneous simulated cost (USD/second) for a given resource snapshot.

    Proportional billing: cost = max(cpu_fraction, memory_fraction) * node_cost_per_second
    where fractions are relative to one m5.xlarge node (4 vCPU / 16 GiB).
    """
    if cpu_millicores < 0:
        raise ValueError(f"cpu_millicores must be >= 0, got {cpu_millicores}")
    if memory_mib < 0:
        raise ValueError(f"memory_mib must be >= 0, got {memory_mib}")

    node_cost_per_second = M5_XLARGE_HOURLY / SECONDS_PER_HOUR

    cpu_fraction    = cpu_millicores / (M5_XLARGE_VCPU * 1000)
    memory_fraction = memory_mib     / M5_XLARGE_MEMORY

    # Dominant-resource billing: whichever resource drives more node usage
    resource_fraction = max(cpu_fraction, memory_fraction)
    return resource_fraction * node_cost_per_second


def compute_cost_summary(rows: List[MetricRow], interval_seconds: int = 10) -> CostSummary:
    """
    Aggregate a list of MetricRow snapshots into a CostSummary for one strategy run.
    """
    if not rows:
        raise ValueError("Cannot compute cost summary for empty metric series.")

    strategies = {r.strategy for r in rows}
    if len(strategies) > 1:
        raise ValueError(
            f"All rows must belong to the same strategy. Found: {strategies}"
        )

    strategy = rows[0].strategy
    n = len(rows)

    total_cpu    = sum(r.cpu_millicores for r in rows)
    total_memory = sum(r.memory_mib     for r in rows)
    total_exec   = sum(r.executor_count for r in rows)

    avg_cpu    = total_cpu    / n
    avg_memory = total_memory / n
    avg_exec   = total_exec   / n

    peak_exec = max(r.executor_count  for r in rows)
    peak_cpu  = max(r.cpu_millicores  for r in rows)

    # Simulated cost: sum of per-interval costs
    simulated_cost = sum(
        cost_per_second(r.cpu_millicores, r.memory_mib) * interval_seconds
        for r in rows
    )

    duration = n * interval_seconds

    # Efficiency: average used vs. peak provisioned capacity
    provisioned_cpu_millicores = peak_exec * 1000   # 1 vCPU per executor
    provisioned_memory_mib     = peak_exec * 512    # 512 MiB per executor (approximated for all if 1 GiB wasn't requested effectively initially)
    # The new app.py has pod_memory 512 or 1024, but keeping 512 as an estimation baseline for the efficiency ratio
    
    cpu_efficiency    = (avg_cpu    / provisioned_cpu_millicores * 100) if provisioned_cpu_millicores > 0 else 0.0
    memory_efficiency = (avg_memory / provisioned_memory_mib     * 100) if provisioned_memory_mib     > 0 else 0.0

    return CostSummary(
        strategy               = strategy,
        duration_seconds       = duration,
        avg_executor_count     = avg_exec,
        avg_cpu_millicores     = avg_cpu,
        avg_memory_mib         = avg_memory,
        peak_executor_count    = peak_exec,
        peak_cpu_millicores    = peak_cpu,
        simulated_cost_usd     = simulated_cost,
        cpu_efficiency_pct     = cpu_efficiency,
        memory_efficiency_pct  = memory_efficiency,
    )


def load_csv(filepath: str) -> List[MetricRow]:
    """
    Load a results CSV produced by run_experiment.sh into a list of MetricRow objects.
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Results file not found: {filepath}")

    rows: List[MetricRow] = []
    with open(filepath, newline="") as fh:
        reader = csv.DictReader(fh)
        required = {"timestamp", "strategy", "executor_count", "cpu_millicores", "memory_mib"}
        if not required.issubset(set(reader.fieldnames or [])):
            raise ValueError(
                f"CSV missing required columns. Expected {required}, "
                f"got {set(reader.fieldnames or [])}"
            )
        for line in reader:
            rows.append(MetricRow(
                timestamp      = int(line["timestamp"]),
                strategy       = line["strategy"].strip().lower(),
                executor_count = int(line["executor_count"]),
                cpu_millicores = float(line["cpu_millicores"]),
                memory_mib     = float(line["memory_mib"]),
            ))
    return rows


def compare_strategies(results_csv: str) -> List[CostSummary]:
    """
    Load a combined results CSV and return one CostSummary per strategy, sorted by cost.
    """
    rows = load_csv(results_csv)

    by_strategy: dict = {}
    for row in rows:
        by_strategy.setdefault(row.strategy, []).append(row)

    summaries = [
        compute_cost_summary(strategy_rows)
        for strategy_rows in by_strategy.values()
    ]

    return sorted(summaries, key=lambda s: s.simulated_cost_usd)

# ---------------------------------------------------------------------------
# CLI Output Formatting
# ---------------------------------------------------------------------------
def print_warnings(rows: List[MetricRow]) -> None:
    """Warn if CPU/memory data is all zeros (metrics-server not installed)."""
    all_cpu_zero = all(r.cpu_millicores == 0 for r in rows)
    all_mem_zero = all(r.memory_mib == 0 for r in rows)
    if all_cpu_zero or all_mem_zero:
        print()
        print("⚠️  WARNING: CPU/memory columns are all zero.")
        print("   kubectl top had no data — metrics-server may not be installed.")
        print("   Install it with:")
        print("   kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml")
        print()


def print_table(summaries: List[CostSummary]) -> None:
    strategies_present = [s.strategy for s in summaries]
    # Enforce order for column headers based on our preferred array, if present
    ordered_summaries = []
    for s_name in STRATEGY_ORDER:
        matching = [s for s in summaries if s.strategy == s_name]
        if matching:
            ordered_summaries.append(matching[0])
            
    col_w = 18
    metrics = [
        ("duration_seconds",      "Duration (s)",          "lower"),
        ("avg_executor_count",    "Avg Executors",          "lower"),
        ("peak_executor_count",   "Peak Executors",         "lower"),
        ("avg_cpu_millicores",    "Avg CPU (millicores)",   "lower"),
        ("avg_memory_mib",        "Avg Memory (MiB)",       "lower"),
        ("simulated_cost_usd",    "Simulated Cost ($)",     "lower"),
        ("cpu_efficiency_pct",    "CPU Efficiency (%)",     "higher"),
        ("memory_efficiency_pct", "Memory Efficiency (%)",  "higher"),
    ]

    header = f"{'Metric':<28}" + "".join(f"{STRATEGY_LABELS.get(s.strategy, s.strategy):>{col_w}}" for s in ordered_summaries)
    print()
    print("=" * len(header))
    print(f"  AWS Cost Model  |  m5.xlarge on-demand us-east-1  |  ${M5_XLARGE_HOURLY}/hr per node")
    print(f"  Cost uses dominant-resource billing (max of CPU/Memory fractional use of Node)")
    print("=" * len(header))
    print(header)
    print("-" * len(header))

    for key, label, direction in metrics:
        values = {s.strategy: round(getattr(s, key), 4) if isinstance(getattr(s, key), float) else getattr(s, key) for s in ordered_summaries}

        if direction is None:
            best = None
        elif direction == "lower":
            best = min(values, key=values.get)
        else:
            best = max(values, key=values.get)

        def fmt(s_name: str) -> str:
            v = values[s_name]
            cell = f"★ {v}" if s_name == best else str(v)
            return f"{cell:>{col_w}}"

        print(f"{label:<28}" + "".join(fmt(s.strategy) for s in ordered_summaries))

    print("=" * len(header))
    print()
    print("★ = winner for that metric")
    print()


def print_hypothesis(summaries: List[CostSummary]) -> None:
    if len(summaries) < 2:
        return

    cheapest = min(summaries, key=lambda s: s.simulated_cost_usd)
    fastest  = min(summaries, key=lambda s: s.duration_seconds)
    leanest  = min(summaries, key=lambda s: s.avg_executor_count)
    most_eff = max(summaries, key=lambda s: s.cpu_efficiency_pct)

    print("--- Interpretation ---")
    print(f"  Most cost-efficient:        {STRATEGY_LABELS.get(cheapest.strategy, cheapest.strategy)}")
    print(f"  Fastest job:                {STRATEGY_LABELS.get(fastest.strategy, fastest.strategy)}")
    print(f"  Fewest avg pods (leanest):  {STRATEGY_LABELS.get(leanest.strategy, leanest.strategy)}")
    print(f"  Best CPU efficiency:        {STRATEGY_LABELS.get(most_eff.strategy, most_eff.strategy)}")
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

    summaries = compare_strategies(csv_path)
    print_table(summaries)
    print_hypothesis(summaries)
