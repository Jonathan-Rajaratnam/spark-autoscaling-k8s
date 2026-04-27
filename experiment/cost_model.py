#!/usr/bin/env python3
"""
cost_model.py
Converts per-strategy resource time-series into simulated AWS on-demand cost figures.
Pricing baseline: AWS m7i-flex.large ($0.09576/hr, us-east-1)

The simulated cost reflects EC2 worker-node pricing only. It excludes the
fixed Amazon EKS control-plane fee and any EKS Auto Mode surcharge.

Usage:
    python3 experiment/cost_model.py experiment/results/<timestamp>-<mode>.csv
"""

import csv
import os
import sys
from dataclasses import dataclass
from typing import List, Optional


# ---------------------------------------------------------------------------
# Pricing constants (AWS m7i-flex.large on-demand, us-east-1, April 2026)
# ---------------------------------------------------------------------------
M7I_FLEX_LARGE_HOURLY = 0.09576   # USD per hour
M7I_FLEX_LARGE_VCPU   = 2         # vCPUs per node
M7I_FLEX_LARGE_MEMORY = 8192      # MiB per node
SECONDS_PER_HOUR = 3600

STRATEGY_ORDER = ["dynamic", "keda", "hpa", "dra"]
STRATEGY_LABELS = {
    "dynamic": "Dynamic Alloc",
    "dra":     "Dynamic Alloc",
    "keda":    "KEDA",
    "hpa":     "HPA",
}

STRATEGY_ALIASES = {
    "dynamic": "dra",
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
    scale_up_latency_seconds: Optional[float]
    scale_down_time_seconds: Optional[float]
    avg_executor_count:     float
    avg_cpu_utilisation_pct: float
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
    where fractions are relative to one m7i-flex.large node (2 vCPU / 8 GiB).
    """
    if cpu_millicores < 0:
        raise ValueError(f"cpu_millicores must be >= 0, got {cpu_millicores}")
    if memory_mib < 0:
        raise ValueError(f"memory_mib must be >= 0, got {memory_mib}")

    node_cost_per_second = M7I_FLEX_LARGE_HOURLY / SECONDS_PER_HOUR

    cpu_fraction    = cpu_millicores / (M7I_FLEX_LARGE_VCPU * 1000)
    memory_fraction = memory_mib     / M7I_FLEX_LARGE_MEMORY

    # Dominant-resource billing: whichever resource drives more node usage
    resource_fraction = max(cpu_fraction, memory_fraction)
    return resource_fraction * node_cost_per_second


def _first_positive_executor_index(rows: List[MetricRow]) -> Optional[int]:
    for idx, row in enumerate(rows):
        if row.executor_count > 0:
            return idx
    return None


def _scale_up_latency_seconds(rows: List[MetricRow]) -> Optional[float]:
    baseline_idx = _first_positive_executor_index(rows)
    if baseline_idx is None:
        return None

    baseline_exec = rows[baseline_idx].executor_count
    baseline_ts = rows[baseline_idx].timestamp

    for row in rows[baseline_idx + 1:]:
        if row.executor_count > baseline_exec:
            return float(row.timestamp - baseline_ts)
    return None


def _scale_down_time_seconds(rows: List[MetricRow], peak_exec: int) -> Optional[float]:
    peak_idx = None
    for idx, row in enumerate(rows):
        if row.executor_count == peak_exec:
            peak_idx = idx

    if peak_idx is None:
        return None

    peak_ts = rows[peak_idx].timestamp
    for row in rows[peak_idx + 1:]:
        if row.executor_count < peak_exec:
            return float(row.timestamp - peak_ts)
    return None


def _average_cpu_utilisation_pct(rows: List[MetricRow]) -> float:
    samples = []
    for row in rows:
        if row.executor_count <= 0:
            continue
        provisioned_cpu = row.executor_count * 1000
        utilisation = (row.cpu_millicores / provisioned_cpu) * 100
        samples.append(max(0.0, min(utilisation, 100.0)))

    if not samples:
        return 0.0
    return sum(samples) / len(samples)


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

    ordered_rows = sorted(rows, key=lambda row: row.timestamp)
    strategy = ordered_rows[0].strategy
    n = len(ordered_rows)

    total_cpu    = sum(r.cpu_millicores for r in ordered_rows)
    total_memory = sum(r.memory_mib     for r in ordered_rows)
    total_exec   = sum(r.executor_count for r in ordered_rows)

    avg_cpu    = total_cpu    / n
    avg_memory = total_memory / n
    avg_exec   = total_exec   / n

    peak_exec = max(r.executor_count  for r in ordered_rows)
    peak_cpu  = max(r.cpu_millicores  for r in ordered_rows)
    scale_up_latency = _scale_up_latency_seconds(ordered_rows)
    scale_down_time = _scale_down_time_seconds(ordered_rows, peak_exec)
    avg_cpu_utilisation = _average_cpu_utilisation_pct(ordered_rows)

    # Simulated cost: sum of per-interval costs.
    # When Prometheus samples are unavailable but executor counts were captured,
    # fall back to provisioned executor resources so the run still has a
    # meaningful, non-zero cost baseline.
    simulated_cost = sum(
        cost_per_second(
            r.cpu_millicores if (r.cpu_millicores > 0 or r.memory_mib > 0) else r.executor_count * 1000,
            r.memory_mib if (r.cpu_millicores > 0 or r.memory_mib > 0) else r.executor_count * 512,
        ) * interval_seconds
        for r in ordered_rows
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
        scale_up_latency_seconds = scale_up_latency,
        scale_down_time_seconds  = scale_down_time,
        avg_executor_count     = avg_exec,
        avg_cpu_utilisation_pct = avg_cpu_utilisation,
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
        for row_num, line in enumerate(reader, start=2):
            try:
                rows.append(MetricRow(
                    timestamp      = int(line["timestamp"]),
                    strategy       = STRATEGY_ALIASES.get(
                        line["strategy"].strip().lower(),
                        line["strategy"].strip().lower(),
                    ),
                    executor_count = int(line["executor_count"]),
                    cpu_millicores = float(line["cpu_millicores"]),
                    memory_mib     = float(line["memory_mib"]),
                ))
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"Invalid numeric data in {filepath} at row {row_num}: {line}"
                ) from exc
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
    ordered_summaries = []
    for s_name in STRATEGY_ORDER:
        matching = [s for s in summaries if s.strategy == s_name]
        if matching:
            ordered_summaries.append(matching[0])
            
    col_w = 18
    metrics = [
        ("scale_up_latency_seconds", "Scale-up Latency (s)", "lower"),
        ("peak_executor_count",   "Peak Executors",         "lower"),
        ("avg_cpu_utilisation_pct", "Avg CPU Util (%)",     "higher"),
        ("scale_down_time_seconds", "Scale-down Time (s)",  "lower"),
        ("simulated_cost_usd",    "Simulated Cost ($)",     "lower"),
    ]

    header = f"{'Metric':<28}" + "".join(f"{STRATEGY_LABELS.get(s.strategy, s.strategy):>{col_w}}" for s in ordered_summaries)
    print()
    print("=" * len(header))
    print(f"  AWS Cost Model  |  m7i-flex.large on-demand us-east-1  |  ${M7I_FLEX_LARGE_HOURLY}/hr per node")
    print("  EC2 worker-node pricing only; excludes EKS control-plane and Auto Mode fees")
    print(f"  Cost uses dominant-resource billing (max of CPU/Memory fractional use of Node)")
    print("=" * len(header))
    print(header)
    print("-" * len(header))

    for key, label, direction in metrics:
        values = {
            s.strategy: (
                round(getattr(s, key), 4)
                if isinstance(getattr(s, key), float) and getattr(s, key) is not None
                else getattr(s, key)
            )
            for s in ordered_summaries
        }

        comparable = {name: value for name, value in values.items() if value is not None}

        if direction is None or not comparable:
            best = None
        elif direction == "lower":
            best = min(comparable, key=comparable.get)
        else:
            best = max(comparable, key=comparable.get)

        def fmt(s_name: str) -> str:
            v = values[s_name]
            text = "N/A" if v is None else str(v)
            cell = f"★ {text}" if s_name == best else text
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
    most_eff = max(summaries, key=lambda s: s.avg_cpu_utilisation_pct)

    print("--- Interpretation ---")
    print(f"  Most cost-efficient:        {STRATEGY_LABELS.get(cheapest.strategy, cheapest.strategy)}")
    print(f"  Fastest job:                {STRATEGY_LABELS.get(fastest.strategy, fastest.strategy)}")
    print(f"  Fewest avg pods (leanest):  {STRATEGY_LABELS.get(leanest.strategy, leanest.strategy)}")
    print(f"  Highest avg CPU util:       {STRATEGY_LABELS.get(most_eff.strategy, most_eff.strategy)}")
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
