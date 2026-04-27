"""
tests/performance/test_scaling_behaviour.py

Performance and system-level tests that analyse experiment harness output CSVs.
These tests validate that:
  - Scaling events occurred within expected latency bounds
  - Scale-down was observed during the 120-second idle window
  - Cost figures are within plausible bounds for the workload duration
  - No strategy exceeded the replica cap of 3

These tests operate on CSV files produced by run_experiment.sh and therefore
do NOT require a live Kubernetes cluster. They are skipped automatically
when no results CSV is present.

Requirements covered:
  FR2  – Benchmark modes generate autoscaler pressure (scaling events observed)
  FR5  – Experiment harness captures metrics at 10-second intervals
  FR11 – Scale-up occurs within expected latency for each strategy
  FR12 – Scale-down is observed during idle window
  FR13 – No strategy exceeds the 3-replica cap

Run with:  pytest tests/performance/test_scaling_behaviour.py -v
"""

import os
import sys
from collections import defaultdict
from typing import Dict, List, Optional

import pytest

exp_dir = os.path.join(os.path.dirname(__file__), "../../experiment")
sys.path.insert(0, os.path.abspath(exp_dir))
from cost_model import MetricRow, compute_cost_summary, load_csv

# Paths where run_experiment.sh writes results
RESULTS_SEARCH_PATHS = [
    "experiment/results",
    os.path.join(os.path.dirname(__file__), "../../experiment/results"),
    "/home/claude/experiment/results",
]

CAPTURE_INTERVAL_SEC = 10       # run_experiment.sh polling interval
MAX_REPLICAS         = 3        # executor/replica cap across all strategies
SCALE_DOWN_WINDOW    = 120      # seconds of idle sleep after computation
# Max expected row-count during scale-down window
SCALEDOWN_ROWS       = SCALE_DOWN_WINDOW // CAPTURE_INTERVAL_SEC  # 12 rows

# Latency thresholds (generous to account for polling jitter)
DRA_SCALEUP_LATENCY_ROWS  = 3    # DRA should scale within 3 intervals (30s)
KEDA_SCALEUP_LATENCY_ROWS = 4    # KEDA polling at 15s; 4 intervals = 40s max
HPA_SCALEUP_LATENCY_ROWS  = 4    # HPA polling at 15s; same as KEDA

# Simulated cost sanity bounds (USD) for a ~10-minute benchmark run
MIN_EXPECTED_COST_USD = 0.0001
MAX_EXPECTED_COST_USD = 1.00


# ---------------------------------------------------------------------------
# Fixture: locate and load results CSV
# ---------------------------------------------------------------------------

def _find_results_csv() -> Optional[str]:
    for search in RESULTS_SEARCH_PATHS:
        if os.path.isdir(search):
            csvs = sorted([
                f for f in os.listdir(search) if f.endswith(".csv")
            ], reverse=True)
            for csv_name in csvs:
                candidate = os.path.join(search, csv_name)
                try:
                    rows = load_csv(candidate)
                except (FileNotFoundError, ValueError):
                    continue
                strategies = {row.strategy for row in rows}
                if {"dra", "keda", "hpa"}.issubset(strategies):
                    return candidate
    return None


@pytest.fixture(scope="module")
def results_csv_path():
    path = _find_results_csv()
    if path is None:
        pytest.skip(
            "No complete experiment results CSV found. "
            "Run ./experiment/run_experiment.sh to generate results."
        )
    return path


@pytest.fixture(scope="module")
def rows_by_strategy(results_csv_path) -> Dict[str, List[MetricRow]]:
    all_rows = load_csv(results_csv_path)
    grouped: Dict[str, List[MetricRow]] = defaultdict(list)
    for row in all_rows:
        grouped[row.strategy].append(row)
    # Sort each strategy's rows by timestamp
    for strat in grouped:
        grouped[strat].sort(key=lambda r: r.timestamp)
    return dict(grouped)


# ---------------------------------------------------------------------------
# TC-P01  Harness output structure
# ---------------------------------------------------------------------------
class TestHarnessOutputStructure:

    def test_results_csv_contains_all_three_strategies(self, rows_by_strategy):
        """TC-P01 | FR5 | Results CSV must contain rows for all three strategies."""
        missing = {"dra", "keda", "hpa"} - set(rows_by_strategy.keys())
        assert not missing, \
            f"Strategies missing from results CSV: {missing}"

    def test_minimum_row_count_per_strategy(self, rows_by_strategy):
        """TC-P02 | FR5 | Each strategy must have at least 30 rows (5-minute run)."""
        for strategy, rows in rows_by_strategy.items():
            assert len(rows) >= 30, (
                f"Strategy '{strategy}' has only {len(rows)} rows. "
                "Expected >= 30 (300 seconds at 10-second intervals)."
            )

    def test_capture_interval_is_10_seconds(self, rows_by_strategy):
        """TC-P03 | FR5 | Rows should be spaced approximately 10 seconds apart."""
        for strategy, rows in rows_by_strategy.items():
            if len(rows) < 2:
                continue
            gaps = [
                rows[i + 1].timestamp - rows[i].timestamp
                for i in range(min(10, len(rows) - 1))
            ]
            avg_gap = sum(gaps) / len(gaps)
            assert 8 <= avg_gap <= 15, (
                f"Strategy '{strategy}': average timestamp gap is {avg_gap:.1f}s, "
                f"expected ~10s. Full gaps: {gaps}"
            )

    def test_no_negative_cpu_values(self, rows_by_strategy):
        """TC-P04 | FR5 | No row should contain a negative CPU value."""
        for strategy, rows in rows_by_strategy.items():
            negatives = [r for r in rows if r.cpu_millicores < 0]
            assert not negatives, \
                f"Strategy '{strategy}': {len(negatives)} rows with negative CPU"

    def test_no_negative_memory_values(self, rows_by_strategy):
        """TC-P05 | FR5 | No row should contain a negative memory value."""
        for strategy, rows in rows_by_strategy.items():
            negatives = [r for r in rows if r.memory_mib < 0]
            assert not negatives, \
                f"Strategy '{strategy}': {len(negatives)} rows with negative memory"


# ---------------------------------------------------------------------------
# TC-P06  Replica cap enforcement
# ---------------------------------------------------------------------------
class TestReplicaCap:

    def test_dra_never_exceeds_3_executors(self, rows_by_strategy):
        """TC-P06 | FR13 | DRA executor count must never exceed 3."""
        rows = rows_by_strategy.get("dra", [])
        violations = [r for r in rows if r.executor_count > MAX_REPLICAS]
        assert not violations, (
            f"DRA exceeded {MAX_REPLICAS} executors in "
            f"{len(violations)} rows: "
            f"{[r.executor_count for r in violations]}"
        )

    def test_keda_never_exceeds_3_replicas(self, rows_by_strategy):
        """TC-P07 | FR13 | KEDA worker replica count must never exceed 3."""
        rows = rows_by_strategy.get("keda", [])
        violations = [r for r in rows if r.executor_count > MAX_REPLICAS]
        assert not violations, (
            f"KEDA exceeded {MAX_REPLICAS} replicas in {len(violations)} rows"
        )

    def test_hpa_never_exceeds_3_replicas(self, rows_by_strategy):
        """TC-P08 | FR13 | HPA worker replica count must never exceed 3."""
        rows = rows_by_strategy.get("hpa", [])
        violations = [r for r in rows if r.executor_count > MAX_REPLICAS]
        assert not violations, (
            f"HPA exceeded {MAX_REPLICAS} replicas in {len(violations)} rows"
        )

    def test_all_strategies_maintain_min_1_executor(self, rows_by_strategy):
        """TC-P09 | FR13 | All strategies must maintain at least 1 executor during the run."""
        for strategy, rows in rows_by_strategy.items():
            # Allow 0 during scale-down window only — not during active workload
            active_rows = rows[:len(rows) - SCALEDOWN_ROWS]
            zero_rows = [r for r in active_rows if r.executor_count < 1]
            assert not zero_rows, (
                f"Strategy '{strategy}': executor count dropped below 1 "
                f"during active workload phase ({len(zero_rows)} occurrences)"
            )


# ---------------------------------------------------------------------------
# TC-P10  Scale-up detection
# ---------------------------------------------------------------------------
class TestScaleUpBehaviour:

    def _first_scaleup_row(self, rows: List[MetricRow]) -> Optional[int]:
        """Return the index of the first row where executor_count > 1."""
        for i, r in enumerate(rows):
            if r.executor_count > 1:
                return i
        return None

    def test_dra_scales_up_during_benchmark(self, rows_by_strategy):
        """TC-P10 | FR2 | DRA must scale to >1 executor during the benchmark."""
        rows = rows_by_strategy.get("dra", [])
        idx = self._first_scaleup_row(rows)
        assert idx is not None, \
            ("DRA never scaled above 1 executor. "
             "Benchmark may not have generated sufficient backlog, "
             "or DRA configuration may be incorrect.")

    def test_keda_scales_up_during_benchmark(self, rows_by_strategy):
        """TC-P11 | FR2 | KEDA must scale to >1 worker during the benchmark."""
        rows = rows_by_strategy.get("keda", [])
        idx = self._first_scaleup_row(rows)
        assert idx is not None, \
            ("KEDA never scaled above 1 worker. "
             "JVM heap or CPU thresholds may not have been breached.")

    def test_hpa_scales_up_during_benchmark(self, rows_by_strategy):
        """TC-P12 | FR2 | HPA must scale to >1 worker during the benchmark."""
        rows = rows_by_strategy.get("hpa", [])
        idx = self._first_scaleup_row(rows)
        assert idx is not None, \
            ("HPA never scaled above 1 worker. "
             "Custom metrics may not have been registered, or thresholds not breached.")

    def test_keda_scaleup_latency_within_bound(self, rows_by_strategy):
        """TC-P13 | FR11 | KEDA scale-up should occur within 40 seconds of job start."""
        rows = rows_by_strategy.get("keda", [])
        idx = self._first_scaleup_row(rows)
        if idx is None:
            pytest.skip("KEDA scale-up not observed")
        assert idx <= KEDA_SCALEUP_LATENCY_ROWS, (
            f"KEDA scale-up first observed at row {idx} "
            f"({idx * CAPTURE_INTERVAL_SEC}s), "
            f"expected within {KEDA_SCALEUP_LATENCY_ROWS * CAPTURE_INTERVAL_SEC}s"
        )

    def test_hpa_scaleup_latency_within_bound(self, rows_by_strategy):
        """TC-P14 | FR11 | HPA scale-up should occur within 40 seconds of job start."""
        rows = rows_by_strategy.get("hpa", [])
        idx = self._first_scaleup_row(rows)
        if idx is None:
            pytest.skip("HPA scale-up not observed")
        assert idx <= HPA_SCALEUP_LATENCY_ROWS, (
            f"HPA scale-up first observed at row {idx} "
            f"({idx * CAPTURE_INTERVAL_SEC}s), "
            f"expected within {HPA_SCALEUP_LATENCY_ROWS * CAPTURE_INTERVAL_SEC}s"
        )


# ---------------------------------------------------------------------------
# TC-P15  Scale-down detection (120-second idle window)
# ---------------------------------------------------------------------------
class TestScaleDownBehaviour:

    def test_dra_scales_down_during_idle_window(self, rows_by_strategy):
        """TC-P15 | FR12 | DRA must release executors during the 120s idle window."""
        rows = rows_by_strategy.get("dra", [])
        if len(rows) <= SCALEDOWN_ROWS:
            pytest.skip("Insufficient rows to evaluate scale-down window")
        idle_rows = rows[-SCALEDOWN_ROWS:]
        min_during_idle = min(r.executor_count for r in idle_rows)
        assert min_during_idle < max(r.executor_count for r in rows), \
            ("DRA did not reduce executor count during idle window. "
             "executorIdleTimeout may be longer than SCALE_DOWN_SLEEP_SECONDS.")

    def test_keda_scales_down_during_idle_window(self, rows_by_strategy):
        """TC-P16 | FR12 | KEDA must reduce worker replicas during the 120s idle window."""
        rows = rows_by_strategy.get("keda", [])
        if len(rows) <= SCALEDOWN_ROWS:
            pytest.skip("Insufficient rows to evaluate scale-down window")
        idle_rows = rows[-SCALEDOWN_ROWS:]
        max_active = max(r.executor_count for r in rows[:-SCALEDOWN_ROWS])
        min_idle   = min(r.executor_count for r in idle_rows)
        assert min_idle < max_active, \
            ("KEDA did not scale down during idle window. "
             f"Peak active: {max_active}, min during idle: {min_idle}")

    def test_hpa_scales_down_during_idle_window(self, rows_by_strategy):
        """TC-P17 | FR12 | HPA must reduce worker replicas during the 120s idle window."""
        rows = rows_by_strategy.get("hpa", [])
        if len(rows) <= SCALEDOWN_ROWS:
            pytest.skip("Insufficient rows to evaluate scale-down window")
        idle_rows = rows[-SCALEDOWN_ROWS:]
        max_active = max(r.executor_count for r in rows[:-SCALEDOWN_ROWS])
        min_idle   = min(r.executor_count for r in idle_rows)
        assert min_idle < max_active, (
            f"HPA did not scale down during idle window. "
            f"Peak: {max_active}, idle min: {min_idle}. "
            f"Check 90s stabilisation window vs {SCALE_DOWN_WINDOW}s sleep."
        )


# ---------------------------------------------------------------------------
# TC-P18  Cost model plausibility
# ---------------------------------------------------------------------------
class TestCostModelPlausibility:

    def test_all_strategies_produce_positive_cost(self, rows_by_strategy):
        """TC-P18 | FR12 | Every strategy must produce a positive simulated cost."""
        for strategy, rows in rows_by_strategy.items():
            summary = compute_cost_summary(rows)
            assert summary.simulated_cost_usd > 0, \
                f"Strategy '{strategy}' produced zero cost"

    def test_cost_within_plausible_range(self, rows_by_strategy):
        """TC-P19 | FR12 | Simulated cost must be within plausible bounds for workload."""
        for strategy, rows in rows_by_strategy.items():
            summary = compute_cost_summary(rows)
            assert MIN_EXPECTED_COST_USD <= summary.simulated_cost_usd <= MAX_EXPECTED_COST_USD, (
                f"Strategy '{strategy}' cost ${summary.simulated_cost_usd:.6f} "
                f"outside plausible range "
                f"[${MIN_EXPECTED_COST_USD}, ${MAX_EXPECTED_COST_USD}]"
            )

    def test_three_executor_run_costs_more_than_one_executor_run(
            self, rows_by_strategy):
        """TC-P20 | FR12 | A run that peaked at 3 executors must cost more than one at 1."""
        # Find strategies that reached 3 vs those that stayed at 1
        costs = {}
        peaks = {}
        for strategy, rows in rows_by_strategy.items():
            summary = compute_cost_summary(rows)
            costs[strategy] = summary.simulated_cost_usd
            peaks[strategy] = summary.peak_executor_count

        peak_3_strategies = [s for s, p in peaks.items() if p == 3]
        peak_1_strategies = [s for s, p in peaks.items() if p == 1]

        if not peak_3_strategies or not peak_1_strategies:
            pytest.skip("Need at least one strategy at peak=3 and one at peak=1")

        min_cost_3 = min(costs[s] for s in peak_3_strategies)
        max_cost_1 = max(costs[s] for s in peak_1_strategies)
        assert min_cost_3 > max_cost_1, (
            f"Expected strategies that scaled to 3 replicas to cost more than "
            f"those staying at 1. Got: {costs}"
        )
