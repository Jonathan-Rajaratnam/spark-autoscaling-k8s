"""
tests/unit/test_cost_model.py

Unit tests for cost_model.py.
Requirements covered: FR5 (cost modelling), FR12 (quantitative comparison).

Run with:  pytest tests/unit/test_cost_model.py -v
"""

import csv
import os
import sys
import tempfile

import pytest

exp_dir = os.path.join(os.path.dirname(__file__), "../../experiment")
sys.path.insert(0, os.path.abspath(exp_dir))
from cost_model import (
    M5_XLARGE_HOURLY,
    SECONDS_PER_HOUR,
    CostSummary,
    MetricRow,
    compare_strategies,
    compute_cost_summary,
    cost_per_second,
    load_csv,
)


# ---------------------------------------------------------------------------
# TC-U01  cost_per_second: zero resource usage costs nothing
# ---------------------------------------------------------------------------
class TestCostPerSecond:

    def test_zero_resources_zero_cost(self):
        """TC-U01 | FR5 | Zero CPU and memory should produce zero cost."""
        assert cost_per_second(0.0, 0.0) == 0.0

    def test_full_node_cpu_costs_one_node_per_second(self):
        """TC-U02 | FR5 | 4000 millicores (full m5.xlarge) = 1 node/s cost."""
        expected = M5_XLARGE_HOURLY / SECONDS_PER_HOUR
        result = cost_per_second(4000.0, 0.0)
        assert abs(result - expected) < 1e-10, \
            f"Expected {expected:.10f}, got {result:.10f}"

    def test_half_node_cpu_halves_cost(self):
        """TC-U03 | FR5 | 2000 millicores = half a node = half the cost."""
        full  = cost_per_second(4000.0, 0.0)
        half  = cost_per_second(2000.0, 0.0)
        assert abs(half - full / 2) < 1e-10

    def test_dominant_resource_billing_memory(self):
        """TC-U04 | FR5 | Memory-dominant workload bills on memory fraction."""
        # 100 millicores CPU (2.5% of node) vs 8192 MiB memory (50% of node)
        # memory dominates → cost should equal 50% of node/s rate
        expected = (8192 / 16384) * (M5_XLARGE_HOURLY / SECONDS_PER_HOUR)
        result = cost_per_second(100.0, 8192.0)
        assert abs(result - expected) < 1e-10

    def test_dominant_resource_billing_cpu(self):
        """TC-U05 | FR5 | CPU-dominant workload bills on CPU fraction."""
        # 3000 millicores (75%) vs 512 MiB memory (3.1%)
        expected = (3000 / 4000) * (M5_XLARGE_HOURLY / SECONDS_PER_HOUR)
        result = cost_per_second(3000.0, 512.0)
        assert abs(result - expected) < 1e-10

    def test_negative_cpu_raises(self):
        """TC-U06 | FR5 | Negative CPU should raise ValueError."""
        with pytest.raises(ValueError, match="cpu_millicores must be >= 0"):
            cost_per_second(-1.0, 512.0)

    def test_negative_memory_raises(self):
        """TC-U07 | FR5 | Negative memory should raise ValueError."""
        with pytest.raises(ValueError, match="memory_mib must be >= 0"):
            cost_per_second(500.0, -1.0)

    def test_three_executor_cost_higher_than_one(self):
        """TC-U08 | FR5 | Three executors at full load should cost more than one."""
        cost_one   = cost_per_second(1000.0, 512.0)
        cost_three = cost_per_second(3000.0, 1536.0)
        assert cost_three > cost_one


# ---------------------------------------------------------------------------
# TC-U09  compute_cost_summary: basic aggregation correctness
# ---------------------------------------------------------------------------
class TestComputeCostSummary:

    def _make_rows(self, strategy: str, n: int = 6,
                   cpu: float = 1000.0, mem: float = 512.0,
                   exec_count: int = 1) -> list:
        return [
            MetricRow(timestamp=i * 10, strategy=strategy,
                      executor_count=exec_count,
                      cpu_millicores=cpu, memory_mib=mem)
            for i in range(n)
        ]

    def test_empty_rows_raises(self):
        """TC-U09 | FR5 | Empty metric series should raise ValueError."""
        with pytest.raises(ValueError, match="empty metric series"):
            compute_cost_summary([])

    def test_mixed_strategies_raises(self):
        """TC-U10 | FR5 | Mixed-strategy rows should raise ValueError."""
        rows = (
            self._make_rows("dra", n=3) +
            self._make_rows("keda", n=3)
        )
        with pytest.raises(ValueError, match="same strategy"):
            compute_cost_summary(rows)

    def test_duration_is_n_times_interval(self):
        """TC-U11 | FR5 | Duration = row_count × interval_seconds."""
        rows = self._make_rows("dra", n=12)
        summary = compute_cost_summary(rows, interval_seconds=10)
        assert summary.duration_seconds == 120

    def test_peak_executor_count_correct(self):
        """TC-U12 | FR5 | Peak executor count matches highest value in series."""
        rows = [
            MetricRow(0, "keda", 1, 500, 256),
            MetricRow(10, "keda", 3, 2800, 1400),
            MetricRow(20, "keda", 2, 1800, 900),
        ]
        summary = compute_cost_summary(rows)
        assert summary.peak_executor_count == 3

    def test_simulated_cost_is_positive(self):
        """TC-U13 | FR12 | Simulated cost must be > 0 for any non-zero workload."""
        rows = self._make_rows("hpa", n=30, cpu=2000.0, mem=1024.0)
        summary = compute_cost_summary(rows)
        assert summary.simulated_cost_usd > 0

    def test_higher_resource_usage_means_higher_cost(self):
        """TC-U14 | FR12 | Strategy with higher avg CPU should incur higher cost."""
        rows_low  = self._make_rows("dra",  n=30, cpu=500.0,  mem=256.0)
        rows_high = self._make_rows("keda", n=30, cpu=3000.0, mem=1400.0)
        low_cost  = compute_cost_summary(rows_low).simulated_cost_usd
        high_cost = compute_cost_summary(rows_high).simulated_cost_usd
        assert high_cost > low_cost, \
            "Higher resource usage must produce a higher simulated cost"

    def test_strategy_name_preserved(self):
        """TC-U15 | FR5 | Strategy name must be preserved in summary output."""
        rows = self._make_rows("hpa")
        summary = compute_cost_summary(rows)
        assert summary.strategy == "hpa"

    def test_scale_up_latency_detected_from_first_growth(self):
        """TC-U16 | FR12 | Scale-up latency should be time until executor count first increases."""
        rows = [
            MetricRow(0, "dra", 1, 500, 256),
            MetricRow(10, "dra", 1, 600, 260),
            MetricRow(20, "dra", 2, 1500, 700),
            MetricRow(30, "dra", 2, 1400, 680),
        ]
        summary = compute_cost_summary(rows)
        assert summary.scale_up_latency_seconds == 20.0

    def test_scale_down_time_measures_drop_after_peak(self):
        """TC-U17 | FR12 | Scale-down time should be measured from the last peak sample."""
        rows = [
            MetricRow(0, "keda", 1, 600, 300),
            MetricRow(10, "keda", 3, 2400, 1500),
            MetricRow(20, "keda", 3, 2200, 1400),
            MetricRow(30, "keda", 2, 1200, 800),
        ]
        summary = compute_cost_summary(rows)
        assert summary.scale_down_time_seconds == 10.0

    def test_average_cpu_utilisation_uses_live_executor_capacity(self):
        """TC-U18 | FR12 | Avg CPU utilisation should be averaged against current executor capacity."""
        rows = [
            MetricRow(0, "hpa", 1, 500, 256),   # 50%
            MetricRow(10, "hpa", 2, 1000, 512), # 50%
            MetricRow(20, "hpa", 2, 1500, 768), # 75%
        ]
        summary = compute_cost_summary(rows)
        assert abs(summary.avg_cpu_utilisation_pct - 58.3333) < 0.01

    def test_cpu_efficiency_between_0_and_100(self):
        """TC-U19 | FR12 | CPU efficiency percentage must be in [0, 100]."""
        rows = self._make_rows("dra", exec_count=3, cpu=1500.0)
        summary = compute_cost_summary(rows)
        assert 0.0 <= summary.cpu_efficiency_pct <= 100.0

    def test_memory_efficiency_between_0_and_100(self):
        """TC-U20 | FR12 | Memory efficiency percentage must be in [0, 100]."""
        rows = self._make_rows("keda", exec_count=3, mem=768.0)
        summary = compute_cost_summary(rows)
        assert 0.0 <= summary.memory_efficiency_pct <= 100.0


# ---------------------------------------------------------------------------
# TC-U18  load_csv: file parsing and validation
# ---------------------------------------------------------------------------
class TestLoadCsv:

    def _write_csv(self, rows: list, path: str):
        fieldnames = ["timestamp", "strategy", "executor_count",
                      "cpu_millicores", "memory_mib"]
        with open(path, "w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    def test_valid_csv_loads_correctly(self):
        """TC-U18 | FR5 | Well-formed CSV should load all rows without error."""
        data = [
            {"timestamp": 0,  "strategy": "dra",  "executor_count": 1,
             "cpu_millicores": 500.0, "memory_mib": 256.0},
            {"timestamp": 10, "strategy": "dra",  "executor_count": 2,
             "cpu_millicores": 1800.0, "memory_mib": 900.0},
        ]
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv",
                                         delete=False) as fh:
            path = fh.name
        try:
            self._write_csv(data, path)
            rows = load_csv(path)
            assert len(rows) == 2
            assert rows[0].strategy == "dra"
            assert rows[1].executor_count == 2
        finally:
            os.unlink(path)

    def test_missing_file_raises_file_not_found(self):
        """TC-U19 | FR5 | Non-existent path should raise FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            load_csv("/tmp/does_not_exist_xyz.csv")

    def test_missing_column_raises_value_error(self):
        """TC-U20 | FR5 | CSV missing required column should raise ValueError."""
        with tempfile.NamedTemporaryFile(
                mode="w", suffix=".csv", delete=False) as fh:
            fh.write("timestamp,strategy,executor_count\n")
            fh.write("0,dra,1\n")
            path = fh.name
        try:
            with pytest.raises(ValueError, match="missing required columns"):
                load_csv(path)
        finally:
            os.unlink(path)

    def test_numeric_values_parsed_as_float(self):
        """TC-U21 | FR5 | cpu_millicores and memory_mib should be parsed as float."""
        data = [{"timestamp": 0, "strategy": "hpa", "executor_count": 1,
                 "cpu_millicores": "1234.56", "memory_mib": "789.01"}]
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv",
                                         delete=False) as fh:
            path = fh.name
        try:
            self._write_csv(data, path)
            rows = load_csv(path)
            assert isinstance(rows[0].cpu_millicores, float)
            assert abs(rows[0].cpu_millicores - 1234.56) < 0.001
        finally:
            os.unlink(path)

    def test_invalid_numeric_value_raises_value_error(self):
        """TC-U22 | FR5 | Malformed numeric data should raise a helpful ValueError."""
        data = [{"timestamp": 0, "strategy": "dra", "executor_count": 1,
                 "cpu_millicores": "warning-text", "memory_mib": "512"}]
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv",
                                         delete=False) as fh:
            path = fh.name
        try:
            self._write_csv(data, path)
            with pytest.raises(ValueError, match="Invalid numeric data"):
                load_csv(path)
        finally:
            os.unlink(path)


# ---------------------------------------------------------------------------
# TC-U23  compare_strategies: multi-strategy ranking
# ---------------------------------------------------------------------------
class TestCompareStrategies:

    def _write_multi_strategy_csv(self, path: str):
        fieldnames = ["timestamp", "strategy", "executor_count",
                      "cpu_millicores", "memory_mib"]
        rows = []
        for i in range(10):
            rows.append({"timestamp": i * 10, "strategy": "dra",
                         "executor_count": 1, "cpu_millicores": 500, "memory_mib": 256})
        for i in range(10):
            rows.append({"timestamp": i * 10, "strategy": "keda",
                         "executor_count": 2, "cpu_millicores": 1800, "memory_mib": 900})
        for i in range(10):
            rows.append({"timestamp": i * 10, "strategy": "hpa",
                         "executor_count": 3, "cpu_millicores": 2800, "memory_mib": 1400})
        with open(path, "w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    def test_returns_three_summaries(self):
        """TC-U23 | FR12 | compare_strategies should return one summary per strategy."""
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as fh:
            path = fh.name
        try:
            self._write_multi_strategy_csv(path)
            summaries = compare_strategies(path)
            assert len(summaries) == 3
        finally:
            os.unlink(path)

    def test_results_sorted_by_cost_ascending(self):
        """TC-U24 | FR12 | Results should be sorted cheapest-first."""
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as fh:
            path = fh.name
        try:
            self._write_multi_strategy_csv(path)
            summaries = compare_strategies(path)
            costs = [s.simulated_cost_usd for s in summaries]
            assert costs == sorted(costs), \
                f"Expected ascending cost order, got {costs}"
        finally:
            os.unlink(path)

    def test_all_strategy_names_present(self):
        """TC-U25 | FR12 | All three strategy names should appear in the results."""
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as fh:
            path = fh.name
        try:
            self._write_multi_strategy_csv(path)
            summaries = compare_strategies(path)
            names = {s.strategy for s in summaries}
            assert names == {"dra", "keda", "hpa"}
        finally:
            os.unlink(path)
