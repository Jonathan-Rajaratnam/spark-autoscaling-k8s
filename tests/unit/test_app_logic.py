"""
tests/unit/test_app_logic.py

Unit tests for app_logic.py — mode validation, partition configuration,
schema correctness, autoscaler configuration constants, and CSV row validation.

Requirements covered:
  FR1  – PySpark workload accepts a mode argument
  FR2  – Benchmark modes generate sustained autoscaler pressure
  FR3  – Executor resource budgets match manifest values
  FR4  – Autoscaler thresholds match manifest values
  FR7  – Metric CSV rows conform to expected schema

Run with:  pytest tests/unit/test_app_logic.py -v
"""

import os
import sys

import pytest

src_dir = os.path.join(os.path.dirname(__file__), "../../")
sys.path.insert(0, os.path.abspath(src_dir))
from app_logic import (
    AUTOSCALER_CONFIG,
    BENCHMARK_MODES,
    EXECUTOR_RESOURCES,
    DRIVER_RESOURCES,
    SCALE_DOWN_SLEEP_SECONDS,
    VALID_MODES,
    get_autoscaler_config,
    get_expected_columns,
    get_partition_count,
    is_benchmark_mode,
    validate_csv_row,
    validate_mode,
)


# ---------------------------------------------------------------------------
# TC-U25  validate_mode: valid and invalid inputs
# ---------------------------------------------------------------------------
class TestValidateMode:

    def test_all_valid_modes_accepted(self):
        """TC-U25 | FR1 | All five valid modes should be accepted without error."""
        for mode in VALID_MODES:
            assert validate_mode(mode) == mode

    def test_mode_normalised_to_lowercase(self):
        """TC-U26 | FR1 | Mode argument should be normalised to lower case."""
        assert validate_mode("TOP_GAMES") == "top_games"
        assert validate_mode("Steam_Heavy") == "steam_heavy"

    def test_whitespace_stripped(self):
        """TC-U27 | FR1 | Leading/trailing whitespace should be stripped."""
        assert validate_mode("  sentiment  ") == "sentiment"

    def test_invalid_mode_raises_value_error(self):
        """TC-U28 | FR1 | Unknown mode should raise ValueError."""
        with pytest.raises(ValueError, match="Invalid mode"):
            validate_mode("spark_streaming")

    def test_empty_string_raises_value_error(self):
        """TC-U29 | FR1 | Empty mode string should raise ValueError."""
        with pytest.raises(ValueError, match="Invalid mode"):
            validate_mode("")


# ---------------------------------------------------------------------------
# TC-U30  is_benchmark_mode: distinguishing lightweight vs. sustained modes
# ---------------------------------------------------------------------------
class TestIsBenchmarkMode:

    def test_steam_heavy_is_benchmark(self):
        """TC-U30 | FR2 | steam_heavy must be identified as a benchmark mode."""
        assert is_benchmark_mode("steam_heavy") is True

    def test_property_prices_is_benchmark(self):
        """TC-U31 | FR2 | property_prices must be identified as a benchmark mode."""
        assert is_benchmark_mode("property_prices") is True

    def test_top_games_is_not_benchmark(self):
        """TC-U32 | FR2 | top_games is a lightweight validation mode."""
        assert is_benchmark_mode("top_games") is False

    def test_sentiment_is_not_benchmark(self):
        """TC-U33 | FR2 | sentiment is a lightweight validation mode."""
        assert is_benchmark_mode("sentiment") is False

    def test_user_activity_is_not_benchmark(self):
        """TC-U34 | FR2 | user_activity is a lightweight validation mode."""
        assert is_benchmark_mode("user_activity") is False

    def test_exactly_two_benchmark_modes_exist(self):
        """TC-U35 | FR2 | Exactly two benchmark modes should be defined."""
        assert len(BENCHMARK_MODES) == 2


# ---------------------------------------------------------------------------
# TC-U36  get_partition_count: partition configuration
# ---------------------------------------------------------------------------
class TestGetPartitionCount:

    def test_lightweight_modes_use_300_partitions(self):
        """TC-U36 | FR1 | Lightweight modes must use repartition(300)."""
        for mode in ("top_games", "sentiment", "user_activity"):
            assert get_partition_count(mode) == 300, \
                f"Expected 300 partitions for {mode}"

    def test_steam_heavy_uses_500_partitions(self):
        """TC-U37 | FR2 | steam_heavy must use repartition(500) for recommendations."""
        assert get_partition_count("steam_heavy") == 500

    def test_property_prices_uses_500_partitions(self):
        """TC-U38 | FR2 | property_prices must use repartition(500)."""
        assert get_partition_count("property_prices") == 500

    def test_invalid_mode_raises(self):
        """TC-U39 | FR1 | Partition count lookup on invalid mode should raise."""
        with pytest.raises(ValueError):
            get_partition_count("unknown_mode")


# ---------------------------------------------------------------------------
# TC-U40  get_expected_columns: output schema validation
# ---------------------------------------------------------------------------
class TestGetExpectedColumns:

    def test_top_games_schema(self):
        """TC-U40 | FR1 | top_games output must contain app_id and recommendation_count."""
        cols = get_expected_columns("top_games")
        assert "app_id" in cols
        assert "recommendation_count" in cols

    def test_sentiment_schema(self):
        """TC-U41 | FR1 | sentiment output must contain positive and negative counts."""
        cols = get_expected_columns("sentiment")
        assert "positive_count" in cols
        assert "negative_count" in cols

    def test_steam_heavy_schema_includes_window_column(self):
        """TC-U42 | FR2 | steam_heavy output must include row_number (window function)."""
        cols = get_expected_columns("steam_heavy")
        assert "row_number" in cols
        assert "weighted_score" in cols

    def test_property_prices_schema(self):
        """TC-U43 | FR2 | property_prices output must contain county and avg_price."""
        cols = get_expected_columns("property_prices")
        assert "county" in cols
        assert "avg_price" in cols


# ---------------------------------------------------------------------------
# TC-U44  get_autoscaler_config: autoscaler threshold constants
# ---------------------------------------------------------------------------
class TestGetAutoscalerConfig:

    def test_keda_jvm_threshold_is_65_pct(self):
        """TC-U44 | FR4 | KEDA JVM heap threshold must be 65% (matches ScaledObject)."""
        cfg = get_autoscaler_config("keda")
        assert cfg["jvm_heap_threshold_pct"] == 65

    def test_keda_cpu_threshold_is_70_pct(self):
        """TC-U45 | FR4 | KEDA CPU threshold must be 70% (matches ScaledObject)."""
        cfg = get_autoscaler_config("keda")
        assert cfg["cpu_threshold_pct"] == 70

    def test_hpa_thresholds_match_keda(self):
        """TC-U46 | FR4 | HPA thresholds must match KEDA for experimental validity."""
        keda = get_autoscaler_config("keda")
        hpa  = get_autoscaler_config("hpa")
        assert keda["jvm_heap_threshold_pct"] == hpa["jvm_heap_threshold_pct"], \
            "HPA and KEDA JVM thresholds must be identical (controlled comparison)"
        assert keda["cpu_threshold_pct"] == hpa["cpu_threshold_pct"], \
            "HPA and KEDA CPU thresholds must be identical (controlled comparison)"

    def test_all_strategies_max_3_replicas(self):
        """TC-U47 | FR3 | All strategies must cap at 3 executors/replicas."""
        for strategy in ("keda", "hpa"):
            cfg = get_autoscaler_config(strategy)
            assert cfg["max_replicas"] == 3, \
                f"Strategy {strategy} should cap at 3 replicas"
        dra = get_autoscaler_config("dra")
        assert dra["max_executors"] == 3

    def test_all_strategies_min_1_replica(self):
        """TC-U48 | FR3 | All strategies must maintain at least 1 executor/replica."""
        for strategy in ("keda", "hpa"):
            cfg = get_autoscaler_config(strategy)
            assert cfg["min_replicas"] == 1

    def test_dra_backlog_timeout_is_5_seconds(self):
        """TC-U49 | FR4 | DRA scheduler backlog timeout must be 5s (matches sparkConf)."""
        cfg = get_autoscaler_config("dra")
        assert cfg["scheduler_backlog_timeout_sec"] == 5

    def test_dra_idle_timeout_is_60_seconds(self):
        """TC-U50 | FR4 | DRA executor idle timeout must be 60s (matches sparkConf)."""
        cfg = get_autoscaler_config("dra")
        assert cfg["executor_idle_timeout_sec"] == 60

    def test_hpa_scaledown_window_longer_than_scaleup(self):
        """TC-U51 | FR4 | HPA scale-down stabilisation window must exceed scale-up."""
        cfg = get_autoscaler_config("hpa")
        assert cfg["scaledown_stabilisation_sec"] > cfg["scaleup_stabilisation_sec"], \
            "Scale-down window (90s) must be longer than scale-up window (30s)"

    def test_unknown_strategy_raises(self):
        """TC-U52 | FR4 | Unknown strategy name should raise KeyError."""
        with pytest.raises(KeyError, match="Unknown strategy"):
            get_autoscaler_config("mythical_scaler")

    def test_keda_hpa_polling_interval_matches(self):
        """TC-U53 | FR4 | KEDA and HPA polling intervals must match (15s each)."""
        keda = get_autoscaler_config("keda")
        hpa  = get_autoscaler_config("hpa")
        assert keda["polling_interval_sec"] == 15
        assert hpa["polling_interval_sec"]  == 15


# ---------------------------------------------------------------------------
# TC-U54  resource budget constants
# ---------------------------------------------------------------------------
class TestResourceBudgets:

    def test_executor_cpu_is_1_core(self):
        """TC-U54 | FR3 | Executor CPU budget must be 1 vCPU (matches manifests)."""
        assert EXECUTOR_RESOURCES["cpu_cores"] == 1

    def test_executor_memory_is_512_mib(self):
        """TC-U55 | FR3 | Executor memory budget must be 512 MiB (matches manifests)."""
        assert EXECUTOR_RESOURCES["memory_mib"] == 512

    def test_scale_down_sleep_is_120_seconds(self):
        """TC-U56 | FR2 | Scale-down observation window must be 120s (matches app.py)."""
        assert SCALE_DOWN_SLEEP_SECONDS == 120


# ---------------------------------------------------------------------------
# TC-U57  validate_csv_row: metric row schema enforcement
# ---------------------------------------------------------------------------
class TestValidateCsvRow:

    def _valid_row(self, **overrides):
        base = {
            "timestamp":      "0",
            "strategy":       "keda",
            "executor_count": "2",
            "cpu_millicores": "1800.0",
            "memory_mib":     "900.0",
        }
        base.update(overrides)
        return base

    def test_valid_row_passes(self):
        """TC-U57 | FR7 | A well-formed CSV row should pass validation."""
        assert validate_csv_row(self._valid_row()) is True

    def test_missing_field_raises(self):
        """TC-U58 | FR7 | Row missing a required field should raise ValueError."""
        row = self._valid_row()
        del row["cpu_millicores"]
        with pytest.raises(ValueError, match="missing fields"):
            validate_csv_row(row)

    def test_invalid_strategy_raises(self):
        """TC-U59 | FR7 | Unknown strategy value should raise ValueError."""
        with pytest.raises(ValueError, match="Unknown strategy"):
            validate_csv_row(self._valid_row(strategy="yarn"))

    def test_negative_cpu_raises(self):
        """TC-U60 | FR7 | Negative cpu_millicores should raise ValueError."""
        with pytest.raises(ValueError, match="cpu_millicores"):
            validate_csv_row(self._valid_row(cpu_millicores="-1.0"))

    def test_excessive_executor_count_raises(self):
        """TC-U61 | FR7 | executor_count > 10 should raise as implausible."""
        with pytest.raises(ValueError, match="executor_count"):
            validate_csv_row(self._valid_row(executor_count="99"))

    def test_all_three_strategies_accepted(self):
        """TC-U62 | FR7 | Rows from all three strategies should pass validation."""
        for strat in ("dra", "keda", "hpa"):
            assert validate_csv_row(self._valid_row(strategy=strat)) is True
