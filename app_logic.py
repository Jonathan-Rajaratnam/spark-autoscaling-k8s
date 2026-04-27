"""
app_logic.py
Validation and configuration logic extracted from app.py for unit-testability.
The full app.py requires a running Spark cluster; this module isolates the
pure-Python logic that can be tested without one.
"""

VALID_MODES = {
    "top_games",
    "sentiment",
    "user_activity",
    "steam_heavy",
    "property_prices",
}

# Partition counts used per mode (mirrors app.py constants)
PARTITION_COUNTS = {
    "top_games":        300,
    "sentiment":        300,
    "user_activity":    300,
    "steam_heavy":      500,   # recommendations; games=50, users=200 (asymmetric)
    "property_prices":  500,
}

BENCHMARK_MODES = {"steam_heavy", "property_prices"}
LIGHTWEIGHT_MODES = VALID_MODES - BENCHMARK_MODES

# Expected output schema columns per mode
EXPECTED_COLUMNS = {
    "top_games":        {"app_id", "recommendation_count"},
    "sentiment":        {"app_id", "positive_count", "negative_count"},
    "user_activity":    {"user_id", "review_count"},
    "steam_heavy":      {"app_id", "weighted_score", "row_number"},
    "property_prices":  {"county", "avg_price", "transaction_count"},
}

# Autoscaler threshold configuration (mirrors manifest values)
AUTOSCALER_CONFIG = {
    "keda": {
        "jvm_heap_threshold_pct":  65,
        "cpu_threshold_pct":       70,
        "polling_interval_sec":    15,
        "min_replicas":            1,
        "max_replicas":            3,
    },
    "hpa": {
        "jvm_heap_threshold_pct":  65,
        "cpu_threshold_pct":       70,
        "polling_interval_sec":    15,
        "min_replicas":            1,
        "max_replicas":            3,
        "scaleup_stabilisation_sec":   30,
        "scaledown_stabilisation_sec": 90,
    },
    "dra": {
        "scheduler_backlog_timeout_sec":    5,
        "sustained_scheduler_backlog_sec":  10,
        "executor_idle_timeout_sec":        60,
        "min_executors":                    1,
        "max_executors":                    3,
    },
}

# Per-executor resource budget (mirrors manifest values)
EXECUTOR_RESOURCES = {
    "cpu_cores":   1,
    "memory_mib":  512,
}

DRIVER_RESOURCES = {
    "cpu_cores":   1,
    "memory_mib":  1024,
}

SCALE_DOWN_SLEEP_SECONDS = 120   # deliberate sleep after computation


def validate_mode(mode: str) -> str:
    """
    Validate and normalise a mode argument.

    Returns:
        The lower-cased mode string if valid.

    Raises:
        ValueError: if mode is not in VALID_MODES.
    """
    normalised = mode.strip().lower()
    if normalised not in VALID_MODES:
        raise ValueError(
            f"Invalid mode '{mode}'. Must be one of: {sorted(VALID_MODES)}"
        )
    return normalised


def is_benchmark_mode(mode: str) -> bool:
    """Return True if the mode is a sustained benchmark (generates autoscaler pressure)."""
    return validate_mode(mode) in BENCHMARK_MODES


def get_partition_count(mode: str) -> int:
    """Return the repartition() count used by a given mode."""
    return PARTITION_COUNTS[validate_mode(mode)]


def get_expected_columns(mode: str) -> set:
    """Return the expected output DataFrame columns for a given mode."""
    return EXPECTED_COLUMNS[validate_mode(mode)]


def get_autoscaler_config(strategy: str) -> dict:
    """
    Return the autoscaler configuration dict for a given strategy.

    Args:
        strategy: "dra" | "keda" | "hpa"

    Raises:
        KeyError: if strategy is not recognised.
    """
    key = strategy.strip().lower()
    if key not in AUTOSCALER_CONFIG:
        raise KeyError(
            f"Unknown strategy '{strategy}'. Must be one of: {list(AUTOSCALER_CONFIG)}"
        )
    return AUTOSCALER_CONFIG[key]


def validate_csv_row(row: dict) -> bool:
    """
    Validate that a metric CSV row produced by run_experiment.sh has the correct
    structure and plausible values.

    Returns True if valid, raises ValueError with a descriptive message if not.
    """
    required_fields = {"timestamp", "strategy", "executor_count", "cpu_millicores", "memory_mib"}
    missing = required_fields - set(row.keys())
    if missing:
        raise ValueError(f"CSV row missing fields: {missing}")

    if row["strategy"] not in {"dra", "dynamic", "keda", "hpa"}:
        raise ValueError(f"Unknown strategy value: '{row['strategy']}'")

    try:
        executor_count = int(row["executor_count"])
        cpu_millicores = float(row["cpu_millicores"])
        memory_mib     = float(row["memory_mib"])
    except (ValueError, TypeError) as exc:
        raise ValueError(f"Non-numeric metric value in row: {exc}") from exc

    if not (0 <= executor_count <= 10):
        raise ValueError(f"executor_count {executor_count} outside plausible range [0, 10]")
    if cpu_millicores < 0:
        raise ValueError(f"cpu_millicores {cpu_millicores} must be >= 0")
    if memory_mib < 0:
        raise ValueError(f"memory_mib {memory_mib} must be >= 0")

    return True
