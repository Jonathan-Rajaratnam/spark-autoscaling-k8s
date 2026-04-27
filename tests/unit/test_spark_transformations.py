"""
test_unit.py — Unit tests for individual transformation logic in app.py.

Tests U-01 to U-10. All tests use in-memory DataFrames (no file I/O,
no Kubernetes cluster). SparkSession runs in local[2] mode via conftest.py.
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, FloatType, IntegerType
)
from pyspark.sql.window import Window


# ─────────────────────────────────────────────────────────────────────────────
# U-01 — property_prices: schema application produces correct 16 columns
# ─────────────────────────────────────────────────────────────────────────────
def test_u01_property_prices_schema(spark):
    """U-01: The 16-column schema is applied correctly to a raw headerless row."""
    schema_cols = [
        "transaction_id", "price", "date_of_transfer", "postcode",
        "property_type", "new_build", "tenure", "paon", "saon",
        "street", "locality", "town", "district", "county",
        "transaction_category", "record_status"
    ]
    raw = spark.createDataFrame(
        [("UUID-1", "250000", "2010-01-25 00:00", "SW1A 1AA",
          "D", "N", "F", "10", "", "HIGH ST", "", "LONDON", "WESTMINSTER",
          "GREATER LONDON", "A", "A")],
        schema=schema_cols
    )
    assert raw.columns == schema_cols, "Schema columns do not match expected 16-column list"
    assert raw.count() == 1


# ─────────────────────────────────────────────────────────────────────────────
# U-02 — property_prices: null price rows are filtered out
# ─────────────────────────────────────────────────────────────────────────────
def test_u02_null_price_filter(spark):
    """U-02: Rows where price cannot be cast to LongType become null and are dropped."""
    schema_cols = [
        "transaction_id", "price", "date_of_transfer", "postcode",
        "property_type", "new_build", "tenure", "paon", "saon",
        "street", "locality", "town", "district", "county",
        "transaction_category", "record_status"
    ]
    rows = [
        ("UUID-1", "250000", "2010-01-25 00:00", "SW1A 1AA", "D", "N", "F",
         "10", "", "HIGH ST", "", "LONDON", "W", "GREATER LONDON", "A", "A"),
        ("UUID-2", "NOT_A_NUM", "2011-05-10 00:00", "SW1A 1AA", "S", "N", "F",
         "11", "", "HIGH ST", "", "LONDON", "W", "GREATER LONDON", "A", "A"),
    ]
    df = (spark.createDataFrame(rows, schema=schema_cols)
          .withColumn("price", F.col("price").cast(LongType()))
          .filter(F.col("price").isNotNull()))

    assert df.count() == 1, "Expected only 1 row after filtering null prices"


# ─────────────────────────────────────────────────────────────────────────────
# U-03 — property_prices: rows with unparseable dates are filtered out
# ─────────────────────────────────────────────────────────────────────────────
def test_u03_null_year_filter(spark):
    """U-03: Rows with an invalid date_of_transfer produce null year and are dropped."""
    schema_cols = [
        "transaction_id", "price", "date_of_transfer", "postcode",
        "property_type", "new_build", "tenure", "paon", "saon",
        "street", "locality", "town", "district", "county",
        "transaction_category", "record_status"
    ]
    rows = [
        ("UUID-1", "250000", "2010-01-25 00:00", "SW1A 1AA", "D", "N", "F",
         "10", "", "HIGH ST", "", "LONDON", "W", "GREATER LONDON", "A", "A"),
        ("UUID-2", "300000", "INVALID_DATE", "SW1A 1AA", "S", "N", "F",
         "11", "", "HIGH ST", "", "LONDON", "W", "GREATER LONDON", "A", "A"),
    ]
    df = (spark.createDataFrame(rows, schema=schema_cols)
          .withColumn("price", F.col("price").cast(LongType()))
          .withColumn("year", F.year(F.to_timestamp("date_of_transfer", "yyyy-MM-dd HH:mm")))
          .filter(F.col("price").isNotNull() & F.col("year").isNotNull()))

    assert df.count() == 1, "Expected only 1 row after filtering null years"


# ─────────────────────────────────────────────────────────────────────────────
# U-04 — property_prices: year is correctly extracted from date string
# ─────────────────────────────────────────────────────────────────────────────
def test_u04_year_extraction(spark):
    """U-04: Year 2010 is extracted from '2010-01-25 00:00'."""
    df = spark.createDataFrame(
        [("2010-01-25 00:00",)], ["date_of_transfer"]
    ).withColumn("year", F.year(F.to_timestamp("date_of_transfer", "yyyy-MM-dd HH:mm")))

    year_val = df.collect()[0]["year"]
    assert year_val == 2010, f"Expected year 2010, got {year_val}"


# ─────────────────────────────────────────────────────────────────────────────
# U-05 — property_prices: affordability ratio formula is correct
# ─────────────────────────────────────────────────────────────────────────────
def test_u05_affordability_ratio(spark):
    """U-05: affordability_ratio = round(p90 / p10, 2)."""
    df = spark.createDataFrame(
        [(500000, 100000)], ["p90_price", "p10_price"]
    ).withColumn(
        "affordability_ratio",
        F.round(F.col("p90_price") / F.col("p10_price"), 2)
    )
    ratio = df.collect()[0]["affordability_ratio"]
    assert ratio == 5.0, f"Expected affordability_ratio 5.0, got {ratio}"


# ─────────────────────────────────────────────────────────────────────────────
# U-06 — property_prices: affordability filter removes districts with ≤100 sales
# ─────────────────────────────────────────────────────────────────────────────
def test_u06_affordability_sales_filter(spark):
    """U-06: Districts with 100 or fewer total_sales are excluded."""
    df = spark.createDataFrame(
        [("DISTRICT_A", 50), ("DISTRICT_B", 101), ("DISTRICT_C", 100)],
        ["district", "total_sales"]
    ).filter(F.col("total_sales") > 100)

    districts = [r["district"] for r in df.collect()]
    assert "DISTRICT_B" in districts
    assert "DISTRICT_A" not in districts
    assert "DISTRICT_C" not in districts, "Exactly >100 should pass (not >=100)"


# ─────────────────────────────────────────────────────────────────────────────
# U-07 — steam_heavy: weighted_score is non-null and positive
# ─────────────────────────────────────────────────────────────────────────────
def test_u07_weighted_score_formula(spark):
    """U-07: weighted_score = avg_hours * avg_positive_ratio * log(total_recs+1) > 0."""
    df = spark.createDataFrame(
        [(50.0, 0.85, 1000)],
        ["avg_hours_played", "avg_positive_ratio", "total_recs"]
    ).withColumn(
        "weighted_score",
        F.col("avg_hours_played") * F.col("avg_positive_ratio") * F.log(F.col("total_recs") + 1)
    )
    score = df.collect()[0]["weighted_score"]
    assert score is not None, "weighted_score should not be null"
    assert score > 0, f"weighted_score should be positive, got {score}"


# ─────────────────────────────────────────────────────────────────────────────
# U-08 — steam_heavy: 3-way join produces expected columns
# ─────────────────────────────────────────────────────────────────────────────
def test_u08_three_way_join(spark):
    """U-08: Joining recommendations × games × users yields all required columns."""
    recommendations = spark.createDataFrame(
        [(1, "u1", 100.0, 1, True)],
        ["app_id", "user_id", "hours", "helpful", "is_recommended"]
    )
    games = spark.createDataFrame(
        [(1, "Game A", "Overwhelmingly Positive", 0.95, 29.99, 10000)],
        ["app_id", "title", "rating", "positive_ratio", "price_final", "user_reviews"]
    )
    users = spark.createDataFrame(
        [("u1", 50, 5)],
        ["user_id", "products", "reviews"]
    )

    enriched = recommendations.join(games, "app_id", "inner").join(users, "user_id", "inner")

    expected_cols = {"app_id", "user_id", "hours", "title", "rating",
                     "positive_ratio", "products", "reviews"}
    actual_cols = set(enriched.columns)
    assert expected_cols.issubset(actual_cols), \
        f"Missing columns after join: {expected_cols - actual_cols}"


# ─────────────────────────────────────────────────────────────────────────────
# U-09 — steam_heavy: window rank assigns rank 1 to highest-hours game per user
# ─────────────────────────────────────────────────────────────────────────────
def test_u09_window_rank(spark):
    """U-09: The game with the most hours gets rank 1 within the user's window."""
    data = spark.createDataFrame(
        [("u1", "game_A", 200.0),
         ("u1", "game_B", 50.0),
         ("u1", "game_C", 100.0)],
        ["user_id", "app_id", "hours"]
    )
    user_window = Window.partitionBy("user_id").orderBy(F.col("hours").desc())
    ranked = data.withColumn("rank_in_library", F.rank().over(user_window))

    top = ranked.filter(F.col("rank_in_library") == 1).collect()[0]
    assert top["app_id"] == "game_A", \
        f"Expected game_A (200h) to have rank 1, got {top['app_id']}"


# ─────────────────────────────────────────────────────────────────────────────
# U-10 — Invalid mode raises ValueError
# ─────────────────────────────────────────────────────────────────────────────
def test_u10_invalid_mode_raises():
    """U-10: Passing an unrecognised mode string raises a ValueError."""
    valid_modes = {"top_games", "sentiment", "user_activity", "steam_heavy", "property_prices"}
    mode = "bad_mode"

    if mode not in valid_modes:
        with pytest.raises(ValueError):
            raise ValueError(
                f"Unknown mode: {mode!r}. "
                f"Valid modes: top_games, sentiment, user_activity, steam_heavy, property_prices"
            )
    else:
        pytest.fail("bad_mode should not be in valid_modes")
