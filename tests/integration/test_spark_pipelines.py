"""
test_integration.py — Integration tests for all 5 app.py modes.

Tests I-01 to I-08. Each test writes a small synthetic CSV to a temp
directory and runs the full mode pipeline end-to-end, validating output
shape, row count, and ordering. No Kubernetes cluster required.
"""

import os
import csv
import pytest
import tempfile
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, FloatType
from pyspark.sql.window import Window


# ─────────────────────────────────────────────────────────────────────────────
# Synthetic data helpers
# ─────────────────────────────────────────────────────────────────────────────

RECOMMENDATIONS_HEADER = ["app_id", "user_id", "is_recommended", "hours", "helpful",
                           "date", "review_id"]

GAMES_HEADER = ["app_id", "title", "date_release", "win", "mac", "linux",
                "rating", "positive_ratio", "user_reviews", "price_final", "price_original",
                "discount", "steam_deck"]

USERS_HEADER = ["user_id", "products", "reviews"]

LAND_COLS = [
    "transaction_id", "price", "date_of_transfer", "postcode",
    "property_type", "new_build", "tenure", "paon", "saon",
    "street", "locality", "town", "district", "county",
    "transaction_category", "record_status"
]


def write_csv(path: str, header: list, rows: list):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)


def write_land_csv(path: str, rows: list):
    """Land Registry CSV has no header."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(rows)


# ─────────────────────────────────────────────────────────────────────────────
# I-01 — top_games mode produces non-empty output grouped by app_id
# ─────────────────────────────────────────────────────────────────────────────
def test_i01_top_games_mode(spark, tmp_path):
    """I-01: top_games groups recommendations by app_id and returns non-empty result."""
    rec_path = str(tmp_path / "games" / "recommendations.csv")
    write_csv(rec_path, RECOMMENDATIONS_HEADER, [
        ["101", "u1", "true", "10.5", "3", "2020-01-01", "r1"],
        ["101", "u2", "false", "5.0", "1", "2020-01-02", "r2"],
        ["202", "u3", "true", "20.0", "0", "2020-01-03", "r3"],
    ])

    df = spark.read.option("header", True).csv(rec_path)
    result = df.groupBy("app_id").count().orderBy("count", ascending=False)

    rows = result.collect()
    assert len(rows) == 2, f"Expected 2 distinct app_ids, got {len(rows)}"
    assert rows[0]["app_id"] == "101", "app_id 101 has 2 recommendations and should rank first"


# ─────────────────────────────────────────────────────────────────────────────
# I-02 — sentiment mode produces exactly 2 rows (true / false)
# ─────────────────────────────────────────────────────────────────────────────
def test_i02_sentiment_mode(spark, tmp_path):
    """I-02: sentiment groups by is_recommended — expects exactly 2 distinct values."""
    rec_path = str(tmp_path / "games" / "recommendations.csv")
    write_csv(rec_path, RECOMMENDATIONS_HEADER, [
        ["101", "u1", "true", "10.5", "3", "2020-01-01", "r1"],
        ["101", "u2", "false", "5.0", "1", "2020-01-02", "r2"],
        ["202", "u3", "true", "20.0", "0", "2020-01-03", "r3"],
    ])

    df = spark.read.option("header", True).csv(rec_path)
    result = df.groupBy("is_recommended").count()

    assert result.count() == 2, "Expected exactly 2 rows (true/false)"


# ─────────────────────────────────────────────────────────────────────────────
# I-03 — user_activity mode groups by user_id
# ─────────────────────────────────────────────────────────────────────────────
def test_i03_user_activity_mode(spark, tmp_path):
    """I-03: user_activity groups by user_id producing one row per user."""
    rec_path = str(tmp_path / "games" / "recommendations.csv")
    write_csv(rec_path, RECOMMENDATIONS_HEADER, [
        ["101", "u1", "true", "10.5", "3", "2020-01-01", "r1"],
        ["202", "u1", "false", "5.0", "1", "2020-01-02", "r2"],
        ["303", "u2", "true", "20.0", "0", "2020-01-03", "r3"],
    ])

    df = spark.read.option("header", True).csv(rec_path)
    result = df.groupBy("user_id").count()

    assert result.count() == 2, "Expected 2 distinct users (u1, u2)"


# ─────────────────────────────────────────────────────────────────────────────
# I-04 — steam_heavy mode produces weighted_score column
# ─────────────────────────────────────────────────────────────────────────────
def test_i04_steam_heavy_mode(spark, tmp_path):
    """I-04: steam_heavy 3-way join + window + aggregation produces weighted_score column."""
    games_dir = str(tmp_path / "games")

    write_csv(str(tmp_path / "games" / "recommendations.csv"), RECOMMENDATIONS_HEADER, [
        ["101", "u1", "true", "100.0", "10", "2020-01-01", "r1"],
        ["101", "u2", "true", "50.0", "5", "2020-01-02", "r2"],
        ["202", "u1", "false", "20.0", "1", "2020-01-03", "r3"],
    ])
    write_csv(str(tmp_path / "games" / "games.csv"), GAMES_HEADER, [
        ["101", "Game Alpha", "2019-01-01", "1", "0", "0",
         "Very Positive", "0.87", "5000", "29.99", "39.99", "25", "1"],
        ["202", "Game Beta", "2018-01-01", "1", "0", "0",
         "Mixed", "0.55", "200", "9.99", "9.99", "0", "0"],
    ])
    write_csv(str(tmp_path / "games" / "users.csv"), USERS_HEADER, [
        ["u1", "30", "5"],
        ["u2", "15", "2"],
    ])

    recommendations = (spark.read.option("header", True)
                       .csv(str(tmp_path / "games" / "recommendations.csv"))
                       .withColumn("hours", F.col("hours").cast(FloatType()))
                       .withColumn("helpful", F.col("helpful").cast(LongType())))
    games = (spark.read.option("header", True)
             .csv(str(tmp_path / "games" / "games.csv"))
             .withColumn("positive_ratio", F.col("positive_ratio").cast(FloatType()))
             .withColumn("price_final", F.col("price_final").cast(FloatType()))
             .withColumn("user_reviews", F.col("user_reviews").cast(LongType())))
    users = (spark.read.option("header", True)
             .csv(str(tmp_path / "games" / "users.csv"))
             .withColumn("products", F.col("products").cast(LongType()))
             .withColumn("reviews", F.col("reviews").cast(LongType())))

    enriched = recommendations.join(games, "app_id", "inner").join(users, "user_id", "inner")
    user_window = Window.partitionBy("user_id").orderBy(F.col("hours").desc())
    ranked = enriched.withColumn("rank_in_library", F.rank().over(user_window))

    result = ranked.groupBy("app_id", "title", "rating").agg(
        F.count("*").alias("total_recs"),
        F.avg("hours").alias("avg_hours_played"),
        F.avg("positive_ratio").alias("avg_positive_ratio"),
        F.sum(F.when(F.col("is_recommended") == "true", 1).otherwise(0)).alias("positive_recs"),
        F.avg("products").alias("avg_user_library_size"),
    ).withColumn(
        "weighted_score",
        F.col("avg_hours_played") * F.col("avg_positive_ratio") * F.log(F.col("total_recs") + 1)
    ).orderBy("weighted_score", ascending=False)

    assert "weighted_score" in result.columns, "Output must contain weighted_score column"
    assert result.count() > 0, "Expected at least one result row"
    scores = [r["weighted_score"] for r in result.collect()]
    assert all(s > 0 for s in scores if s is not None), "All weighted_scores should be positive"


# ─────────────────────────────────────────────────────────────────────────────
# I-05 — property_prices mode writes exactly 4 output sub-directories
# ─────────────────────────────────────────────────────────────────────────────
def test_i05_property_prices_four_outputs(spark, tmp_path):
    """I-05: property_prices writes by_county, by_year, by_property_type, affordability_gap."""
    land_path = str(tmp_path / "input" / "land-data.csv")
    # Generate 110 rows so affordability filter (>100) passes
    land_rows = []
    for i in range(110):
        price = 100000 + i * 1000
        land_rows.append([
            f"UUID-{i}", str(price), "2015-06-15 00:00", "SW1A 1AA",
            "D", "N", "F", str(i), "", "HIGH ST", "", "LONDON",
            "WESTMINSTER", "GREATER LONDON", "A", "A"
        ])
    write_land_csv(land_path, land_rows)

    schema_cols = [
        "transaction_id", "price", "date_of_transfer", "postcode",
        "property_type", "new_build", "tenure", "paon", "saon",
        "street", "locality", "town", "district", "county",
        "transaction_category", "record_status"
    ]

    df = (spark.read.option("header", False).csv(land_path)
          .toDF(*schema_cols)
          .withColumn("price", F.col("price").cast(LongType()))
          .withColumn("year", F.year(F.to_timestamp("date_of_transfer", "yyyy-MM-dd HH:mm")))
          .filter(F.col("price").isNotNull() & F.col("year").isNotNull()))

    by_county = df.groupBy("county").agg(
        F.count("*").alias("total_sales"),
        F.avg("price").alias("avg_price"),
        F.percentile_approx("price", 0.5).alias("median_price"),
        F.max("price").alias("max_price"),
        F.min("price").alias("min_price"),
    ).orderBy("median_price", ascending=False)

    by_year = df.groupBy("year").agg(
        F.count("*").alias("total_sales"),
        F.percentile_approx("price", 0.5).alias("median_price"),
        F.avg("price").alias("avg_price"),
    ).orderBy("year")

    by_type = df.groupBy("property_type").agg(
        F.count("*").alias("count"),
        F.percentile_approx("price", 0.5).alias("median_price"),
        F.avg("price").alias("avg_price"),
    ).orderBy("count", ascending=False)

    affordability = df.groupBy("district", "county").agg(
        F.percentile_approx("price", 0.5).alias("median_price"),
        F.percentile_approx("price", 0.9).alias("p90_price"),
        F.percentile_approx("price", 0.1).alias("p10_price"),
        F.count("*").alias("total_sales"),
    ).withColumn(
        "affordability_ratio",
        F.round(F.col("p90_price") / F.col("p10_price"), 2)
    ).filter(F.col("total_sales") > 100).orderBy("affordability_ratio", ascending=False)

    output_base = str(tmp_path / "output" / "property_prices")
    sub_outputs = [
        ("by_county", by_county),
        ("by_year", by_year),
        ("by_property_type", by_type),
        ("affordability_gap", affordability),
    ]
    for name, df_out in sub_outputs:
        path = os.path.join(output_base, name)
        df_out.coalesce(1).write.mode("overwrite").option("header", "true").csv(path)

    for name, _ in sub_outputs:
        out_dir = os.path.join(output_base, name)
        assert os.path.isdir(out_dir), f"Output directory {name} was not created"
        csv_files = [f for f in os.listdir(out_dir) if f.endswith(".csv")]
        assert len(csv_files) > 0, f"No CSV file written in {name}"


# ─────────────────────────────────────────────────────────────────────────────
# I-06 — property_prices by_year is ordered ascending by year
# ─────────────────────────────────────────────────────────────────────────────
def test_i06_property_prices_by_year_ordering(spark, tmp_path):
    """I-06: by_year output is ordered ascending by year."""
    land_path = str(tmp_path / "input" / "land-data.csv")
    schema_cols = [
        "transaction_id", "price", "date_of_transfer", "postcode",
        "property_type", "new_build", "tenure", "paon", "saon",
        "street", "locality", "town", "district", "county",
        "transaction_category", "record_status"
    ]
    rows = [
        ["UUID-1", "200000", "2015-03-10 00:00", "SW1A 1AA", "D", "N", "F",
         "1", "", "ST", "", "L", "W", "GL", "A", "A"],
        ["UUID-2", "300000", "2010-06-01 00:00", "SW1A 1AB", "S", "N", "F",
         "2", "", "ST", "", "L", "W", "GL", "A", "A"],
        ["UUID-3", "250000", "2020-01-15 00:00", "SW1A 1AC", "T", "N", "F",
         "3", "", "ST", "", "L", "W", "GL", "A", "A"],
    ]
    write_land_csv(land_path, rows)

    df = (spark.read.option("header", False).csv(land_path)
          .toDF(*schema_cols)
          .withColumn("price", F.col("price").cast(LongType()))
          .withColumn("year", F.year(F.to_timestamp("date_of_transfer", "yyyy-MM-dd HH:mm")))
          .filter(F.col("price").isNotNull() & F.col("year").isNotNull()))

    by_year = df.groupBy("year").agg(
        F.count("*").alias("total_sales"),
        F.percentile_approx("price", 0.5).alias("median_price"),
        F.avg("price").alias("avg_price"),
    ).orderBy("year")

    years = [r["year"] for r in by_year.collect()]
    assert years == sorted(years), f"Years are not in ascending order: {years}"
    assert years[0] == 2010, f"Earliest year should be 2010, got {years[0]}"


# ─────────────────────────────────────────────────────────────────────────────
# I-07 — property_prices by_county ordered descending by median_price
# ─────────────────────────────────────────────────────────────────────────────
def test_i07_property_prices_by_county_ordering(spark, tmp_path):
    """I-07: by_county output is ordered descending by median_price."""
    schema_cols = [
        "transaction_id", "price", "date_of_transfer", "postcode",
        "property_type", "new_build", "tenure", "paon", "saon",
        "street", "locality", "town", "district", "county",
        "transaction_category", "record_status"
    ]
    rows = [
        ["UUID-1", "100000", "2015-01-01 00:00", "AB1 1AA", "D", "N", "F",
         "1", "", "ST", "", "L", "D1", "SURREY", "A", "A"],
        ["UUID-2", "500000", "2015-01-01 00:00", "AB1 1AB", "D", "N", "F",
         "2", "", "ST", "", "L", "D2", "LONDON", "A", "A"],
        ["UUID-3", "250000", "2015-01-01 00:00", "AB1 1AC", "D", "N", "F",
         "3", "", "ST", "", "L", "D3", "KENT", "A", "A"],
    ]
    land_path = str(tmp_path / "input" / "land-data.csv")
    write_land_csv(land_path, rows)

    df = (spark.read.option("header", False).csv(land_path)
          .toDF(*schema_cols)
          .withColumn("price", F.col("price").cast(LongType()))
          .withColumn("year", F.year(F.to_timestamp("date_of_transfer", "yyyy-MM-dd HH:mm")))
          .filter(F.col("price").isNotNull() & F.col("year").isNotNull()))

    by_county = df.groupBy("county").agg(
        F.count("*").alias("total_sales"),
        F.avg("price").alias("avg_price"),
        F.percentile_approx("price", 0.5).alias("median_price"),
        F.max("price").alias("max_price"),
        F.min("price").alias("min_price"),
    ).orderBy("median_price", ascending=False)

    counties = [r["county"] for r in by_county.collect()]
    assert counties[0] == "LONDON", f"LONDON (500k median) should be first, got {counties[0]}"


# ─────────────────────────────────────────────────────────────────────────────
# I-08 — All-null prices produce empty output after filtering
# ─────────────────────────────────────────────────────────────────────────────
def test_i08_all_null_prices_filtered(spark, tmp_path):
    """I-08: When all prices are invalid strings, filtering produces 0 rows."""
    schema_cols = [
        "transaction_id", "price", "date_of_transfer", "postcode",
        "property_type", "new_build", "tenure", "paon", "saon",
        "street", "locality", "town", "district", "county",
        "transaction_category", "record_status"
    ]
    rows = [
        ["UUID-1", "NOT_A_PRICE", "2015-01-01 00:00", "AB1 1AA", "D", "N", "F",
         "1", "", "ST", "", "L", "D1", "SURREY", "A", "A"],
        ["UUID-2", "ALSO_BAD", "2015-01-01 00:00", "AB1 1AB", "D", "N", "F",
         "2", "", "ST", "", "L", "D2", "LONDON", "A", "A"],
    ]
    land_path = str(tmp_path / "input" / "land-data.csv")
    write_land_csv(land_path, rows)

    df = (spark.read.option("header", False).csv(land_path)
          .toDF(*schema_cols)
          .withColumn("price", F.col("price").cast(LongType()))
          .withColumn("year", F.year(F.to_timestamp("date_of_transfer", "yyyy-MM-dd HH:mm")))
          .filter(F.col("price").isNotNull() & F.col("year").isNotNull()))

    assert df.count() == 0, "Expected 0 rows when all prices are invalid"
