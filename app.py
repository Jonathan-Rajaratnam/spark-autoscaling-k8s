import sys
import os
from pyspark.sql import SparkSession
import time

spark = SparkSession.builder \
    .appName("spark-benchmark") \
    .getOrCreate()

data_dir = os.environ.get("DATA_PATH", "data")

mode = sys.argv[1] if len(sys.argv) > 1 else "steam_heavy"

start_time = time.time()

# ──────────────────────────────────────────────────────────────
# MODE: top_games  (quick sanity-check)
# ──────────────────────────────────────────────────────────────
if mode == "top_games":
    df = spark.read.option("header", True) \
        .csv(os.path.join(data_dir, "games", "recommendations.csv")) \
        .repartition(300)
    result = df.groupBy("app_id").count().orderBy("count", ascending=False)

# ──────────────────────────────────────────────────────────────
# MODE: sentiment  (quick sanity-check)
# ──────────────────────────────────────────────────────────────
elif mode == "sentiment":
    df = spark.read.option("header", True) \
        .csv(os.path.join(data_dir, "games", "recommendations.csv")) \
        .repartition(300)
    result = df.groupBy("is_recommended").count()

# ──────────────────────────────────────────────────────────────
# MODE: user_activity  (quick sanity-check)
# ──────────────────────────────────────────────────────────────
elif mode == "user_activity":
    df = spark.read.option("header", True) \
        .csv(os.path.join(data_dir, "games", "recommendations.csv")) \
        .repartition(300)
    result = df.groupBy("user_id").count()

# ──────────────────────────────────────────────────────────────
# MODE: steam_heavy  (HEAVY – 3-table join + window functions)
# Reads all 3 Steam tables from data/games/  (~16M + 50K + 14M rows)
# 1. 3-way shuffle join: recommendations x games x users
# 2. Window function: rank games by hours played per user
# 3. Aggregation: weighted recommendation score per game
# ──────────────────────────────────────────────────────────────

elif mode == "steam_heavy":
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    from pyspark.sql.types import FloatType, LongType

    games_dir = os.path.join(data_dir, "games")

    recommendations = spark.read.option("header", True) \
        .csv(os.path.join(games_dir, "recommendations.csv")) \
        .repartition(500) \
        .withColumn("hours", F.col("hours").cast(FloatType())) \
        .withColumn("helpful", F.col("helpful").cast(LongType()))

    games = spark.read.option("header", True) \
        .csv(os.path.join(games_dir, "games.csv")) \
        .repartition(50) \
        .withColumn("positive_ratio", F.col("positive_ratio").cast(FloatType())) \
        .withColumn("price_final", F.col("price_final").cast(FloatType())) \
        .withColumn("user_reviews", F.col("user_reviews").cast(LongType()))

    users = spark.read.option("header", True) \
        .csv(os.path.join(games_dir, "users.csv")) \
        .repartition(200) \
        .withColumn("products", F.col("products").cast(LongType())) \
        .withColumn("reviews", F.col("reviews").cast(LongType()))

    # 3-way join
    enriched = recommendations \
        .join(games, "app_id", "inner") \
        .join(users, "user_id", "inner")

    # Window ranking: rank each game within a user's library by hours played
    user_window = Window.partitionBy("user_id").orderBy(F.col("hours").desc())
    ranked = enriched.withColumn("rank_in_library", F.rank().over(user_window))

    # Final aggregation calculation: weighted score = avg_hours * positive_ratio * log(recs+1)
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

# ──────────────────────────────────────────────────────────────
# MODE: property_prices  (HEAVY – UK Land Registry ~31M rows)
# Reads data/input/land-data.csv (no header row)
# Schema: UUID, price, date, postcode, property_type, new_build,
#         tenure, PAON, SAON, street, locality, town, district,
#         county, transaction_type, record_status
# Analyses:
#   1. Average & median price by county
#   2. Year-on-year national price trend
#   3. Property type breakdown (D/S/T/F)
#   4. Affordability gap ratio by district
# ──────────────────────────────────────────────────────────────
elif mode == "property_prices":
    from pyspark.sql import functions as F
    from pyspark.sql.types import LongType

    schema_cols = [
        "transaction_id", "price", "date_of_transfer", "postcode",
        "property_type", "new_build", "tenure", "paon", "saon",
        "street", "locality", "town", "district", "county",
        "transaction_category", "record_status"
    ]

    df = spark.read.option("header", False) \
        .csv(os.path.join(data_dir, "input", "land-data.csv")) \
        .toDF(*schema_cols) \
        .repartition(500) \
        .withColumn("price", F.col("price").cast(LongType())) \
        .withColumn("year", F.year(F.to_timestamp("date_of_transfer", "yyyy-MM-dd HH:mm"))) \
        .filter(F.col("price").isNotNull() & F.col("year").isNotNull())

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

    # Write all 4 sub-analyses (forces 4 separate computation passes)
    output_base = os.path.join(data_dir, "output", mode)
    for name, df_out in [
        ("by_county", by_county),
        ("by_year", by_year),
        ("by_property_type", by_type),
        ("affordability_gap", affordability),
    ]:
        path = os.path.join(output_base, name)
        print(f"Writing {name} -> {path}")
        df_out.coalesce(1).write.mode("overwrite").option("header", "true").csv(path)

    elapsed = time.time() - start_time
    print(f"[BENCHMARK] Job elapsed: {elapsed:.2f}s  mode={mode}")
    time.sleep(120)
    spark.stop()
    sys.exit(0)

else:
    raise ValueError(
        f"Unknown mode: {mode!r}. "
        f"Valid modes: top_games, sentiment, user_activity, steam_heavy, property_prices"
    )

# ──────────────────────────────────────────────────────────────
# Write output (all modes except property_prices which exits above)
# ──────────────────────────────────────────────────────────────
output_path = os.path.join(data_dir, "output", mode)
print(f"Writing results to {output_path}")
result.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

elapsed = time.time() - start_time
print(f"[BENCHMARK] Job elapsed: {elapsed:.2f}s  mode={mode}")

# Keep job alive to allow scale-down observation
time.sleep(120)

spark.stop()