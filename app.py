import sys
import os
from pyspark.sql import SparkSession
import time

spark = SparkSession.builder \
    .appName("steam-recommendation-analysis") \
    .getOrCreate()

data_dir = os.environ.get("DATA_PATH", "data")
data_path = os.path.join(data_dir, "recommendations.csv")
df = spark.read.option("header", True).csv(data_path)

df = df.repartition(300)

mode = sys.argv[1] if len(sys.argv) > 1 else "top_games"

if mode == "top_games":
    result = df.groupBy("app_id").count()

elif mode == "sentiment":
    result = df.groupBy("is_recommended").count()

elif mode == "user_activity":
    result = df.groupBy("user_id").count()

else:
    raise ValueError("Unknown analysis mode")

# Force execution and write output
output_path = os.path.join(data_dir, "output", mode)
print(f"Writing results to {output_path}")
result.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

# Keep job alive to observe scale-down
time.sleep(120)

spark.stop()