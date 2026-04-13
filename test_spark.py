from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("metrics-visible-job").getOrCreate()

rdd = spark.sparkContext.parallelize(range(1000), 1000)

def slow_task(x):
    time.sleep(5)   # VERY IMPORTANT
    return x

# Force execution
rdd.foreach(slow_task)

# Keep driver alive briefly after work
time.sleep(60)

spark.stop()