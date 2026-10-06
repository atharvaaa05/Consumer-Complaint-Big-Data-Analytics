import sys
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count


# --------------------------------------------------
# 1. Create Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("ConsumerComplaintBDA_Performance")
    .master("local[*]")
    .config("spark.pyspark.python", sys.executable)
    .config("spark.pyspark.driver.python", sys.executable)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# 2. Paths
# --------------------------------------------------

PARQUET_PATH = (
    r"C:\Users\Atharva\Desktop\Consumer Complaint Analytics"
    r"\data\processed\complaints_parquet_1million"
)

OUTPUT_PATH = (
    r"C:\Users\Atharva\Desktop\Consumer Complaint Analytics"
    r"\results\performance"
)

os.makedirs(OUTPUT_PATH, exist_ok=True)


# --------------------------------------------------
# 3. Start performance measurement
# --------------------------------------------------

print("\n" + "=" * 55)
print("BIG DATA PERFORMANCE ANALYSIS")
print("=" * 55)

print("\nReading Parquet dataset...")

start_time = time.time()

df = spark.read.parquet(PARQUET_PATH)

read_count = df.count()

read_time = time.time() - start_time

print("Records processed:", read_count)
print("Parquet read + count time:", round(read_time, 2), "seconds")


# --------------------------------------------------
# 4. Aggregation performance
# --------------------------------------------------

print("\n" + "=" * 55)
print("SPARK AGGREGATION PERFORMANCE")
print("=" * 55)

start_time = time.time()

product_analysis = (
    df.groupBy("Product")
      .agg(count("*").alias("Complaint_Count"))
      .orderBy(col("Complaint_Count").desc())
)

product_analysis.show(10, truncate=False)

aggregation_time = time.time() - start_time

print(
    "Aggregation + sorting time:",
    round(aggregation_time, 2),
    "seconds"
)


# --------------------------------------------------
# 5. Multidimensional query performance
# --------------------------------------------------

print("\n" + "=" * 55)
print("MULTIDIMENSIONAL QUERY PERFORMANCE")
print("=" * 55)

start_time = time.time()

multidimensional = (
    df.groupBy("Year", "Product", "State")
      .agg(count("*").alias("Complaint_Count"))
      .orderBy(col("Complaint_Count").desc())
)

multidimensional.show(20, truncate=False)

multi_time = time.time() - start_time

print(
    "Multidimensional query time:",
    round(multi_time, 2),
    "seconds"
)


# --------------------------------------------------
# 6. Partition information
# --------------------------------------------------

print("\n" + "=" * 55)
print("PARTITION INFORMATION")
print("=" * 55)

partition_count = df.rdd.getNumPartitions()

print("Spark partitions:", partition_count)


# --------------------------------------------------
# 7. Dataset statistics
# --------------------------------------------------

print("\n" + "=" * 55)
print("DATASET PERFORMANCE SUMMARY")
print("=" * 55)

print("Total records:", read_count)
print("Total columns:", len(df.columns))
print("Spark partitions:", partition_count)
print("Read + count time:", round(read_time, 2), "seconds")
print("Aggregation time:", round(aggregation_time, 2), "seconds")
print("Multidimensional query time:", round(multi_time, 2), "seconds")


# --------------------------------------------------
# 8. Save performance results
# --------------------------------------------------

performance_data = [
    ("Total Records", str(read_count)),
    ("Total Columns", str(len(df.columns))),
    ("Spark Partitions", str(partition_count)),
    ("Read + Count Time Seconds", str(round(read_time, 2))),
    ("Aggregation Time Seconds", str(round(aggregation_time, 2))),
    ("Multidimensional Query Time Seconds", str(round(multi_time, 2)))
]

performance_df = spark.createDataFrame(
    performance_data,
    ["Metric", "Value"]
)

performance_df.write.mode("overwrite").parquet(
    OUTPUT_PATH + r"\performance_metrics"
)


# --------------------------------------------------
# 9. Completion
# --------------------------------------------------

print("\n" + "=" * 55)
print("PERFORMANCE ANALYSIS COMPLETED")
print("=" * 55)

spark.stop()