import sys
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum as spark_sum,
    round as spark_round
)
from pyspark.sql.window import Window


# --------------------------------------------------
# 1. Create Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("ConsumerComplaintBDA_Pareto")
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
    r"\results\pareto"
)

os.makedirs(OUTPUT_PATH, exist_ok=True)


# --------------------------------------------------
# 3. Read processed Parquet
# --------------------------------------------------

print("\n" + "=" * 50)
print("PARETO ANALYSIS")
print("=" * 50)

print("\nReading processed Parquet...")

df = spark.read.parquet(PARQUET_PATH)

print("Total records:", df.count())


# --------------------------------------------------
# 4. Product-wise complaint counts
# --------------------------------------------------

print("\n" + "=" * 50)
print("PRODUCT COMPLAINT CONTRIBUTION")
print("=" * 50)

product_counts = (
    df.groupBy("Product")
      .agg(count("*").alias("Complaint_Count"))
      .orderBy(col("Complaint_Count").desc())
)

product_counts.show(20, truncate=False)


# --------------------------------------------------
# 5. Calculate percentage of total
# --------------------------------------------------

total_complaints = df.count()

pareto = (
    product_counts
    .withColumn(
        "Percentage_of_Total",
        spark_round(
            col("Complaint_Count") * 100 / total_complaints,
            2
        )
    )
)


# --------------------------------------------------
# 6. Calculate cumulative percentage
# --------------------------------------------------

window_spec = (
    Window
    .orderBy(col("Complaint_Count").desc())
    .rowsBetween(
        Window.unboundedPreceding,
        Window.currentRow
    )
)

pareto = (
    pareto
    .withColumn(
        "Cumulative_Complaints",
        spark_sum("Complaint_Count").over(window_spec)
    )
    .withColumn(
        "Cumulative_Percentage",
        spark_round(
            col("Cumulative_Complaints") * 100 / total_complaints,
            2
        )
    )
)


# --------------------------------------------------
# 7. Display Pareto results
# --------------------------------------------------

print("\n" + "=" * 50)
print("PARETO RESULTS")
print("=" * 50)

pareto.show(20, truncate=False)


# --------------------------------------------------
# 8. Find 80% Pareto point
# --------------------------------------------------

print("\n" + "=" * 50)
print("80% PARETO POINT")
print("=" * 50)

pareto_80 = (
    pareto
    .filter(col("Cumulative_Percentage") <= 80)
    .orderBy(col("Complaint_Count").desc())
)

pareto_80.show(20, truncate=False)


# --------------------------------------------------
# 9. Products needed to reach 80%
# --------------------------------------------------

products_to_80 = (
    pareto
    .filter(col("Cumulative_Percentage") <= 80)
    .count()
)

print(
    "Number of products contributing up to 80%:",
    products_to_80
)


# --------------------------------------------------
# 10. Save Pareto results
# --------------------------------------------------

print("\n" + "=" * 50)
print("SAVING PARETO RESULTS")
print("=" * 50)

pareto.write.mode("overwrite").parquet(
    OUTPUT_PATH + r"\product_pareto"
)

pareto_80.write.mode("overwrite").parquet(
    OUTPUT_PATH + r"\pareto_80_percent"
)


# --------------------------------------------------
# 11. Completion
# --------------------------------------------------

print("\n" + "=" * 50)
print("PARETO ANALYSIS COMPLETED")
print("=" * 50)

spark.stop()