import sys
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum as spark_sum,
    round as spark_round,
    desc
)

# ==================================================
# 1. CREATE SPARK SESSION
# ==================================================

spark = (
    SparkSession.builder
    .appName("ConsumerComplaintBDA_GeographicAnalytics")
    .master("local[*]")
    .config("spark.pyspark.python", sys.executable)
    .config("spark.pyspark.driver.python", sys.executable)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ==================================================
# 2. PATHS
# ==================================================

PARQUET_PATH = (
    r"C:\Users\Atharva\Desktop\Consumer Complaint Analytics"
    r"\data\processed\complaints_parquet_1million"
)

OUTPUT_PATH = (
    r"C:\Users\Atharva\Desktop\Consumer Complaint Analytics"
    r"\results\geographic"
)

os.makedirs(OUTPUT_PATH, exist_ok=True)

# ==================================================
# 3. READ PARQUET
# ==================================================

print("\n" + "=" * 50)
print("GEOGRAPHIC ANALYTICS")
print("=" * 50)

print("\nReading processed Parquet...")

df = spark.read.parquet(PARQUET_PATH)

print("Total records:", df.count())

# ==================================================
# 4. STATE-WISE COMPLAINT COUNT
# ==================================================

print("\n" + "=" * 50)
print("STATE-WISE COMPLAINT VOLUME")
print("=" * 50)

state_complaints = (
    df.filter(
        col("State").isNotNull() &
        (col("State") != "")
    )
    .groupBy("State")
    .agg(
        count("*").alias("Complaint_Count")
    )
    .orderBy(desc("Complaint_Count"))
)

state_complaints.show(20, truncate=False)

# ==================================================
# 5. STATE PERCENTAGE CONTRIBUTION
# ==================================================

print("\n" + "=" * 50)
print("STATE PERCENTAGE CONTRIBUTION")
print("=" * 50)

total_complaints = df.count()

state_percentage = (
    state_complaints
    .withColumn(
        "Percentage_of_Total",
        spark_round(
            col("Complaint_Count") * 100 / total_complaints,
            2
        )
    )
    .orderBy(desc("Complaint_Count"))
)

state_percentage.show(20, truncate=False)

# ==================================================
# 6. TOP 10 STATES
# ==================================================

print("\n" + "=" * 50)
print("TOP 10 STATES")
print("=" * 50)

top_states = (
    state_complaints
    .limit(10)
)

top_states.show(10, truncate=False)

# ==================================================
# 7. PRODUCT × STATE ANALYSIS
# ==================================================

print("\n" + "=" * 50)
print("PRODUCT × STATE ANALYSIS")
print("=" * 50)

product_state = (
    df.filter(
        col("State").isNotNull() &
        col("Product").isNotNull()
    )
    .groupBy("State", "Product")
    .agg(
        count("*").alias("Complaint_Count")
    )
    .orderBy(
        col("State"),
        desc("Complaint_Count")
    )
)

product_state.show(30, truncate=False)

# ==================================================
# 8. STATE × SUBMISSION METHOD
# ==================================================

print("\n" + "=" * 50)
print("STATE × SUBMISSION METHOD")
print("=" * 50)

state_submission = (
    df.filter(
        col("State").isNotNull() &
        col("Submitted via").isNotNull()
    )
    .groupBy("State", "Submitted via")
    .agg(
        count("*").alias("Complaint_Count")
    )
    .orderBy(
        col("State"),
        desc("Complaint_Count")
    )
)

state_submission.show(30, truncate=False)

# ==================================================
# 9. STATE × COMPANY RESPONSE
# ==================================================

print("\n" + "=" * 50)
print("STATE × COMPANY RESPONSE")
print("=" * 50)

state_response = (
    df.filter(
        col("State").isNotNull() &
        col("Company response to consumer").isNotNull()
    )
    .groupBy(
        "State",
        "Company response to consumer"
    )
    .agg(
        count("*").alias("Complaint_Count")
    )
    .orderBy(
        col("State"),
        desc("Complaint_Count")
    )
)

state_response.show(30, truncate=False)

# ==================================================
# 10. TOP PRODUCT IN EACH STATE
# ==================================================

print("\n" + "=" * 50)
print("TOP PRODUCT BY STATE")
print("=" * 50)

top_product_state = (
    df.filter(
        col("State").isNotNull() &
        col("Product").isNotNull()
    )
    .groupBy("State", "Product")
    .agg(
        count("*").alias("Complaint_Count")
    )
    .orderBy(
        col("State"),
        desc("Complaint_Count")
    )
)

top_product_state.show(30, truncate=False)

# ==================================================
# 11. SAVE RESULTS
# ==================================================

print("\n" + "=" * 50)
print("SAVING GEOGRAPHIC RESULTS")
print("=" * 50)

state_complaints.write.mode("overwrite").parquet(
    OUTPUT_PATH + r"\state_complaints"
)

state_percentage.write.mode("overwrite").parquet(
    OUTPUT_PATH + r"\state_percentage"
)

top_states.write.mode("overwrite").parquet(
    OUTPUT_PATH + r"\top_states"
)

product_state.write.mode("overwrite").parquet(
    OUTPUT_PATH + r"\product_state"
)

state_submission.write.mode("overwrite").parquet(
    OUTPUT_PATH + r"\state_submission"
)

state_response.write.mode("overwrite").parquet(
    OUTPUT_PATH + r"\state_response"
)

top_product_state.write.mode("overwrite").parquet(
    OUTPUT_PATH + r"\top_product_state"
)

# ==================================================
# 12. COMPLETED
# ==================================================

print("\n" + "=" * 50)
print("GEOGRAPHIC ANALYTICS COMPLETED")
print("=" * 50)

spark.stop()