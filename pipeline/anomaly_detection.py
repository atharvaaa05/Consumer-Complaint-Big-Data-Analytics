import sys
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    avg,
    stddev,
    round as spark_round
)


# --------------------------------------------------
# 1. Create Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("ConsumerComplaintBDA_AnomalyDetection")
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
    r"\results\anomaly"
)

os.makedirs(OUTPUT_PATH, exist_ok=True)


# --------------------------------------------------
# 3. Read processed Parquet
# --------------------------------------------------

print("\n" + "=" * 50)
print("STATISTICAL ANOMALY DETECTION")
print("=" * 50)

print("\nReading processed Parquet...")

df = spark.read.parquet(PARQUET_PATH)

print("Total records:", df.count())


# --------------------------------------------------
# 4. Monthly complaint counts
# --------------------------------------------------

print("\n" + "=" * 50)
print("MONTHLY COMPLAINT VOLUME")
print("=" * 50)

monthly = (
    df.groupBy("Year", "Month")
      .agg(count("*").alias("Complaint_Count"))
      .orderBy("Year", "Month")
)

monthly.show(50, truncate=False)


# --------------------------------------------------
# 5. Calculate mean and standard deviation
# --------------------------------------------------

statistics = monthly.agg(
    avg("Complaint_Count").alias("Mean_Complaints"),
    stddev("Complaint_Count").alias("StdDev_Complaints")
).collect()[0]

mean_complaints = statistics["Mean_Complaints"]
std_complaints = statistics["StdDev_Complaints"]

print("\nMean monthly complaints:", mean_complaints)
print("Standard deviation:", std_complaints)


# --------------------------------------------------
# 6. Calculate Z-score
# --------------------------------------------------

anomalies = (
    monthly
    .withColumn(
        "Z_Score",
        spark_round(
            (col("Complaint_Count") - mean_complaints)
            / std_complaints,
            2
        )
    )
)


# --------------------------------------------------
# 7. Identify anomalies
# --------------------------------------------------

# |Z| >= 2 is treated as a statistical anomaly

anomalies = (
    anomalies
    .withColumn(
        "Anomaly",
        (col("Z_Score") >= 2) |
        (col("Z_Score") <= -2)
    )
)


# --------------------------------------------------
# 8. Display all anomalies
# --------------------------------------------------

print("\n" + "=" * 50)
print("DETECTED ANOMALIES")
print("=" * 50)

detected = (
    anomalies
    .filter(col("Anomaly") == True)
    .orderBy(col("Complaint_Count").desc())
)

detected.show(30, truncate=False)


# --------------------------------------------------
# 9. High-volume anomalies
# --------------------------------------------------

print("\n" + "=" * 50)
print("HIGH-VOLUME ANOMALIES")
print("=" * 50)

high_anomalies = (
    anomalies
    .filter(col("Z_Score") >= 2)
    .orderBy(col("Z_Score").desc())
)

high_anomalies.show(20, truncate=False)


# --------------------------------------------------
# 10. Low-volume anomalies
# --------------------------------------------------

print("\n" + "=" * 50)
print("LOW-VOLUME ANOMALIES")
print("=" * 50)

low_anomalies = (
    anomalies
    .filter(col("Z_Score") <= -2)
    .orderBy(col("Z_Score").asc())
)

low_anomalies.show(20, truncate=False)


# --------------------------------------------------
# 11. Anomaly summary
# --------------------------------------------------

total_anomalies = detected.count()
high_count = high_anomalies.count()
low_count = low_anomalies.count()

print("\n" + "=" * 50)
print("ANOMALY SUMMARY")
print("=" * 50)

print("Total anomalous months:", total_anomalies)
print("High-volume anomalies:", high_count)
print("Low-volume anomalies:", low_count)


# --------------------------------------------------
# 12. Save complete anomaly analysis
# --------------------------------------------------

print("\n" + "=" * 50)
print("SAVING ANOMALY RESULTS")
print("=" * 50)

anomalies.write.mode("overwrite").parquet(
    OUTPUT_PATH + r"\monthly_anomalies"
)

detected.write.mode("overwrite").parquet(
    OUTPUT_PATH + r"\detected_anomalies"
)


# --------------------------------------------------
# 13. Completion
# --------------------------------------------------

print("\n" + "=" * 50)
print("ANOMALY DETECTION COMPLETED")
print("=" * 50)

spark.stop()