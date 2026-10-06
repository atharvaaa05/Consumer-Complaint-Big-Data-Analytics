import sys
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum as spark_sum,
    avg,
    max as spark_max,
    min as spark_min,
    lag,
    round as spark_round
)
from pyspark.sql.window import Window


# --------------------------------------------------
# 1. Create Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("ConsumerComplaintBDA_TimeSeries")
    .master("local[*]")
    .config("spark.pyspark.python", sys.executable)
    .config("spark.pyspark.driver.python", sys.executable)
    .config(
        "spark.local.dir",
        r"C:\Users\Atharva\Desktop\Consumer Complaint Analytics\data\spark_temp"
    )
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
    r"\results\time_series"
)

os.makedirs(OUTPUT_PATH, exist_ok=True)


# --------------------------------------------------
# 3. Read processed Parquet
# --------------------------------------------------

print("\n==============================================")
print("TIME-SERIES ANALYTICS")
print("==============================================")

df = spark.read.parquet(PARQUET_PATH)

print("Total records:", df.count())


# ==================================================
# 4. MONTHLY COMPLAINT TREND
# ==================================================

print("\n==============================================")
print("MONTHLY COMPLAINT TREND")
print("==============================================")

monthly = (
    df.groupBy("Year", "Month")
    .agg(
        count("*").alias("Complaint_Count")
    )
    .orderBy("Year", "Month")
)

monthly.show(50, truncate=False)


# ==================================================
# 5. YEARLY COMPLAINT TREND
# ==================================================

print("\n==============================================")
print("YEARLY COMPLAINT TREND")
print("==============================================")

yearly = (
    df.groupBy("Year")
    .agg(
        count("*").alias("Complaint_Count")
    )
    .orderBy("Year")
)

yearly.show(30, truncate=False)


# ==================================================
# 6. YEAR-OVER-YEAR GROWTH
# ==================================================

print("\n==============================================")
print("YEAR-OVER-YEAR GROWTH")
print("==============================================")

year_window = Window.orderBy("Year")

yearly_yoy = (
    yearly
    .withColumn(
        "Previous_Year_Complaints",
        lag("Complaint_Count").over(year_window)
    )
    .withColumn(
        "YoY_Growth_Percentage",
        spark_round(
            (
                (
                    col("Complaint_Count")
                    - col("Previous_Year_Complaints")
                )
                / col("Previous_Year_Complaints")
            ) * 100,
            2
        )
    )
)

yearly_yoy.show(30, truncate=False)


# ==================================================
# 7. THREE-MONTH ROLLING AVERAGE
# ==================================================

print("\n==============================================")
print("3-MONTH ROLLING AVERAGE")
print("==============================================")

monthly_window = (
    Window
    .orderBy("Year", "Month")
    .rowsBetween(-2, Window.currentRow)
)

monthly_rolling = (
    monthly
    .withColumn(
        "Three_Month_Rolling_Average",
        spark_round(
            avg("Complaint_Count").over(monthly_window),
            2
        )
    )
)

monthly_rolling.show(50, truncate=False)


# ==================================================
# 8. PEAK COMPLAINT MONTHS
# ==================================================

print("\n==============================================")
print("PEAK COMPLAINT MONTHS")
print("==============================================")

peak_months = (
    monthly
    .orderBy(
        col("Complaint_Count").desc()
    )
)

peak_months.show(10, truncate=False)


# ==================================================
# 9. LOWEST COMPLAINT MONTHS
# ==================================================

print("\n==============================================")
print("LOWEST COMPLAINT MONTHS")
print("==============================================")

lowest_months = (
    monthly
    .orderBy(
        col("Complaint_Count").asc()
    )
)

lowest_months.show(10, truncate=False)


# --------------------------------------------------
# 8. Seasonal / Month Pattern
# --------------------------------------------------

print("\n" + "=" * 50)
print("SEASONAL / MONTH PATTERN")
print("=" * 50)

seasonal = (
    df.groupBy("Month")
    .agg(
        count("*").alias("Complaint_Count")
    )
    .orderBy(col("Month"))
)

seasonal.show(12, truncate=False)

# Save seasonal results
seasonal.write.mode("overwrite").parquet(
    OUTPUT_PATH + "/seasonal_pattern"
)


# ==================================================
# 11. MONTHLY SHARE OF TOTAL COMPLAINTS
# ==================================================

print("\n==============================================")
print("MONTHLY SHARE OF TOTAL")
print("==============================================")

total_records = df.count()

monthly_share = (
    monthly
    .withColumn(
        "Percentage_of_Total",
        spark_round(
            (
                col("Complaint_Count")
                / total_records
            ) * 100,
            2
        )
    )
)

monthly_share.show(50, truncate=False)


# ==================================================
# 12. YEAR-MONTH PEAK ANALYSIS
# ==================================================

print("\n==============================================")
print("YEAR-MONTH PEAK ANALYSIS")
print("==============================================")

highest_month = (
    monthly
    .orderBy(
        col("Complaint_Count").desc()
    )
    .limit(1)
)

print("Highest complaint month:")
highest_month.show(truncate=False)


# ==================================================
# 13. SAVE RESULTS
# ==================================================

print("\n==============================================")
print("SAVING TIME-SERIES RESULTS")
print("==============================================")

monthly.write.mode("overwrite").parquet(
    OUTPUT_PATH + r"\monthly_trend"
)

yearly_yoy.write.mode("overwrite").parquet(
    OUTPUT_PATH + r"\yearly_yoy"
)

monthly_rolling.write.mode("overwrite").parquet(
    OUTPUT_PATH + r"\monthly_rolling"
)

peak_months.write.mode("overwrite").parquet(
    OUTPUT_PATH + r"\peak_months"
)

seasonal.write.mode("overwrite").parquet(
    OUTPUT_PATH + r"\seasonal"
)

monthly_share.write.mode("overwrite").parquet(
    OUTPUT_PATH + r"\monthly_share"
)


# ==================================================
# 14. FINISH
# ==================================================

spark.stop()

print("\n==============================================")
print("TIME-SERIES ANALYTICS COMPLETED")
print("==============================================")