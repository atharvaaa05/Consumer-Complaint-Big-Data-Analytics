import sys
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum as spark_sum,
    lag,
    round as spark_round,
    when
)
from pyspark.sql.window import Window


# --------------------------------------------------
# 1. Create Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("ConsumerComplaintBDA_WindowAnalytics")
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
    r"\results\window_analytics"
)

os.makedirs(OUTPUT_PATH, exist_ok=True)


# --------------------------------------------------
# 3. Read Parquet
# --------------------------------------------------

print("\n==============================================")
print("SPARK WINDOW FUNCTION ANALYTICS")
print("==============================================")

df = spark.read.parquet(PARQUET_PATH)

print("Total records:", df.count())


# ==================================================
# 4. PRODUCT RANKING
# ==================================================

print("\n==============================================")
print("PRODUCT RANKING")
print("==============================================")

product_counts = (
    df.groupBy("Product")
    .agg(count("*").alias("Complaint_Count"))
)

product_window = Window.orderBy(
    col("Complaint_Count").desc()
)

product_ranking = (
    product_counts
    .withColumn(
        "Rank",
        spark_sum(col("Complaint_Count")).over(
            Window.rowsBetween(
                Window.unboundedPreceding,
                Window.currentRow
            )
        )
    )
)

# Use row_number-style ranking separately
from pyspark.sql.functions import row_number

product_ranking = (
    product_counts
    .withColumn(
        "Rank",
        row_number().over(product_window)
    )
    .orderBy("Rank")
)

product_ranking.show(20, truncate=False)


# ==================================================
# 5. COMPANY RANKING WITHIN PRODUCT
# ==================================================

print("\n==============================================")
print("COMPANY RANKING WITHIN PRODUCT")
print("==============================================")

company_product_counts = (
    df.groupBy(
        "Product",
        "Company"
    )
    .agg(
        count("*").alias("Complaint_Count")
    )
)

company_product_window = Window.partitionBy(
    "Product"
).orderBy(
    col("Complaint_Count").desc()
)

company_product_ranking = (
    company_product_counts
    .withColumn(
        "Company_Rank",
        row_number().over(company_product_window)
    )
    .filter(
        col("Company_Rank") <= 5
    )
    .orderBy(
        "Product",
        "Company_Rank"
    )
)

company_product_ranking.show(50, truncate=False)


# ==================================================
# 6. RUNNING CUMULATIVE COMPLAINT COUNT
# ==================================================

print("\n==============================================")
print("RUNNING CUMULATIVE COMPLAINT COUNT")
print("==============================================")

year_counts = (
    df.groupBy("Year")
    .agg(
        count("*").alias("Complaint_Count")
    )
    .orderBy("Year")
)

running_window = (
    Window
    .orderBy("Year")
    .rowsBetween(
        Window.unboundedPreceding,
        Window.currentRow
    )
)

running_total = (
    year_counts
    .withColumn(
        "Cumulative_Complaints",
        spark_sum("Complaint_Count").over(running_window)
    )
)

running_total.show(30, truncate=False)


# ==================================================
# 7. MONTHLY COMPLAINT COUNTS
# ==================================================

print("\n==============================================")
print("MONTHLY COMPLAINT COUNTS")
print("==============================================")

monthly_counts = (
    df.groupBy(
        "Year",
        "Month"
    )
    .agg(
        count("*").alias("Complaint_Count")
    )
)


# ==================================================
# 8. PREVIOUS MONTH USING LAG
# ==================================================

print("\n==============================================")
print("PREVIOUS MONTH COMPARISON")
print("==============================================")

monthly_window = Window.orderBy(
    "Year",
    "Month"
)

monthly_lag = (
    monthly_counts
    .withColumn(
        "Previous_Month_Complaints",
        lag("Complaint_Count").over(monthly_window)
    )
    .orderBy(
        "Year",
        "Month"
    )
)

monthly_lag.show(50, truncate=False)


# ==================================================
# 9. MONTH-OVER-MONTH GROWTH
# ==================================================

print("\n==============================================")
print("MONTH-OVER-MONTH GROWTH")
print("==============================================")

monthly_growth = (
    monthly_lag
    .withColumn(
        "MoM_Growth_Percentage",
        spark_round(
            when(
                col("Previous_Month_Complaints").isNotNull()
                & (col("Previous_Month_Complaints") != 0),
                (
                    (
                        col("Complaint_Count")
                        - col("Previous_Month_Complaints")
                    )
                    / col("Previous_Month_Complaints")
                ) * 100
            ),
            2
        )
    )
)

monthly_growth.show(50, truncate=False)


# ==================================================
# 10. PERCENTAGE CONTRIBUTION
# ==================================================

print("\n==============================================")
print("PRODUCT PERCENTAGE CONTRIBUTION")
print("==============================================")

total_complaints = df.count()

product_contribution = (
    product_counts
    .withColumn(
        "Percentage_of_Total",
        spark_round(
            (
                col("Complaint_Count")
                / total_complaints
            ) * 100,
            2
        )
    )
    .orderBy(
        col("Complaint_Count").desc()
    )
)

product_contribution.show(20, truncate=False)


# ==================================================
# 11. CUMULATIVE PERCENTAGE CONTRIBUTION
# ==================================================

print("\n==============================================")
print("CUMULATIVE PERCENTAGE CONTRIBUTION")
print("==============================================")

contribution_window = (
    Window
    .orderBy(
        col("Complaint_Count").desc()
    )
    .rowsBetween(
        Window.unboundedPreceding,
        Window.currentRow
    )
)

pareto_data = (
    product_contribution
    .withColumn(
        "Cumulative_Percentage",
        spark_round(
            spark_sum("Percentage_of_Total")
            .over(contribution_window),
            2
        )
    )
)

pareto_data.show(20, truncate=False)


# ==================================================
# 12. SAVE RESULTS
# ==================================================

print("\n==============================================")
print("SAVING WINDOW ANALYTICS")
print("==============================================")

product_ranking.write.mode("overwrite").parquet(
    OUTPUT_PATH + r"\product_ranking"
)

company_product_ranking.write.mode("overwrite").parquet(
    OUTPUT_PATH + r"\company_product_ranking"
)

running_total.write.mode("overwrite").parquet(
    OUTPUT_PATH + r"\running_total"
)

monthly_growth.write.mode("overwrite").parquet(
    OUTPUT_PATH + r"\monthly_growth"
)

pareto_data.write.mode("overwrite").parquet(
    OUTPUT_PATH + r"\pareto_data"
)


# ==================================================
# 13. FINISH
# ==================================================

spark.stop()

print("\n==============================================")
print("WINDOW ANALYTICS COMPLETED")
print("==============================================")