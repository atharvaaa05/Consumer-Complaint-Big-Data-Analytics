import sys
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    desc,
    sum as spark_sum
)

# --------------------------------------------------
# 1. Create Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("ConsumerComplaintBDA_SparkSQL")
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
    r"\results\spark_sql"
)

os.makedirs(OUTPUT_PATH, exist_ok=True)


# --------------------------------------------------
# 3. Read Parquet
# --------------------------------------------------

print("\n==============================================")
print("SPARK SQL ANALYTICS")
print("==============================================")

print("\nReading processed Parquet...")

df = spark.read.parquet(PARQUET_PATH)

print("Total records:", df.count())


# --------------------------------------------------
# 4. Register Spark SQL temporary view
# --------------------------------------------------

df.createOrReplaceTempView("complaints")

print("\nSpark SQL view 'complaints' created.")


# --------------------------------------------------
# 5. TOP PRODUCTS
# --------------------------------------------------

print("\n==============================================")
print("TOP COMPLAINT PRODUCTS")
print("==============================================")

top_products = spark.sql("""
    SELECT
        Product,
        COUNT(*) AS Complaint_Count
    FROM complaints
    GROUP BY Product
    ORDER BY Complaint_Count DESC
""")

top_products.show(20, truncate=False)


# --------------------------------------------------
# 6. TOP ISSUES
# --------------------------------------------------

print("\n==============================================")
print("TOP COMPLAINT ISSUES")
print("==============================================")

top_issues = spark.sql("""
    SELECT
        Issue,
        COUNT(*) AS Complaint_Count
    FROM complaints
    GROUP BY Issue
    ORDER BY Complaint_Count DESC
    LIMIT 20
""")

top_issues.show(20, truncate=False)


# --------------------------------------------------
# 7. TOP COMPANIES
# --------------------------------------------------

print("\n==============================================")
print("TOP COMPANIES BY COMPLAINTS")
print("==============================================")

top_companies = spark.sql("""
    SELECT
        Company,
        COUNT(*) AS Complaint_Count
    FROM complaints
    GROUP BY Company
    ORDER BY Complaint_Count DESC
    LIMIT 20
""")

top_companies.show(20, truncate=False)


# --------------------------------------------------
# 8. STATE-WISE ANALYSIS
# --------------------------------------------------

print("\n==============================================")
print("TOP STATES BY COMPLAINTS")
print("==============================================")

state_analysis = spark.sql("""
    SELECT
        State,
        COUNT(*) AS Complaint_Count
    FROM complaints
    WHERE State IS NOT NULL
    GROUP BY State
    ORDER BY Complaint_Count DESC
""")

state_analysis.show(20, truncate=False)


# --------------------------------------------------
# 9. SUBMISSION METHOD
# --------------------------------------------------

print("\n==============================================")
print("SUBMISSION METHOD ANALYSIS")
print("==============================================")

submission_analysis = spark.sql("""
    SELECT
        `Submitted via` AS Submission_Method,
        COUNT(*) AS Complaint_Count
    FROM complaints
    GROUP BY `Submitted via`
    ORDER BY Complaint_Count DESC
""")

submission_analysis.show(20, truncate=False)


# --------------------------------------------------
# 10. COMPANY RESPONSE
# --------------------------------------------------

print("\n==============================================")
print("COMPANY RESPONSE ANALYSIS")
print("==============================================")

response_analysis = spark.sql("""
    SELECT
        `Company response to consumer` AS Company_Response,
        COUNT(*) AS Complaint_Count
    FROM complaints
    GROUP BY `Company response to consumer`
    ORDER BY Complaint_Count DESC
""")

response_analysis.show(20, truncate=False)


# --------------------------------------------------
# 11. TIMELY RESPONSE
# --------------------------------------------------

print("\n==============================================")
print("TIMELY RESPONSE ANALYSIS")
print("==============================================")

timely_analysis = spark.sql("""
    SELECT
        `Timely response?` AS Timely_Response,
        COUNT(*) AS Complaint_Count
    FROM complaints
    GROUP BY `Timely response?`
    ORDER BY Complaint_Count DESC
""")

timely_analysis.show(20, truncate=False)


# --------------------------------------------------
# 12. YEAR-WISE ANALYSIS
# --------------------------------------------------

print("\n==============================================")
print("YEAR-WISE COMPLAINT VOLUME")
print("==============================================")

year_analysis = spark.sql("""
    SELECT
        Year,
        COUNT(*) AS Complaint_Count
    FROM complaints
    GROUP BY Year
    ORDER BY Year
""")

year_analysis.show(30, truncate=False)


# --------------------------------------------------
# 13. MONTH-WISE ANALYSIS
# --------------------------------------------------

print("\n==============================================")
print("MONTH-WISE COMPLAINT VOLUME")
print("==============================================")

month_analysis = spark.sql("""
    SELECT
        Year,
        Month,
        COUNT(*) AS Complaint_Count
    FROM complaints
    GROUP BY Year, Month
    ORDER BY Year, Month
""")

month_analysis.show(100, truncate=False)


# --------------------------------------------------
# 14. PRODUCT × STATE ANALYSIS
# --------------------------------------------------

print("\n==============================================")
print("PRODUCT × STATE ANALYSIS")
print("==============================================")

product_state = spark.sql("""
    SELECT
        Product,
        State,
        COUNT(*) AS Complaint_Count
    FROM complaints
    WHERE State IS NOT NULL
    GROUP BY Product, State
    ORDER BY Complaint_Count DESC
    LIMIT 30
""")

product_state.show(30, truncate=False)


# --------------------------------------------------
# 15. PRODUCT × SUBMISSION METHOD
# --------------------------------------------------

print("\n==============================================")
print("PRODUCT × SUBMISSION METHOD")
print("==============================================")

product_submission = spark.sql("""
    SELECT
        Product,
        `Submitted via` AS Submission_Method,
        COUNT(*) AS Complaint_Count
    FROM complaints
    GROUP BY Product, `Submitted via`
    ORDER BY Complaint_Count DESC
    LIMIT 30
""")

product_submission.show(30, truncate=False)


# --------------------------------------------------
# 16. PRODUCT × COMPANY RESPONSE
# --------------------------------------------------

print("\n==============================================")
print("PRODUCT × COMPANY RESPONSE")
print("==============================================")

product_response = spark.sql("""
    SELECT
        Product,
        `Company response to consumer` AS Company_Response,
        COUNT(*) AS Complaint_Count
    FROM complaints
    GROUP BY Product, `Company response to consumer`
    ORDER BY Complaint_Count DESC
    LIMIT 30
""")

product_response.show(30, truncate=False)


# --------------------------------------------------
# 17. SAVE RESULTS AS PARQUET
# --------------------------------------------------

print("\n==============================================")
print("SAVING SQL RESULTS")
print("==============================================")

top_products.write.mode("overwrite").parquet(
    OUTPUT_PATH + r"\top_products"
)

top_issues.write.mode("overwrite").parquet(
    OUTPUT_PATH + r"\top_issues"
)

top_companies.write.mode("overwrite").parquet(
    OUTPUT_PATH + r"\top_companies"
)

state_analysis.write.mode("overwrite").parquet(
    OUTPUT_PATH + r"\state_analysis"
)

submission_analysis.write.mode("overwrite").parquet(
    OUTPUT_PATH + r"\submission_analysis"
)

response_analysis.write.mode("overwrite").parquet(
    OUTPUT_PATH + r"\response_analysis"
)

timely_analysis.write.mode("overwrite").parquet(
    OUTPUT_PATH + r"\timely_analysis"
)

year_analysis.write.mode("overwrite").parquet(
    OUTPUT_PATH + r"\year_analysis"
)

month_analysis.write.mode("overwrite").parquet(
    OUTPUT_PATH + r"\month_analysis"
)

product_state.write.mode("overwrite").parquet(
    OUTPUT_PATH + r"\product_state"
)

product_submission.write.mode("overwrite").parquet(
    OUTPUT_PATH + r"\product_submission"
)

product_response.write.mode("overwrite").parquet(
    OUTPUT_PATH + r"\product_response"
)


# --------------------------------------------------
# 18. Finish
# --------------------------------------------------

spark.stop()

print("\n==============================================")
print("SPARK SQL ANALYTICS COMPLETED")
print("==============================================")