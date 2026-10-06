import sys
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count


# --------------------------------------------------
# 1. Create Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("ConsumerComplaintBDA_Multidimensional")
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
    r"\results\multidimensional"
)

os.makedirs(OUTPUT_PATH, exist_ok=True)


# --------------------------------------------------
# 3. Read processed Parquet
# --------------------------------------------------

print("\n" + "=" * 50)
print("MULTIDIMENSIONAL ANALYSIS")
print("=" * 50)

print("\nReading processed Parquet...")

df = spark.read.parquet(PARQUET_PATH)

print("Total records:", df.count())


# --------------------------------------------------
# 4. Product × State
# --------------------------------------------------

print("\n" + "=" * 50)
print("PRODUCT × STATE ANALYSIS")
print("=" * 50)

product_state = (
    df.groupBy("Product", "State")
      .agg(count("*").alias("Complaint_Count"))
      .orderBy(col("Complaint_Count").desc())
)

product_state.show(30, truncate=False)

product_state.write.mode("overwrite").parquet(
    OUTPUT_PATH + r"\product_state"
)


# --------------------------------------------------
# 5. Product × Submission Method
# --------------------------------------------------

print("\n" + "=" * 50)
print("PRODUCT × SUBMISSION METHOD")
print("=" * 50)

product_submission = (
    df.groupBy("Product", "Submitted via")
      .agg(count("*").alias("Complaint_Count"))
      .orderBy(col("Complaint_Count").desc())
)

product_submission.show(30, truncate=False)

product_submission.write.mode("overwrite").parquet(
    OUTPUT_PATH + r"\product_submission"
)


# --------------------------------------------------
# 6. Product × Company Response
# --------------------------------------------------

print("\n" + "=" * 50)
print("PRODUCT × COMPANY RESPONSE")
print("=" * 50)

product_response = (
    df.groupBy("Product", "Company response to consumer")
      .agg(count("*").alias("Complaint_Count"))
      .orderBy(col("Complaint_Count").desc())
)

product_response.show(30, truncate=False)

product_response.write.mode("overwrite").parquet(
    OUTPUT_PATH + r"\product_response"
)


# --------------------------------------------------
# 7. State × Product
# --------------------------------------------------

print("\n" + "=" * 50)
print("STATE × PRODUCT ANALYSIS")
print("=" * 50)

state_product = (
    df.groupBy("State", "Product")
      .agg(count("*").alias("Complaint_Count"))
      .orderBy(col("Complaint_Count").desc())
)

state_product.show(30, truncate=False)

state_product.write.mode("overwrite").parquet(
    OUTPUT_PATH + r"\state_product"
)


# --------------------------------------------------
# 8. Year × Product
# --------------------------------------------------

print("\n" + "=" * 50)
print("YEAR × PRODUCT ANALYSIS")
print("=" * 50)

year_product = (
    df.groupBy("Year", "Product")
      .agg(count("*").alias("Complaint_Count"))
      .orderBy(
          col("Year").asc(),
          col("Complaint_Count").desc()
      )
)

year_product.show(50, truncate=False)

year_product.write.mode("overwrite").parquet(
    OUTPUT_PATH + r"\year_product"
)


# --------------------------------------------------
# 9. Year × State
# --------------------------------------------------

print("\n" + "=" * 50)
print("YEAR × STATE ANALYSIS")
print("=" * 50)

year_state = (
    df.groupBy("Year", "State")
      .agg(count("*").alias("Complaint_Count"))
      .orderBy(
          col("Year").asc(),
          col("Complaint_Count").desc()
      )
)

year_state.show(50, truncate=False)

year_state.write.mode("overwrite").parquet(
    OUTPUT_PATH + r"\year_state"
)


# --------------------------------------------------
# 10. Product × State × Submission Method
# --------------------------------------------------

print("\n" + "=" * 50)
print("THREE-DIMENSIONAL ANALYSIS")
print("=" * 50)

three_dimensional = (
    df.groupBy(
        "Product",
        "State",
        "Submitted via"
    )
    .agg(count("*").alias("Complaint_Count"))
    .orderBy(col("Complaint_Count").desc())
)

three_dimensional.show(50, truncate=False)

three_dimensional.write.mode("overwrite").parquet(
    OUTPUT_PATH + r"\product_state_submission"
)


# --------------------------------------------------
# 11. Completion
# --------------------------------------------------

print("\n" + "=" * 50)
print("MULTIDIMENSIONAL ANALYTICS COMPLETED")
print("=" * 50)

spark.stop()