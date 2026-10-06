from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    to_date,
    year,
    month,
    count
)
from pyspark.storagelevel import StorageLevel


# --------------------------------------------------
# 1. Create Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("ConsumerComplaintBDA_ETL")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "200")
.config(
    "spark.local.dir",
    r"C:\Users\Atharva\Desktop\Consumer Complaint Analytics\data\spark_temp"
)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# 2. File paths
# --------------------------------------------------

INPUT_FILE = r"C:\Users\Atharva\Desktop\Consumer Complaint Analytics\data\raw\complaints_1million.csv"
OUTPUT_PATH = r"C:\Users\Atharva\Desktop\Consumer Complaint Analytics\data\processed\complaints_parquet_1million"


# --------------------------------------------------
# 3. Read the 14M+ record CSV
# --------------------------------------------------

print("\n==============================================")
print("CONSUMER COMPLAINT BIG DATA ETL")
print("==============================================")

print("\nReading large CFPB dataset...")
print("Input:", INPUT_FILE)

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .option("multiLine", True)
    .option("escape", '"')
    .option("mode", "PERMISSIVE")
    .csv(INPUT_FILE)
)

raw_count = df.count()

print("\nRaw record count:", raw_count)


# --------------------------------------------------
# 4. Clean string columns
# --------------------------------------------------

string_columns = [
    "Product",
    "Sub-product",
    "Issue",
    "Sub-issue",
    "Company",
    "State",
    "Submitted via",
    "Company response to consumer",
    "Timely response?"
]

for column_name in string_columns:
    if column_name in df.columns:
        df = df.withColumn(
            column_name,
            trim(col(column_name))
        )


# --------------------------------------------------
# 5. Convert complaint date
# --------------------------------------------------

df = df.withColumn(
    "Date received",
    to_date(col("Date received"), "MM/dd/yyyy")
)


# --------------------------------------------------
# 6. Create time dimensions
# --------------------------------------------------

df = df.withColumn(
    "Year",
    year(col("Date received"))
)

df = df.withColumn(
    "Month",
    month(col("Date received"))
)


# --------------------------------------------------
# 7. Remove records with invalid/missing dates
# --------------------------------------------------

before_date_cleaning = df.count()

df = df.filter(
    col("Date received").isNotNull()
)

after_date_cleaning = df.count()

print(
    "\nRecords removed due to invalid/missing dates:",
    before_date_cleaning - after_date_cleaning
)


# --------------------------------------------------
# 8. Remove duplicate complaints
#    Complaint ID is the unique complaint identifier
# --------------------------------------------------

before_duplicates = df.count()

if "Complaint ID" in df.columns:

    df = df.dropDuplicates(["Complaint ID"])

else:

    print("\nWARNING: Complaint ID column not found.")
    print("Skipping complaint ID based deduplication.")


after_duplicates = df.count()

print("Records before duplicate removal:", before_duplicates)
print("Records after duplicate removal:", after_duplicates)
print("Duplicate complaints removed:", before_duplicates - after_duplicates)


# --------------------------------------------------
# 9. Cache cleaned dataset
# --------------------------------------------------

df = df.persist(StorageLevel.MEMORY_AND_DISK)

final_count = df.count()

print("\n==============================================")
print("DATA CLEANING SUMMARY")
print("==============================================")
print("Raw records:", raw_count)
print("Final records:", final_count)
print("Total records removed:", raw_count - final_count)


# --------------------------------------------------
# 10. Show schema
# --------------------------------------------------

print("\n==============================================")
print("FINAL DATA SCHEMA")
print("==============================================")

df.printSchema()


# --------------------------------------------------
# 11. Save as partitioned Parquet
# --------------------------------------------------

print("\n==============================================")
print("WRITING PARQUET")
print("==============================================")

(
    df.write
    .mode("overwrite")
    .partitionBy("Year")
    .parquet(OUTPUT_PATH)
)

print("\nParquet data written to:")
print(OUTPUT_PATH)


# --------------------------------------------------
# 12. Spark SQL-style aggregation
# --------------------------------------------------

print("\n==============================================")
print("TOP COMPLAINT PRODUCTS")
print("==============================================")

(
    df.groupBy("Product")
    .agg(
        count("*").alias("Complaint_Count")
    )
    .orderBy(
        col("Complaint_Count").desc()
    )
    .show(10, truncate=False)
)


# --------------------------------------------------
# 13. Year-wise complaint volume
# --------------------------------------------------

print("\n==============================================")
print("YEAR-WISE COMPLAINT VOLUME")
print("==============================================")

(
    df.groupBy("Year")
    .agg(
        count("*").alias("Complaint_Count")
    )
    .orderBy("Year")
    .show(30, truncate=False)
)


# --------------------------------------------------
# 14. Stop Spark
# --------------------------------------------------

df.unpersist()

spark.stop()

print("\n==============================================")
print("ETL PIPELINE COMPLETED SUCCESSFULLY")
print("==============================================")