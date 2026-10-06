import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    when,
    isnan,
    trim
)

# --------------------------------------------------
# 1. Create Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("ConsumerComplaintBDA_DataQuality")
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
    r"\results\data_quality"
)


# --------------------------------------------------
# 3. Read processed Parquet
# --------------------------------------------------

print("\n==============================================")
print("DATA QUALITY ANALYSIS")
print("==============================================")

print("\nReading processed Parquet...")

df = spark.read.parquet(PARQUET_PATH)

total_records = df.count()
total_columns = len(df.columns)

print("\nTotal records:", total_records)
print("Total columns:", total_columns)


# --------------------------------------------------
# 4. Display schema
# --------------------------------------------------

print("\n==============================================")
print("SCHEMA")
print("==============================================")

df.printSchema()


# --------------------------------------------------
# 5. Missing-value analysis
# --------------------------------------------------

print("\n==============================================")
print("MISSING VALUE ANALYSIS")
print("==============================================")

missing_expressions = []

for column_name in df.columns:

    missing_expressions.append(
        count(
            when(
                col(column_name).isNull()
                | (
                    col(column_name).cast("string").isNotNull()
                    & (trim(col(column_name).cast("string")) == "")
                ),
                True
            )
        ).alias(column_name)
    )

missing_row = df.select(missing_expressions).collect()[0]

missing_data = []

for column_name in df.columns:

    missing_count = missing_row[column_name]

    missing_percentage = (
        (missing_count / total_records) * 100
        if total_records > 0
        else 0
    )

    missing_data.append(
        (
            column_name,
            int(missing_count),
            round(missing_percentage, 2)
        )
    )


missing_df = spark.createDataFrame(
    missing_data,
    [
        "Column",
        "Missing_Count",
        "Missing_Percentage"
    ]
)

missing_df.orderBy(
    col("Missing_Count").desc()
).show(
    len(df.columns),
    truncate=False
)


# --------------------------------------------------
# 6. Completeness analysis
# --------------------------------------------------

print("\n==============================================")
print("DATA COMPLETENESS")
print("==============================================")

completeness_df = (
    missing_df
    .withColumn(
        "Completeness_Percentage",
        100 - col("Missing_Percentage")
    )
    .orderBy(
        col("Completeness_Percentage").asc()
    )
)

completeness_df.show(
    len(df.columns),
    truncate=False
)


# --------------------------------------------------
# 7. Unique-value analysis
# --------------------------------------------------

print("\n==============================================")
print("UNIQUE VALUE ANALYSIS")
print("==============================================")

important_columns = [
    "Product",
    "Sub-product",
    "Issue",
    "Company",
    "State",
    "Submitted via",
    "Company response to consumer",
    "Timely response?"
]

unique_data = []

for column_name in important_columns:

    if column_name in df.columns:

        unique_count = df.select(
            col(column_name)
        ).distinct().count()

        unique_data.append(
            (
                column_name,
                int(unique_count)
            )
        )

unique_df = spark.createDataFrame(
    unique_data,
    [
        "Column",
        "Unique_Values"
    ]
)

unique_df.show(
    truncate=False
)


# --------------------------------------------------
# 8. Duplicate Complaint ID check
# --------------------------------------------------

print("\n==============================================")
print("DUPLICATE COMPLAINT ID CHECK")
print("==============================================")

if "Complaint ID" in df.columns:

    total_ids = df.select("Complaint ID").count()

    unique_ids = df.select(
        "Complaint ID"
    ).distinct().count()

    duplicate_ids = total_ids - unique_ids

    print("Total Complaint IDs:", total_ids)
    print("Unique Complaint IDs:", unique_ids)
    print("Duplicate Complaint IDs:", duplicate_ids)


# --------------------------------------------------
# 9. Date quality
# --------------------------------------------------

print("\n==============================================")
print("DATE QUALITY")
print("==============================================")

if "Date received" in df.columns:

    invalid_dates = df.filter(
        col("Date received").isNull()
    ).count()

    print("Invalid / missing dates:", invalid_dates)


# --------------------------------------------------
# 10. Save data-quality results
# --------------------------------------------------

print("\n==============================================")
print("SAVING RESULTS")
print("==============================================")

import os

os.makedirs(OUTPUT_PATH, exist_ok=True)

missing_df.coalesce(1).write.mode("overwrite").option(
    "header", True
).csv(
    OUTPUT_PATH + r"\missing_values"
)

completeness_df.coalesce(1).write.mode("overwrite").option(
    "header", True
).csv(
    OUTPUT_PATH + r"\completeness"
)

unique_df.coalesce(1).write.mode("overwrite").option(
    "header", True
).csv(
    OUTPUT_PATH + r"\unique_values"
)


# --------------------------------------------------
# 11. Finish
# --------------------------------------------------

spark.stop()

print("\n==============================================")
print("DATA QUALITY ANALYSIS COMPLETED")
print("==============================================")