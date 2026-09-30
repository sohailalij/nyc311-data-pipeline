from pyspark.sql import SparkSession

from transform_functions import (
    deduplicate,
    standardize_categorical_fields,
    handle_missing_values,
    add_derived_features,
    join_reference_dimensions,
)

INPUT_PATH = "data/processed/valid_311_requests.csv"
OUTPUT_PATH = "data/processed/cleaned_311_requests"

AGENCY_DIM_PATH = "data/reference/dim_agency.csv"
COMPLAINT_DIM_PATH = "data/reference/dim_complaint_type.csv"
BOROUGH_DIM_PATH = "data/reference/dim_borough.csv"


spark = (
    SparkSession.builder
    .appName("NYC311Cleaning")
    .master("local[4]")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "16")
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    .getOrCreate()
)


print("\n=== Loading validated data ===")
df = spark.read.option("header", True).option("inferSchema", True).csv(INPUT_PATH)
initial_count = df.count()
print(f"Initial row count: {initial_count}")


print("\n=== Removing duplicate unique keys ===")
before_dedup = df.count()
df = deduplicate(df)
after_dedup = df.count()
print(f"Rows before deduplication: {before_dedup}")
print(f"Rows after deduplication:  {after_dedup}")
print(f"Duplicates removed:       {before_dedup - after_dedup}")


print("\n=== Standardizing categorical fields ===")
df = standardize_categorical_fields(df)


print("\n=== Handling missing values ===")
df = handle_missing_values(df)


print("\n=== Creating derived features ===")
df = add_derived_features(df)


print("\n=== Loading reference dimensions ===")
dim_agency = (
    spark.read.option("header", True).option("inferSchema", True)
    .csv(AGENCY_DIM_PATH)
    .select("agency_id", "agency_code", "agency_name")
)
dim_complaint_type = (
    spark.read.option("header", True).option("inferSchema", True)
    .csv(COMPLAINT_DIM_PATH)
    .select("complaint_type_id", "complaint_type", "category")
)
dim_borough = (
    spark.read.option("header", True).option("inferSchema", True)
    .csv(BOROUGH_DIM_PATH)
    .select("borough_id", "borough")
)


print("\n=== Joining reference dimensions ===")
df = join_reference_dimensions(df, dim_agency, dim_complaint_type, dim_borough)


print("\n=== Reference join validation ===")
df.select(
    "agency", "agency_id", "agency_name", "dim_agency_name",
    "complaint_type", "complaint_type_id", "complaint_category",
    "borough", "borough_id"
).show(10, truncate=False)

print("\n=== Derived columns sample ===")
df.select(
    "unique_key", "complaint_type", "created_date", "closed_date",
    "resolution_hours", "resolution_days", "created_year",
    "created_month", "created_hour", "created_day_of_week"
).show(10, truncate=False)


print("\n=== Writing cleaned dataset to Parquet ===")
(
    df.write.mode("overwrite")
    .partitionBy("created_year", "created_month")
    .parquet(OUTPUT_PATH)
)
print(f"Cleaned dataset written to: {OUTPUT_PATH}")

spark.stop()
print("\nCleaning and transformation completed successfully.")