import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

BUCKET = "nyc311-pipeline-sohail-2026"

spark = (
    SparkSession.builder
    .appName("NYC311_UploadToS3")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "16")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .getOrCreate()
)

# Reference dimension tables
print("=== Uploading reference dimension tables ===")
for name in ["dim_agency", "dim_complaint_type", "dim_borough"]:
    local_path = f"data/reference/{name}.csv"
    s3_path = f"s3a://{BUCKET}/reference/{name}.csv"
    df = spark.read.option("header", True).option("inferSchema", True).csv(local_path)
    df.write.mode("overwrite").option("header", True).csv(s3_path)
    print(f"  {name}: {df.count()} rows uploaded to {s3_path}")

# Curated Parquet output (the module 5 result)
print("\n=== Uploading curated Parquet dataset ===")
curated_local = "data/processed/cleaned_311_requests"
curated_s3 = f"s3a://{BUCKET}/curated/cleaned_311_requests"
df_curated = spark.read.parquet(curated_local)
row_count = df_curated.count()
print(f"Local row count: {row_count}")

df_curated.write.mode("overwrite").partitionBy("created_year", "created_month").parquet(curated_s3)
print(f"Uploaded to {curated_s3}")

# Validate the upload matches
print("\n=== Validating S3 copy matches local ===")
df_s3_check = spark.read.parquet(curated_s3)
s3_count = df_s3_check.count()
print(f"S3 row count: {s3_count}")
print(f"Match: {row_count == s3_count}")

spark.stop()