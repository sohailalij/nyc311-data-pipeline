import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

BUCKET = "nyc311-pipeline-sohail-2026"

spark = (
    SparkSession.builder
    .appName("S3ConnectivityTest")
    .config("spark.jars.packages", "")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .getOrCreate()
)

print("=== SPARK + S3 CONNECTIVITY TEST ===")
print("Spark version:", spark.version)

test_df = spark.createDataFrame([(1, "test"), (2, "connection")], ["id", "value"])
test_path = f"s3a://{BUCKET}/connectivity-test/"

print(f"\nWriting test data to {test_path}")
test_df.write.mode("overwrite").parquet(test_path)
print("Write succeeded.")

print(f"\nReading it back from {test_path}")
read_back = spark.read.parquet(test_path)
read_back.show()

print("\nS3 connectivity confirmed.")
spark.stop()