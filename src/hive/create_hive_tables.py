import os
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType, IntegerType, LongType, DoubleType, FloatType,
    BooleanType, DateType, TimestampType
)

spark = (
    SparkSession.builder
    .appName("NYC311_HiveWarehouseSetup")
    .enableHiveSupport()
    .getOrCreate()
)

# 1. Read the module 5 output directly to get its real, authoritative schema
parquet_path = "data/processed/cleaned_311_requests"
df = spark.read.parquet(parquet_path)

partition_cols = ["created_year", "created_month"]
data_cols = [f for f in df.schema.fields if f.name not in partition_cols]

# 2. Map Spark types to Hive DDL types
def to_hive_type(spark_type):
    mapping = {
        StringType: "STRING",
        IntegerType: "INT",
        LongType: "BIGINT",
        DoubleType: "DOUBLE",
        FloatType: "FLOAT",
        BooleanType: "BOOLEAN",
        DateType: "DATE",
        TimestampType: "TIMESTAMP",
    }
    return mapping.get(type(spark_type), "STRING")

column_defs = ",\n    ".join(
    f"`{f.name}` {to_hive_type(f.dataType)}" for f in data_cols
)

# 3. Build an unambiguous absolute file:/// URI instead of a relative path
abs_path = os.path.abspath(parquet_path).replace("\\", "/")
location_uri = f"file:///{abs_path}"

ddl = f"""
CREATE EXTERNAL TABLE service_requests (
    {column_defs}
)
PARTITIONED BY (created_year INT, created_month INT)
STORED AS PARQUET
LOCATION '{location_uri}'
"""

print("=== LOCATION URI ===")
print(location_uri)
print("=== GENERATED DDL ===")
print(ddl)

# 4. Create database, drop any stale table from earlier attempts, recreate with correct location
spark.sql("CREATE DATABASE IF NOT EXISTS nyc311_analytics_db")
spark.sql("USE nyc311_analytics_db")
spark.sql("DROP TABLE IF EXISTS service_requests")
spark.sql(ddl)
spark.sql("ALTER TABLE service_requests RECOVER PARTITIONS")

print("=== TABLES ===")
spark.sql("SHOW TABLES").show()

print("=== PARTITIONS ===")
spark.sql("SHOW PARTITIONS service_requests").show(20, truncate=False)

print("=== ROW COUNT CHECK ===")
spark.sql("SELECT COUNT(*) AS total_rows FROM service_requests").show()

spark.stop()