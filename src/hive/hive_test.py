from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("HiveConnectivityTest")
    .enableHiveSupport()
    .getOrCreate()
)

print("=== SPARK SESSION WITH HIVE SUPPORT ===")
print("Spark version:", spark.version)

spark.sql("SHOW DATABASES").show()

spark.stop()