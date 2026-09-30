from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("NYC311_HiveSampleQueries")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sql("USE nyc311_analytics_db")

print("=== MONTHLY COMPLAINT VOLUME (partition-pruned) ===")
spark.sql("""
    SELECT created_year, created_month, COUNT(*) AS total_requests
    FROM service_requests
    WHERE created_year = 2023
    GROUP BY created_year, created_month
    ORDER BY created_month
""").show(12)

print("=== TOP 10 COMPLAINT TYPES BY VOLUME ===")
spark.sql("""
    SELECT complaint_type, complaint_category, COUNT(*) AS total
    FROM service_requests
    GROUP BY complaint_type, complaint_category
    ORDER BY total DESC
    LIMIT 10
""").show(truncate=False)

spark.stop()