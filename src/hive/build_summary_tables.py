from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("NYC311_HiveSummaryTables")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sql("USE nyc311_analytics_db")

# Drop and recreate on each run so this script stays idempotent/rerunnable
spark.sql("DROP TABLE IF EXISTS borough_category_monthly_summary")

spark.sql("""
    CREATE TABLE borough_category_monthly_summary AS
    SELECT
        created_year,
        created_month,
        borough,
        complaint_category,
        COUNT(*) AS total_requests,
        ROUND(AVG(resolution_hours), 2) AS avg_resolution_hours,
        SUM(CASE WHEN status = 'CLOSED' THEN 1 ELSE 0 END) AS closed_requests,
        SUM(CASE WHEN resolution_hours IS NULL THEN 1 ELSE 0 END) AS unresolved_requests
    FROM service_requests
    GROUP BY created_year, created_month, borough, complaint_category
""")

print("=== SUMMARY TABLE ROW COUNT ===")
spark.sql("SELECT COUNT(*) AS summary_rows FROM borough_category_monthly_summary").show()

print("=== SAMPLE ROWS ===")
spark.sql("""
    SELECT * FROM borough_category_monthly_summary
    ORDER BY created_month, total_requests DESC
    LIMIT 10
""").show(truncate=False)

print("=== VALIDATION: does summary total match raw table total? ===")
spark.sql("""
    SELECT
        (SELECT COUNT(*) FROM service_requests) AS raw_total,
        (SELECT SUM(total_requests) FROM borough_category_monthly_summary) AS summary_total
""").show()

print("=== WINDOW FUNCTION: TOP CATEGORY PER BOROUGH PER MONTH ===")
spark.sql("""
    SELECT created_month, borough, complaint_category, total_requests, category_rank
    FROM (
        SELECT
            created_month,
            borough,
            complaint_category,
            total_requests,
            RANK() OVER (
                PARTITION BY borough, created_month
                ORDER BY total_requests DESC
            ) AS category_rank
        FROM borough_category_monthly_summary
    ) ranked
    WHERE category_rank = 1
    ORDER BY created_month, borough
""").show(80, truncate=False)

print("=== BUILDING DAILY SUMMARY TABLE ===")
spark.sql("DROP TABLE IF EXISTS borough_category_daily_summary")

spark.sql("""
    CREATE TABLE borough_category_daily_summary AS
    SELECT
        TO_DATE(created_date) AS request_date,
        borough,
        complaint_category,
        COUNT(*) AS total_requests,
        ROUND(AVG(resolution_hours), 2) AS avg_resolution_hours,
        SUM(CASE WHEN status = 'CLOSED' THEN 1 ELSE 0 END) AS closed_requests,
        SUM(CASE WHEN resolution_hours IS NULL THEN 1 ELSE 0 END) AS unresolved_requests
    FROM service_requests
    GROUP BY TO_DATE(created_date), borough, complaint_category
""")

print("=== DAILY SUMMARY ROW COUNT ===")
spark.sql("SELECT COUNT(*) AS daily_summary_rows FROM borough_category_daily_summary").show()

print("=== DAILY VALIDATION: does daily total match raw total? ===")
spark.sql("""
    SELECT
        (SELECT COUNT(*) FROM service_requests) AS raw_total,
        (SELECT SUM(total_requests) FROM borough_category_daily_summary) AS daily_total
""").show()

spark.stop()