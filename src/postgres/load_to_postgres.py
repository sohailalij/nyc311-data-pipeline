import os
from pyspark.sql import SparkSession
import psycopg2
from psycopg2.extras import execute_values

from postgres_helpers import clean_row

# --- Config: read from environment variables, never hard-code credentials ---
PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = os.environ.get("PG_PORT", "5432")
PG_DB = os.environ.get("PG_DB", "nyc311_analytics")
PG_USER = os.environ.get("PG_USER", "postgres")
PG_PASSWORD = os.environ.get("PG_PASSWORD")

if not PG_PASSWORD:
    raise ValueError("Set the PG_PASSWORD environment variable before running this script.")

spark = (
    SparkSession.builder
    .appName("NYC311_LoadToPostgres")
    .enableHiveSupport()
    .getOrCreate()
)
spark.sql("USE nyc311_analytics_db")


def load_table(spark_table, pg_table, columns):
    print(f"=== Loading {spark_table} -> {pg_table} ===")
    df = spark.sql(f"SELECT {', '.join(columns)} FROM {spark_table}")
    pdf = df.toPandas()
    print(f"Rows to load: {len(pdf)}")

    conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD)
    cur = conn.cursor()

    cur.execute(f"TRUNCATE TABLE {pg_table} RESTART IDENTITY")

    col_list = ", ".join(columns)
    values = [clean_row(row) for row in pdf.itertuples(index=False, name=None)]
    insert_sql = f"INSERT INTO {pg_table} ({col_list}) VALUES %s"

    execute_values(cur, insert_sql, values, page_size=5000)
    conn.commit()

    cur.execute(f"SELECT COUNT(*) FROM {pg_table}")
    pg_count = cur.fetchone()[0]
    print(f"Rows now in Postgres: {pg_count}")

    cur.close()
    conn.close()
    return len(pdf), pg_count


monthly_cols = [
    "created_year", "created_month", "borough", "complaint_category",
    "total_requests", "avg_resolution_hours", "closed_requests", "unresolved_requests"
]
daily_cols = [
    "request_date", "borough", "complaint_category",
    "total_requests", "avg_resolution_hours", "closed_requests", "unresolved_requests"
]

monthly_source, monthly_loaded = load_table(
    "borough_category_monthly_summary", "borough_category_monthly_summary", monthly_cols
)
daily_source, daily_loaded = load_table(
    "borough_category_daily_summary", "borough_category_daily_summary", daily_cols
)

print("=== LOAD VALIDATION ===")
print(f"Monthly: Hive={monthly_source}, Postgres={monthly_loaded}, match={monthly_source == monthly_loaded}")
print(f"Daily:   Hive={daily_source}, Postgres={daily_loaded}, match={daily_source == daily_loaded}")

spark.stop()