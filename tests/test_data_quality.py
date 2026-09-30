import os

import pytest
from pyspark.sql import functions as F

PARQUET_PATH = os.path.join(
    os.path.dirname(__file__), "..", "data", "processed", "cleaned_311_requests"
)

EXPECTED_ROW_COUNT = 3220409  # validated at every stage of the pipeline

pytestmark = pytest.mark.skipif(
    not os.path.exists(PARQUET_PATH),
    reason="Cleaned Parquet output not found; run clean_311_data.py first",
)


@pytest.fixture(scope="module")
def stats(spark):
    """Compute every metric in a single pass over the 3.2M rows."""
    df = spark.read.parquet(PARQUET_PATH)
    row = df.agg(
        F.count("*").alias("total_rows"),
        F.countDistinct("unique_key").alias("distinct_keys"),
        F.sum(F.when(F.col("unique_key").isNull(), 1).otherwise(0)).alias("null_keys"),
        F.min("created_month").alias("min_month"),
        F.max("created_month").alias("max_month"),
        F.countDistinct("created_month").alias("distinct_months"),
        F.min("created_year").alias("min_year"),
        F.max("created_year").alias("max_year"),
        F.min("resolution_hours").alias("min_resolution_hours"),
        F.sum(F.when(F.col("complaint_category").isNull(), 1).otherwise(0)).alias("null_categories"),
        F.sum(F.when(F.col("borough").isNull(), 1).otherwise(0)).alias("null_boroughs"),
    ).first()
    return row.asDict()


def test_row_count_matches_validated_total(stats):
    assert stats["total_rows"] == EXPECTED_ROW_COUNT


def test_unique_key_has_no_nulls(stats):
    assert stats["null_keys"] == 0


def test_unique_key_has_no_duplicates(stats):
    assert stats["distinct_keys"] == stats["total_rows"]


def test_created_month_is_valid_and_complete(stats):
    assert stats["min_month"] == 1
    assert stats["max_month"] == 12
    assert stats["distinct_months"] == 12


def test_all_records_are_from_2023(stats):
    assert stats["min_year"] == 2023
    assert stats["max_year"] == 2023


def test_resolution_hours_is_never_negative(stats):
    assert stats["min_resolution_hours"] >= 0


def test_every_record_has_a_complaint_category(stats):
    assert stats["null_categories"] == 0


def test_every_record_has_a_borough(stats):
    assert stats["null_boroughs"] == 0