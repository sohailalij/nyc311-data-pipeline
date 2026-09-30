from datetime import datetime

from pyspark.sql.types import StructType, StructField, TimestampType

from transform_functions import (
    deduplicate,
    standardize_categorical_fields,
    handle_missing_values,
    add_derived_features,
    join_reference_dimensions,
)


def test_deduplicate_removes_duplicate_unique_keys(spark):
    data = [(1, "A"), (1, "A"), (2, "B")]
    df = spark.createDataFrame(data, ["unique_key", "value"])
    result = deduplicate(df)
    assert result.count() == 2


def test_standardize_categorical_fields_uppercases_and_trims(spark):
    data = [("  nypd  ", "  brooklyn ", " open ", " Noise ", " loud music ")]
    df = spark.createDataFrame(
        data, ["agency", "borough", "status", "complaint_type", "descriptor"]
    )
    result = standardize_categorical_fields(df).collect()[0]
    assert result["agency"] == "NYPD"
    assert result["borough"] == "BROOKLYN"
    assert result["status"] == "OPEN"
    assert result["complaint_type"] == "Noise"
    assert result["descriptor"] == "loud music"


def test_handle_missing_values_fills_null_descriptor_and_city(spark):
    data = [(None, None), ("Loud Music", "Brooklyn")]
    df = spark.createDataFrame(data, "descriptor string, city string")
    result = handle_missing_values(df).collect()
    assert result[0]["descriptor"] == "UNKNOWN"
    assert result[0]["city"] == "UNKNOWN"
    assert result[1]["descriptor"] == "Loud Music"
    assert result[1]["city"] == "Brooklyn"


def test_add_derived_features_calculates_resolution_hours_correctly(spark):
    data = [(datetime(2023, 1, 1, 0, 0, 0), datetime(2023, 1, 1, 3, 0, 0))]
    df = spark.createDataFrame(data, ["created_date", "closed_date"])
    result = add_derived_features(df).collect()[0]
    assert result["resolution_hours"] == 3.0
    assert result["resolution_days"] == 0
    assert result["created_year"] == 2023
    assert result["created_month"] == 1


def test_add_derived_features_null_closed_date_gives_null_resolution(spark):
    schema = StructType([
        StructField("created_date", TimestampType(), True),
        StructField("closed_date", TimestampType(), True),
    ])
    data = [(datetime(2023, 1, 1, 0, 0, 0), None)]
    df = spark.createDataFrame(data, schema)
    result = add_derived_features(df).collect()[0]
    assert result["resolution_hours"] is None
    assert result["resolution_days"] is None


def test_join_reference_dimensions_attaches_correct_ids(spark):
    main_data = [("NYPD", "Noise - Residential", "BROOKLYN")]
    df = spark.createDataFrame(main_data, ["agency", "complaint_type", "borough"])

    dim_agency = spark.createDataFrame(
        [(12, "NYPD", "New York City Police Department")],
        ["agency_id", "agency_code", "agency_name"],
    )
    dim_complaint_type = spark.createDataFrame(
        [(127, "Noise - Residential", "Noise")],
        ["complaint_type_id", "complaint_type", "category"],
    )
    dim_borough = spark.createDataFrame(
        [(2, "BROOKLYN")],
        ["borough_id", "borough"],
    )

    result = join_reference_dimensions(
        df, dim_agency, dim_complaint_type, dim_borough
    ).collect()[0]
    assert result["agency_id"] == 12
    assert result["dim_agency_name"] == "New York City Police Department"
    assert result["complaint_type_id"] == 127
    assert result["complaint_category"] == "Noise"
    assert result["borough_id"] == 2