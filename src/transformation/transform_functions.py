from pyspark.sql.functions import (
    col, trim, upper, when, datediff, hour, month, year,
    dayofweek, lit, round
)


def deduplicate(df):
    """Remove duplicate unique_key rows, keeping one copy of each."""
    return df.dropDuplicates(["unique_key"])


def standardize_categorical_fields(df):
    """Uppercase/trim categorical fields for consistent grouping downstream."""
    return (
        df
        .withColumn("agency", upper(trim(col("agency"))))
        .withColumn("borough", upper(trim(col("borough"))))
        .withColumn("status", upper(trim(col("status"))))
        .withColumn("complaint_type", trim(col("complaint_type")))
        .withColumn("descriptor", trim(col("descriptor")))
    )


def handle_missing_values(df):
    """Replace nulls in descriptor/city with an explicit UNKNOWN marker."""
    return (
        df
        .withColumn(
            "descriptor",
            when(col("descriptor").isNull(), lit("UNKNOWN")).otherwise(col("descriptor"))
        )
        .withColumn(
            "city",
            when(col("city").isNull(), lit("UNKNOWN")).otherwise(col("city"))
        )
    )


def add_derived_features(df):
    """Add resolution time metrics and calendar breakdown columns."""
    return (
        df
        .withColumn(
            "resolution_hours",
            when(
                col("closed_date").isNotNull(),
                round((col("closed_date").cast("long") - col("created_date").cast("long")) / 3600, 2)
            )
        )
        .withColumn(
            "resolution_days",
            when(col("closed_date").isNotNull(), datediff(col("closed_date"), col("created_date")))
        )
        .withColumn("created_year", year(col("created_date")))
        .withColumn("created_month", month(col("created_date")))
        .withColumn("created_hour", hour(col("created_date")))
        .withColumn("created_day_of_week", dayofweek(col("created_date")))
    )


def join_reference_dimensions(df, dim_agency, dim_complaint_type, dim_borough):
    """Join agency, complaint type, and borough dimension tables onto the main dataframe."""
    dim_agency = dim_agency.withColumnRenamed("agency_name", "dim_agency_name")
    dim_complaint_type = dim_complaint_type.withColumnRenamed("category", "complaint_category")

    return (
        df
        .join(dim_agency, df["agency"] == dim_agency["agency_code"], "left")
        .drop(dim_agency["agency_code"])
        .join(dim_complaint_type, df["complaint_type"] == dim_complaint_type["complaint_type"], "left")
        .drop(dim_complaint_type["complaint_type"])
        .join(dim_borough, df["borough"] == dim_borough["borough"], "left")
        .drop(dim_borough["borough"])
    )