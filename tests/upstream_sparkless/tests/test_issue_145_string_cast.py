"""Test for issue #145: Explicit string cast() still creates datetime[μs] type."""

from sparkless.sql import SparkSession
from sparkless.functions import col, to_timestamp, to_date
from sparkless.spark_types import StringType


def test_string_cast_schema_is_string_type():
    """Test that explicit string cast creates StringType in schema."""
    spark = SparkSession.builder.appName("BugRepro").getOrCreate()
    try:
        # Create data with timestamp string
        data = [("user1", "2024-01-01 10:30:00", 100.0)]
        df = spark.createDataFrame(data, ["user_id", "timestamp", "value"])

        # Explicitly cast to string
        transformed = df.withColumn(
            "timestamp_str",
            col("timestamp").cast("string"),  # Explicit string cast
        )

        # Check schema - timestamp_str should be StringType
        schema = transformed.schema
        field_dict = {f.name: f.dataType for f in schema.fields}

        assert isinstance(field_dict["timestamp_str"], StringType), (
            f"timestamp_str should be StringType in schema, got {type(field_dict['timestamp_str']).__name__}"
        )

    finally:
        spark.stop()


def test_string_cast_works_with_to_timestamp():
    """Test that string cast works when used with to_timestamp."""
    spark = SparkSession.builder.appName("BugRepro").getOrCreate()
    try:
        # Create data with timestamp string
        data = [("user1", "2024-01-01 10:30:00", 100.0)]
        df = spark.createDataFrame(data, ["user_id", "timestamp", "value"])

        # Explicitly cast to string
        transformed = df.withColumn(
            "timestamp_str",
            col("timestamp").cast("string"),  # Explicit string cast
        )

        # Extract date from timestamp string - this should work
        result = transformed.withColumn(
            "event_date",
            to_date(to_timestamp(col("timestamp_str"), "yyyy-MM-dd HH:mm:ss")),
        )

        # This should not fail - sparkless should see timestamp_str as string
        count = result.count()
        assert count == 1, "Should have 1 row"

    finally:
        spark.stop()


def test_string_cast_from_datetime_column():
    """Test casting a datetime column to string."""
    spark = SparkSession.builder.appName("BugRepro").getOrCreate()
    try:
        from datetime import datetime

        # Create data with actual datetime object
        data = [("user1", datetime(2024, 1, 1, 10, 30, 0), 100.0)]
        df = spark.createDataFrame(data, ["user_id", "timestamp", "value"])

        # Explicitly cast datetime to string
        transformed = df.withColumn(
            "timestamp_str",
            col("timestamp").cast("string"),  # Explicit string cast
        )

        # Check schema
        schema = transformed.schema
        field_dict = {f.name: f.dataType for f in schema.fields}

        assert isinstance(field_dict["timestamp_str"], StringType), (
            f"timestamp_str should be StringType, got {type(field_dict['timestamp_str']).__name__}"
        )

        # Should be able to use it as string
        count = transformed.count()
        assert count == 1, "Should have 1 row"

    finally:
        spark.stop()
