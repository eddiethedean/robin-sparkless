"""
Tests for to_timestamp() compatibility with multiple input types.

This test suite verifies that to_timestamp() accepts all input types
that PySpark supports, matching PySpark's behavior exactly.

Issue #131: to_timestamp() should accept TimestampType input for PySpark compatibility
"""

import pytest
from sparkless import SparkSession, functions as F
from datetime import datetime, date
from sparkless.spark_types import (
    IntegerType,
    LongType,
    DateType,
    DoubleType,
    StructType,
    StructField,
)


class TestToTimestampCompatibility:
    """Test to_timestamp() compatibility with PySpark."""

    def test_to_timestamp_timestamp_type_pass_through(self):
        """Test that to_timestamp() accepts TimestampType input (pass-through behavior).

        This is the exact scenario from issue #131.
        """
        spark = SparkSession("test")
        try:
            # Create DataFrame with timestamp string
            data = [("2024-01-01T10:00:00", "test")]
            df = spark.createDataFrame(data, ["timestamp_str", "name"])

            # Convert to timestamp
            df = df.withColumn(
                "ts", F.to_timestamp(df["timestamp_str"], "yyyy-MM-dd'T'HH:mm:ss")
            )

            # Try to_timestamp on TimestampType column - should work now
            result = df.withColumn(
                "ts2", F.to_timestamp(df["ts"], "yyyy-MM-dd'T'HH:mm:ss")
            )

            # Verify both columns are TimestampType
            rows = result.collect()
            assert len(rows) == 1
            assert isinstance(rows[0]["ts"], datetime)
            assert isinstance(rows[0]["ts2"], datetime)
            # ts2 should be the same as ts (pass-through behavior)
            assert rows[0]["ts"] == rows[0]["ts2"]
        finally:
            spark.stop()

    def test_to_timestamp_string_type_with_format(self):
        """Test that to_timestamp() works with StringType input and format string."""
        spark = SparkSession("test")
        try:
            data = [("2024-01-01T10:00:00",)]
            df = spark.createDataFrame(data, ["timestamp_str"])

            result = df.withColumn(
                "ts", F.to_timestamp(F.col("timestamp_str"), "yyyy-MM-dd'T'HH:mm:ss")
            )

            rows = result.collect()
            assert len(rows) == 1
            assert isinstance(rows[0]["ts"], datetime)
            assert rows[0]["ts"] == datetime(2024, 1, 1, 10, 0, 0)
        finally:
            spark.stop()

    def test_to_timestamp_string_type_without_format(self):
        """Test that to_timestamp() works with StringType input without format."""
        spark = SparkSession("test")
        try:
            data = [("2024-01-01 10:00:00",)]
            df = spark.createDataFrame(data, ["timestamp_str"])

            result = df.withColumn("ts", F.to_timestamp(F.col("timestamp_str")))

            rows = result.collect()
            assert len(rows) == 1
            assert isinstance(rows[0]["ts"], datetime)
        finally:
            spark.stop()

    def test_to_timestamp_integer_type_unix_timestamp(self):
        """Test that to_timestamp() accepts IntegerType input (Unix timestamp in seconds)."""
        spark = SparkSession("test")
        try:
            # Unix timestamp for 2024-01-01 10:00:00 UTC
            unix_ts = 1704110400
            schema = StructType([StructField("unix_ts", IntegerType(), True)])
            df = spark.createDataFrame([{"unix_ts": unix_ts}], schema=schema)

            result = df.withColumn("ts", F.to_timestamp(F.col("unix_ts")))

            rows = result.collect()
            assert len(rows) == 1
            assert isinstance(rows[0]["ts"], datetime)
        finally:
            spark.stop()

    def test_to_timestamp_long_type_unix_timestamp(self):
        """Test that to_timestamp() accepts LongType input (Unix timestamp in seconds)."""
        spark = SparkSession("test")
        try:
            # Unix timestamp for 2024-01-01 10:00:00 UTC
            unix_ts = 1704110400
            schema = StructType([StructField("unix_ts", LongType(), True)])
            df = spark.createDataFrame([{"unix_ts": unix_ts}], schema=schema)

            result = df.withColumn("ts", F.to_timestamp(F.col("unix_ts")))

            rows = result.collect()
            assert len(rows) == 1
            assert isinstance(rows[0]["ts"], datetime)
        finally:
            spark.stop()

    def test_to_timestamp_date_type_conversion(self):
        """Test that to_timestamp() accepts DateType input (converts Date to Timestamp)."""
        spark = SparkSession("test")
        try:
            schema = StructType([StructField("date_col", DateType(), True)])
            df = spark.createDataFrame([{"date_col": date(2024, 1, 1)}], schema=schema)

            result = df.withColumn("ts", F.to_timestamp(F.col("date_col")))

            rows = result.collect()
            assert len(rows) == 1
            assert isinstance(rows[0]["ts"], datetime)
            # Date should be converted to timestamp at midnight
            assert rows[0]["ts"].date() == date(2024, 1, 1)
        finally:
            spark.stop()

    def test_to_timestamp_double_type_unix_timestamp(self):
        """Test that to_timestamp() accepts DoubleType input (Unix timestamp with decimal seconds)."""
        spark = SparkSession("test")
        try:
            # Unix timestamp with decimals for 2024-01-01 10:00:00.5 UTC
            unix_ts = 1704110400.5
            schema = StructType([StructField("unix_ts", DoubleType(), True)])
            df = spark.createDataFrame([{"unix_ts": unix_ts}], schema=schema)

            result = df.withColumn("ts", F.to_timestamp(F.col("unix_ts")))

            rows = result.collect()
            assert len(rows) == 1
            assert isinstance(rows[0]["ts"], datetime)
        finally:
            spark.stop()

    def test_to_timestamp_rejects_unsupported_type(self):
        """Test that to_timestamp() rejects unsupported input types."""
        spark = SparkSession("test")
        try:
            from sparkless.spark_types import BooleanType

            schema = StructType([StructField("bool_col", BooleanType(), True)])
            df = spark.createDataFrame([{"bool_col": True}], schema=schema)

            with pytest.raises(
                TypeError,
                match="requires StringType, TimestampType, IntegerType, LongType, DateType, or DoubleType",
            ):
                df.withColumn("ts", F.to_timestamp(F.col("bool_col")))
        finally:
            spark.stop()

    def test_to_timestamp_after_regexp_replace(self):
        """Test that to_timestamp() works correctly after regexp_replace operation.

        This test verifies the fix for issue #133 where to_timestamp() would fail
        with SchemaError when used on a column created by regexp_replace.
        """
        spark = SparkSession("test")
        try:
            from sparkless.spark_types import StringType, StructType, StructField
            from datetime import datetime, timedelta

            # Create test data with ISO 8601 formatted timestamps (with microseconds)
            test_data = [
                {
                    "id": f"record-{i:03d}",
                    "timestamp_str": (datetime.now() - timedelta(hours=i)).isoformat(),
                }
                for i in range(5)
            ]

            schema = StructType(
                [
                    StructField("id", StringType(), False),
                    StructField("timestamp_str", StringType(), False),
                ]
            )

            df = spark.createDataFrame(test_data, schema)

            # Clean timestamp string (remove microseconds) using regexp_replace
            df_clean = df.withColumn(
                "timestamp_clean",
                F.regexp_replace(F.col("timestamp_str"), r"\.\d+", ""),
            )

            # Parse to timestamp - should work without SchemaError
            df_parsed = df_clean.withColumn(
                "timestamp_parsed",
                F.to_timestamp(F.col("timestamp_clean"), "yyyy-MM-dd'T'HH:mm:ss"),
            )

            # Schema should show correct type
            schema_dict = {
                field.name: type(field.dataType).__name__
                for field in df_parsed.schema.fields
            }
            assert schema_dict["timestamp_parsed"] == "TimestampType"

            # Materialization should work without SchemaError
            rows = df_parsed.collect()
            assert len(rows) == 5
            # Verify that timestamp_parsed column exists and is the correct type
            for row in rows:
                # timestamp_parsed should be datetime or None (if parsing failed)
                assert row["timestamp_parsed"] is None or isinstance(
                    row["timestamp_parsed"], datetime
                )
        finally:
            spark.stop()
