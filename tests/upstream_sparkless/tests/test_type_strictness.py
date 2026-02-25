"""
Tests for strict type checking in functions.

This test suite verifies that functions enforce strict type requirements,
matching PySpark's behavior exactly.
"""

import pytest
from sparkless import SparkSession, functions as F


class TestTypeStrictness:
    """Test strict type checking in functions."""

    def test_to_timestamp_accepts_multiple_types(self):
        """Test that to_timestamp accepts StringType, TimestampType, IntegerType, LongType, DateType, and DoubleType."""
        spark = SparkSession("test")
        try:
            from sparkless.spark_types import (
                IntegerType,
                LongType,
                DateType,
                DoubleType,
                StructType,
                StructField,
            )
            from datetime import datetime, date

            # Test StringType input
            df_str = spark.createDataFrame(
                [{"date_str": "2023-01-01 12:00:00"}], schema=["date_str"]
            )
            result_str = df_str.withColumn("parsed", F.to_timestamp(F.col("date_str")))
            assert result_str is not None
            rows = result_str.collect()
            assert len(rows) == 1
            assert isinstance(rows[0]["parsed"], datetime)

            # Test TimestampType input (pass-through)
            df_ts = spark.createDataFrame(
                [{"ts": datetime(2023, 1, 1, 12, 0, 0)}], schema=["ts"]
            )
            result_ts = df_ts.withColumn("ts2", F.to_timestamp(F.col("ts")))
            assert result_ts is not None
            rows_ts = result_ts.collect()
            assert len(rows_ts) == 1
            assert isinstance(rows_ts[0]["ts2"], datetime)

            # Test IntegerType input (Unix timestamp)
            schema_int = StructType([StructField("unix_ts", IntegerType(), True)])
            df_int = spark.createDataFrame(
                [{"unix_ts": 1672574400}], schema=schema_int
            )  # 2023-01-01 12:00:00 UTC
            result_int = df_int.withColumn("parsed", F.to_timestamp(F.col("unix_ts")))
            assert result_int is not None
            rows_int = result_int.collect()
            assert len(rows_int) == 1
            assert isinstance(rows_int[0]["parsed"], datetime)

            # Test LongType input (Unix timestamp)
            schema_long = StructType([StructField("unix_ts", LongType(), True)])
            df_long = spark.createDataFrame(
                [{"unix_ts": 1672574400}], schema=schema_long
            )
            result_long = df_long.withColumn("parsed", F.to_timestamp(F.col("unix_ts")))
            assert result_long is not None
            rows_long = result_long.collect()
            assert len(rows_long) == 1
            assert isinstance(rows_long[0]["parsed"], datetime)

            # Test DateType input
            schema_date = StructType([StructField("date_col", DateType(), True)])
            df_date = spark.createDataFrame(
                [{"date_col": date(2023, 1, 1)}], schema=schema_date
            )
            result_date = df_date.withColumn(
                "parsed", F.to_timestamp(F.col("date_col"))
            )
            assert result_date is not None
            rows_date = result_date.collect()
            assert len(rows_date) == 1
            assert isinstance(rows_date[0]["parsed"], datetime)

            # Test DoubleType input (Unix timestamp with decimals)
            schema_double = StructType([StructField("unix_ts", DoubleType(), True)])
            df_double = spark.createDataFrame(
                [{"unix_ts": 1672574400.5}], schema=schema_double
            )
            result_double = df_double.withColumn(
                "parsed", F.to_timestamp(F.col("unix_ts"))
            )
            assert result_double is not None
            rows_double = result_double.collect()
            assert len(rows_double) == 1
            assert isinstance(rows_double[0]["parsed"], datetime)

        finally:
            spark.stop()

    def test_to_timestamp_rejects_unsupported_types(self):
        """Test that to_timestamp rejects unsupported input types."""
        spark = SparkSession("test")
        try:
            from sparkless.spark_types import BooleanType, StructType, StructField

            # Create DataFrame with BooleanType (not supported)
            schema = StructType([StructField("bool_col", BooleanType(), True)])
            df = spark.createDataFrame([{"bool_col": True}], schema=schema)

            # This should fail - BooleanType is not supported
            with pytest.raises(
                TypeError,
                match="requires StringType, TimestampType, IntegerType, LongType, DateType, or DoubleType",
            ):
                df.withColumn("parsed", F.to_timestamp(F.col("bool_col")))
        finally:
            spark.stop()

    def test_to_timestamp_works_with_string(self):
        """Test that to_timestamp works with string input."""
        spark = SparkSession("test")
        try:
            # Create DataFrame with string column
            df = spark.createDataFrame(
                [{"date_str": "2023-01-01 12:00:00"}], schema=["date_str"]
            )

            # This should work
            result = df.withColumn("parsed", F.to_timestamp(F.col("date_str")))
            assert result is not None
        finally:
            spark.stop()

    def test_to_date_requires_string(self):
        """Test that to_date requires string or date input."""
        spark = SparkSession("test")
        try:
            from sparkless.spark_types import IntegerType, StructType, StructField

            # Create DataFrame with explicit IntegerType schema (not string or date)
            schema = StructType([StructField("date", IntegerType(), True)])
            df = spark.createDataFrame([{"date": 12345}], schema=schema)

            # This should fail - to_date requires StringType, TimestampType, or DateType
            with pytest.raises(
                TypeError, match="requires StringType, TimestampType, or DateType input"
            ):
                df.withColumn("parsed", F.to_date(F.col("date")))
        finally:
            spark.stop()

    def test_to_date_works_with_string(self):
        """Test that to_date works with string input."""
        spark = SparkSession("test")
        try:
            # Create DataFrame with string column
            df = spark.createDataFrame(
                [{"date_str": "2023-01-01"}], schema=["date_str"]
            )

            # This should work
            result = df.withColumn("parsed", F.to_date(F.col("date_str")))
            assert result is not None
        finally:
            spark.stop()
