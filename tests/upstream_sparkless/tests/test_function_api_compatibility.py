"""
Tests for function API compatibility with PySpark.

This test suite verifies that function signatures and calling patterns
match PySpark exactly, ensuring no false positives in tests.
"""

import pytest
from sparkless import SparkSession, functions as F


class TestFunctionAPIs:
    """Test that function APIs match PySpark exactly."""

    def test_current_date_is_function_not_method(self):
        """Test that current_date is a function, not DataFrame method."""
        spark = SparkSession("test")
        try:
            df = spark.createDataFrame([{"id": 1}], ["id"])

            # Should work as function
            result = df.withColumn("today", F.current_date())
            assert result is not None

            # Should NOT work as DataFrame method
            with pytest.raises(AttributeError):
                df.current_date()
        finally:
            spark.stop()

    def test_current_timestamp_is_function_not_method(self):
        """Test that current_timestamp is a function, not DataFrame method."""
        spark = SparkSession("test")
        try:
            df = spark.createDataFrame([{"id": 1}], ["id"])

            # Should work as function
            result = df.withColumn("now", F.current_timestamp())
            assert result is not None

            # Should NOT work as DataFrame method
            with pytest.raises(AttributeError):
                df.current_timestamp()
        finally:
            spark.stop()

    def test_functions_are_static_methods(self):
        """Test that functions are accessible as static methods."""
        spark = SparkSession("test")
        try:
            # All these should work as static method calls
            col_expr = F.col("id")
            assert col_expr is not None

            lit_expr = F.lit(42)
            assert lit_expr is not None

            count_func = F.count("id")
            assert count_func is not None

            row_num = F.row_number()
            assert row_num is not None
        finally:
            spark.stop()

    def test_function_signatures_match_pyspark(self):
        """Test that function signatures match PySpark patterns."""
        spark = SparkSession("test")
        try:
            df = spark.createDataFrame([{"id": 1, "value": 10}], ["id", "value"])

            # Test various function calling patterns that should match PySpark
            result = df.select(
                F.col("id"),
                F.lit(42).alias("constant"),
                F.count("id").alias("count"),
                F.sum("value").alias("sum"),
                F.current_date().alias("today"),
                F.current_timestamp().alias("now"),
            )
            assert result is not None
        finally:
            spark.stop()
