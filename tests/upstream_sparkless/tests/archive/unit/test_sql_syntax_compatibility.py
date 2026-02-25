"""
Tests for SQL syntax compatibility fixes.

This module tests that SQL generation uses correct DuckDB syntax:
- Type casting uses unquoted DuckDB types (INTEGER, DATE, VARCHAR)
- STRPTIME format strings don't have broken quotes
"""

import pytest
from sparkless import SparkSession, F


@pytest.mark.unit
class TestSQLSyntaxCompatibility:
    """Test SQL syntax compatibility fixes."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        session = SparkSession("sql_syntax_test")
        yield session
        session.stop()

    def test_type_casting_with_unquoted_types(self, spark):
        """Test that type casting generates unquoted DuckDB types."""
        test_data = [{"value": 100, "value_str": "100", "value_date": "2025-01-01"}]

        df = spark.createDataFrame(test_data)

        # Test various type casts
        result = (
            df.withColumn("as_int", F.col("value_str").cast("int"))
            .withColumn("as_string", F.col("value").cast("string"))
            .withColumn("as_date", F.col("value_date").cast("date"))
            .select("as_int", "as_string", "as_date")
        )

        rows = result.collect()
        assert len(rows) == 1
        assert rows[0].as_int == 100
        assert rows[0].as_string == "100"
        assert rows[0].as_date is not None

    def test_strptime_format_with_literal_t(self, spark):
        """Test STRPTIME format strings with literal 'T' character."""
        from sparkless.spark_types import StringType, StructType, StructField

        test_data = [
            {"timestamp_str": "2025-10-29T10:30:45"},
            {"timestamp_str": "2025-11-01T14:20:30"},
        ]

        # Explicitly set schema to StringType to avoid type inference issues
        schema = StructType([StructField("timestamp_str", StringType(), True)])
        df = spark.createDataFrame(test_data, schema=schema)

        # Format with 'T' literal should work without SQL syntax errors
        result = df.withColumn(
            "parsed_timestamp",
            F.to_timestamp(F.col("timestamp_str"), "yyyy-MM-dd'T'HH:mm:ss"),
        ).select("timestamp_str", "parsed_timestamp")

        rows = result.collect()
        assert len(rows) == 2
        assert all(row.parsed_timestamp is not None for row in rows)

    def test_strptime_format_with_optional_fractional_seconds(self, spark):
        """Test STRPTIME format strings with optional fractional seconds."""
        from sparkless.spark_types import StringType, StructType, StructField

        test_data = [
            {"timestamp_str": "2025-10-29T10:30:45.123456"},
            {"timestamp_str": "2025-10-29T14:20:30"},
        ]

        # Explicitly set schema to StringType to avoid type inference issues
        schema = StructType([StructField("timestamp_str", StringType(), True)])
        df = spark.createDataFrame(test_data, schema=schema)

        # Format with optional fractional seconds should work
        result = df.withColumn(
            "parsed_timestamp",
            F.to_timestamp(F.col("timestamp_str"), "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]"),
        ).select("timestamp_str", "parsed_timestamp")

        rows = result.collect()
        assert len(rows) == 2
        assert all(row.parsed_timestamp is not None for row in rows)

    def test_type_casting_with_literal_values(self, spark):
        """Test type casting with literal values."""
        test_data = [{"id": 1}]

        df = spark.createDataFrame(test_data)

        # Cast literal values
        result = df.withColumn("casted_int", F.lit(100).cast("int")).select(
            "casted_int"
        )

        rows = result.collect()
        assert len(rows) == 1
        assert rows[0].casted_int == 100

    def test_multiple_casts_in_cte(self, spark):
        """Test multiple type casts in a CTE chain."""
        test_data = [{"value": "100", "date_str": "2025-01-01"}]

        df = spark.createDataFrame(test_data)

        # Multiple casts in sequence
        result = (
            df.withColumn("as_int", F.col("value").cast("int"))
            .withColumn("as_date", F.col("date_str").cast("date"))
            .withColumn("as_string", F.col("as_int").cast("string"))
            .select("as_int", "as_date", "as_string")
        )

        rows = result.collect()
        assert len(rows) == 1
        assert rows[0].as_int == 100
        assert rows[0].as_string == "100"
