"""
Test to_date function with format strings for issue #126.

This test validates that F.to_date() correctly parses date strings with format strings,
matching PySpark behavior for various date formats.
"""

import pytest
from sparkless import SparkSession
from sparkless.spark_types import StructType, StructField, StringType
from sparkless import functions as F


class TestToDateWithFormat:
    """Test to_date function with format strings."""

    @pytest.fixture(autouse=True)
    def setup_method(self):
        """Set up test fixtures."""
        self.spark = SparkSession.builder.appName("to_date_format_test").getOrCreate()
        yield
        self.spark.stop()

    def test_to_date_with_yyyy_mm_dd_format(self):
        """Test to_date with 'yyyy-MM-dd' format (ISO 8601 standard)."""
        schema = StructType(
            [
                StructField("id", StringType(), False),
                StructField("name", StringType(), False),
                StructField("date_of_birth", StringType(), True),
            ]
        )

        data = [
            ("1", "Alice", "2005-12-23"),
            ("2", "Bob", "2004-12-23"),
            ("3", "Charlie", "1996-12-25"),
        ]

        df = self.spark.createDataFrame(data, schema)

        # This should work - convert string to date with format
        df_with_date = df.withColumn(
            "birth_date", F.to_date(F.col("date_of_birth"), "yyyy-MM-dd")
        )

        # Verify the operation completes without error
        result = df_with_date.collect()
        assert len(result) == 3

        # Verify that birth_date column exists and has date values
        for row in result:
            assert "birth_date" in row
            # birth_date should be a date object, not a string
            birth_date = row["birth_date"]
            assert birth_date is not None
            # Check that it's actually a date (not a string)
            assert hasattr(birth_date, "year"), (
                f"birth_date should be a date object, got {type(birth_date)}"
            )
            assert birth_date.year in [2005, 2004, 1996]

    def test_to_date_with_mm_slash_dd_slash_yyyy_format(self):
        """Test to_date with 'MM/dd/yyyy' format (US format)."""
        schema = StructType(
            [
                StructField("id", StringType(), False),
                StructField("event_date", StringType(), True),
            ]
        )

        data = [
            ("1", "12/23/2005"),
            ("2", "01/15/2004"),
            ("3", "06/30/1996"),
        ]

        df = self.spark.createDataFrame(data, schema)

        df_with_date = df.withColumn(
            "parsed_date", F.to_date(F.col("event_date"), "MM/dd/yyyy")
        )

        result = df_with_date.collect()
        assert len(result) == 3

        for row in result:
            assert "parsed_date" in row
            parsed_date = row["parsed_date"]
            assert parsed_date is not None
            assert hasattr(parsed_date, "year")

    def test_to_date_with_dd_hyphen_mm_hyphen_yyyy_format(self):
        """Test to_date with 'dd-MM-yyyy' format (European format)."""
        schema = StructType(
            [
                StructField("id", StringType(), False),
                StructField("event_date", StringType(), True),
            ]
        )

        data = [
            ("1", "23-12-2005"),
            ("2", "15-01-2004"),
            ("3", "30-06-1996"),
        ]

        df = self.spark.createDataFrame(data, schema)

        df_with_date = df.withColumn(
            "parsed_date", F.to_date(F.col("event_date"), "dd-MM-yyyy")
        )

        result = df_with_date.collect()
        assert len(result) == 3

        for row in result:
            assert "parsed_date" in row
            parsed_date = row["parsed_date"]
            assert parsed_date is not None
            assert hasattr(parsed_date, "year")

    def test_to_date_without_format_string(self):
        """Test to_date without format string (should auto-detect common formats)."""
        schema = StructType(
            [
                StructField("id", StringType(), False),
                StructField("date_str", StringType(), True),
            ]
        )

        data = [
            ("1", "2005-12-23"),  # ISO format should work
        ]

        df = self.spark.createDataFrame(data, schema)

        # to_date without format should try to parse common formats
        df_with_date = df.withColumn("parsed_date", F.to_date(F.col("date_str")))

        result = df_with_date.collect()
        assert len(result) == 1
        assert "parsed_date" in result[0]
