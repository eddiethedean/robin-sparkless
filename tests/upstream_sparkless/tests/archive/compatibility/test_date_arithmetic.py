"""
Compatibility tests for date arithmetic operations.

This module tests MockSpark's date arithmetic against PySpark behavior,
ensuring that datediff() auto-converts string dates without requiring explicit to_date().
"""

import pytest
from sparkless import SparkSession, F


@pytest.mark.compatibility
class TestDateArithmeticCompatibility:
    """Test date arithmetic compatibility."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        session = SparkSession("date_arithmetic_test")
        yield session
        session.stop()

    def test_datediff_with_string_dates(self, spark):
        """Test datediff() with string date columns (should auto-convert)."""
        test_data = [
            {"start_date": "2025-01-01", "end_date": "2025-01-15"},
            {"start_date": "2025-02-01", "end_date": "2025-02-28"},
            {"start_date": "2025-01-01", "end_date": "2025-12-31"},
        ]

        df = spark.createDataFrame(test_data)
        result = df.select(
            F.datediff(F.col("end_date"), F.col("start_date")).alias("days_diff")
        )

        rows = result.collect()
        assert len(rows) == 3
        assert rows[0].days_diff == 14  # Jan 15 - Jan 1 = 14 days
        assert rows[1].days_diff == 27  # Feb 28 - Feb 1 = 27 days
        assert rows[2].days_diff == 364  # Dec 31 - Jan 1 = 364 days

    def test_datediff_with_mixed_types(self, spark):
        """Test datediff() with one string date and one date type."""
        test_data = [
            {"date_str": "2025-01-01", "date_col": "2025-01-15"},
        ]

        df = spark.createDataFrame(test_data)
        # Convert one to date type
        df = df.withColumn("date_typed", F.to_date(F.col("date_col")))
        result = df.select(
            F.datediff(F.col("date_typed"), F.col("date_str")).alias("days_diff")
        )

        rows = result.collect()
        assert len(rows) == 1
        assert rows[0].days_diff == 14

    def test_datediff_with_date_literals(self, spark):
        """Test datediff() with date literals."""
        test_data = [
            {"start_date": "2025-01-01"},
            {"start_date": "2025-06-15"},
        ]

        df = spark.createDataFrame(test_data)
        result = df.select(
            F.datediff(F.lit("2025-12-31"), F.col("start_date")).alias("days_diff")
        )

        rows = result.collect()
        assert len(rows) == 2
        # Should handle date literals correctly
        assert rows[0].days_diff == 364  # Dec 31 - Jan 1
        assert rows[1].days_diff == 199  # Dec 31 - Jun 15

    def test_datediff_with_explicit_to_date_still_works(self, spark):
        """Test that explicit to_date() conversion still works (backward compatibility)."""
        test_data = [
            {"start_date": "2025-01-01", "end_date": "2025-01-15"},
        ]

        df = spark.createDataFrame(test_data)
        # Explicit conversion should still work
        df = df.withColumn("start_typed", F.to_date(F.col("start_date")))
        df = df.withColumn("end_typed", F.to_date(F.col("end_date")))
        result = df.select(
            F.datediff(F.col("end_typed"), F.col("start_typed")).alias("days_diff")
        )

        rows = result.collect()
        assert len(rows) == 1
        assert rows[0].days_diff == 14
