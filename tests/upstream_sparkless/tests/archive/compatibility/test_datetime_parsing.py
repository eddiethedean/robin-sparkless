"""
Compatibility tests for datetime parsing with microseconds and optional fractional seconds.

This module tests MockSpark's datetime parsing against PySpark behavior,
specifically focusing on optional fractional seconds patterns like [.SSSSSS].
"""

import pytest
from sparkless import SparkSession, F


@pytest.mark.compatibility
class TestDatetimeParsingCompatibility:
    """Test datetime parsing compatibility, especially with microseconds."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        session = SparkSession("datetime_parsing_test")
        yield session
        session.stop()

    def test_to_timestamp_with_microseconds(self, spark):
        """Test that to_timestamp handles microseconds like PySpark."""
        test_data = [
            {"id": 1, "timestamp_str": "2025-10-29T10:30:45.123456"},
            {"id": 2, "timestamp_str": "2025-10-29T10:30:45.987654"},
            {"id": 3, "timestamp_str": "2025-10-29T10:30:45.000001"},
        ]

        df = spark.createDataFrame(test_data)
        result = df.select(
            "timestamp_str",
            F.to_timestamp(
                F.col("timestamp_str"), "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]"
            ).alias("parsed"),
        )

        # Verify that parsing succeeded (should not raise error)
        rows = result.collect()
        assert len(rows) == 3
        # All should have parsed successfully
        for row in rows:
            assert row.parsed is not None

    def test_to_timestamp_without_microseconds(self, spark):
        """Test that to_timestamp handles timestamps without microseconds."""
        test_data = [
            {"id": 1, "timestamp_str": "2025-10-29T10:30:45"},
            {"id": 2, "timestamp_str": "2025-10-29T14:20:30"},
        ]

        df = spark.createDataFrame(test_data)
        result = df.select(
            "timestamp_str",
            F.to_timestamp(
                F.col("timestamp_str"), "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]"
            ).alias("parsed"),
        )

        # Verify that parsing succeeded
        rows = result.collect()
        assert len(rows) == 2
        for row in rows:
            assert row.parsed is not None

    def test_to_timestamp_mixed_precision(self, spark):
        """Test to_timestamp with mixed precision (some with microseconds, some without)."""
        test_data = [
            {"id": 1, "timestamp_str": "2025-10-29T10:30:45.123456"},
            {"id": 2, "timestamp_str": "2025-10-29T10:30:45"},
            {"id": 3, "timestamp_str": "2025-10-29T10:30:45.999999"},
        ]

        df = spark.createDataFrame(test_data)
        result = df.select(
            "timestamp_str",
            F.to_timestamp(
                F.col("timestamp_str"), "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]"
            ).alias("parsed"),
        )

        # All should parse successfully despite mixed precision
        rows = result.collect()
        assert len(rows) == 3
        for row in rows:
            assert row.parsed is not None

    def test_to_timestamp_with_milliseconds(self, spark):
        """Test to_timestamp with milliseconds (3 digits)."""
        test_data = [
            {"id": 1, "timestamp_str": "2025-10-29T10:30:45.123"},
            {"id": 2, "timestamp_str": "2025-10-29T10:30:45.456"},
        ]

        df = spark.createDataFrame(test_data)
        # Using [.SSS] pattern for milliseconds
        result = df.select(
            "timestamp_str",
            F.to_timestamp(F.col("timestamp_str"), "yyyy-MM-dd'T'HH:mm:ss[.SSS]").alias(
                "parsed"
            ),
        )

        rows = result.collect()
        assert len(rows) == 2
        for row in rows:
            assert row.parsed is not None

    def test_to_timestamp_iso_format(self, spark):
        """Test ISO 8601 format with optional fractional seconds."""
        test_data = [
            {"id": 1, "timestamp_str": "2025-10-29T10:30:45.123456"},
            {"id": 2, "timestamp_str": "2025-10-29T10:30:45"},
        ]

        df = spark.createDataFrame(test_data)
        result = df.select(
            F.to_timestamp(
                F.col("timestamp_str"), "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]"
            ).alias("parsed")
        )

        # Should parse both formats
        rows = result.collect()
        assert len(rows) == 2
        assert all(row.parsed is not None for row in rows)
