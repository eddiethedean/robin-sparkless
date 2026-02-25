"""
Compatibility tests for boolean expression evaluation.

This module tests MockSpark's boolean operations against PySpark behavior,
ensuring that direct boolean operations (| and &) work without requiring when().otherwise() chains.
"""

import pytest
from sparkless import SparkSession, F


@pytest.mark.compatibility
class TestBooleanExpressionsCompatibility:
    """Test boolean expression compatibility."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        session = SparkSession("boolean_expressions_test")
        yield session
        session.stop()

    def test_boolean_or_operation(self, spark):
        """Test OR operation (|) creating boolean columns."""
        test_data = [
            {"value": 25, "min": 10, "max": 50},
            {"value": 5, "min": 10, "max": 50},
            {"value": 60, "min": 10, "max": 50},
        ]

        df = spark.createDataFrame(test_data)
        result = df.withColumn(
            "out_of_range",
            (F.col("value") < F.col("min")) | (F.col("value") > F.col("max")),
        )

        rows = result.collect()
        assert len(rows) == 3
        # First row: 25 is in range, should be False
        assert rows[0].out_of_range is False
        # Second row: 5 < 10, should be True
        assert rows[1].out_of_range is True
        # Third row: 60 > 50, should be True
        assert rows[2].out_of_range is True

    def test_boolean_and_operation(self, spark):
        """Test AND operation (&) creating boolean columns."""
        test_data = [
            {"age": 25, "has_license": True},
            {"age": 17, "has_license": True},
            {"age": 25, "has_license": False},
        ]

        df = spark.createDataFrame(test_data)
        result = df.withColumn(
            "can_drive", (F.col("age") >= 18) & (F.col("has_license") == F.lit(True))
        )

        rows = result.collect()
        assert len(rows) == 3
        # First row: age >= 18 and has license, should be True
        assert rows[0].can_drive is True
        # Second row: age < 18, should be False
        assert rows[1].can_drive is False
        # Third row: no license, should be False
        assert rows[2].can_drive is False

    def test_boolean_not_operation(self, spark):
        """Test NOT operation (!) creating boolean columns."""
        test_data = [
            {"is_active": True},
            {"is_active": False},
            {"is_active": True},
        ]

        df = spark.createDataFrame(test_data)
        result = df.withColumn("is_inactive", ~F.col("is_active"))

        rows = result.collect()
        assert len(rows) == 3
        assert rows[0].is_inactive is False
        assert rows[1].is_inactive is True
        assert rows[2].is_inactive is False

    def test_complex_boolean_expression(self, spark):
        """Test complex boolean expression with multiple operations."""
        test_data = [
            {"value": 25, "min": 10, "max": 50, "threshold": 20},
            {"value": 15, "min": 10, "max": 50, "threshold": 20},
            {"value": 60, "min": 10, "max": 50, "threshold": 20},
        ]

        df = spark.createDataFrame(test_data)
        result = df.withColumn(
            "is_valid",
            ((F.col("value") >= F.col("min")) & (F.col("value") <= F.col("max")))
            | (F.col("value") > F.col("threshold")),
        )

        rows = result.collect()
        assert len(rows) == 3
        # All should be True based on the conditions
        assert all(row.is_valid is True for row in rows)

    def test_boolean_in_filter(self, spark):
        """Test boolean expressions used in filter operations."""
        test_data = [
            {"name": "Alice", "age": 25},
            {"name": "Bob", "age": 17},
            {"name": "Charlie", "age": 30},
        ]

        df = spark.createDataFrame(test_data)
        result = df.filter((F.col("age") >= 18) | (F.col("name") == "Bob"))

        rows = result.collect()
        # Should include Alice (>=18), Bob (name match), and Charlie (>=18)
        assert len(rows) == 3
