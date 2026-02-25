"""
Compatibility tests for string concatenation operations.

This module tests MockSpark's string concatenation against PySpark behavior,
focusing on concat_ws, + operator, and type coercion.
"""

import pytest
from sparkless import SparkSession, F


@pytest.mark.compatibility
class TestStringConcatenationCompatibility:
    """Test string concatenation compatibility."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        session = SparkSession("string_concatenation_test")
        yield session
        session.stop()

    def test_concat_ws_basic(self, spark):
        """Test concat_ws with basic string columns."""
        test_data = [
            {"first_name": "John", "last_name": "Doe"},
            {"first_name": "Jane", "last_name": "Smith"},
        ]

        df = spark.createDataFrame(test_data)
        result = df.select(
            F.concat_ws(" ", F.col("first_name"), F.col("last_name")).alias("full_name")
        )

        rows = result.collect()
        assert len(rows) == 2
        assert rows[0].full_name == "John Doe"
        assert rows[1].full_name == "Jane Smith"

    def test_concat_ws_multiple_columns(self, spark):
        """Test concat_ws with multiple columns."""
        test_data = [
            {"first": "John", "middle": "Q", "last": "Doe"},
            {"first": "Jane", "middle": "A", "last": "Smith"},
        ]

        df = spark.createDataFrame(test_data)
        result = df.select(
            F.concat_ws("-", F.col("first"), F.col("middle"), F.col("last")).alias(
                "full_name"
            )
        )

        rows = result.collect()
        assert len(rows) == 2
        assert rows[0].full_name == "John-Q-Doe"
        assert rows[1].full_name == "Jane-A-Smith"

    def test_string_plus_operator(self, spark):
        """Test string + operator for concatenation."""
        test_data = [
            {"first_name": "John", "last_name": "Doe"},
            {"first_name": "Jane", "last_name": "Smith"},
        ]

        df = spark.createDataFrame(test_data)
        result = df.select(
            (F.col("first_name") + F.lit(" ") + F.col("last_name")).alias("full_name")
        )

        rows = result.collect()
        assert len(rows) == 2
        assert rows[0].full_name == "John Doe"
        assert rows[1].full_name == "Jane Smith"

    def test_string_plus_operator_with_literal_first(self, spark):
        """Test string + operator with literal first."""
        test_data = [
            {"name": "World"},
            {"name": "Spark"},
        ]

        df = spark.createDataFrame(test_data)
        result = df.select((F.lit("Hello ") + F.col("name")).alias("greeting"))

        rows = result.collect()
        assert len(rows) == 2
        assert rows[0].greeting == "Hello World"
        assert rows[1].greeting == "Hello Spark"

    def test_string_plus_with_varchar_column(self, spark):
        """Test string + operator with VARCHAR column and string literal."""
        test_data = [
            {"text": "Hello"},
            {"text": "World"},
        ]

        df = spark.createDataFrame(test_data)
        result = df.select((F.col("text") + F.lit(" World")).alias("greeting"))

        rows = result.collect()
        assert len(rows) == 2
        assert rows[0].greeting == "Hello World"
        assert rows[1].greeting == "World World"
