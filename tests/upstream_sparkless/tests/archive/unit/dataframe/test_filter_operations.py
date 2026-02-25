"""
Unit tests for DataFrame filter operations.
"""

import pytest
from sparkless import SparkSession, F


@pytest.mark.unit
class TestFilterOperations:
    """Test DataFrame filter operations."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        return SparkSession("test")

    @pytest.fixture
    def sample_data(self):
        """Sample data for testing."""
        return [
            {"id": 1, "name": "Alice", "age": 25, "salary": 50000.0, "active": True},
            {"id": 2, "name": "Bob", "age": 30, "salary": 60000.0, "active": False},
            {"id": 3, "name": "Charlie", "age": 35, "salary": 70000.0, "active": True},
            {"id": 4, "name": "Diana", "age": 28, "salary": 55000.0, "active": True},
        ]

    def test_filter_by_column(self, spark, sample_data):
        """Test filtering by column value."""
        df = spark.createDataFrame(sample_data)
        result = df.filter(df.age > 30)

        assert result.count() == 1
        assert len(result.columns) == 5

    def test_filter_with_boolean_column(self, spark, sample_data):
        """Test filtering with boolean column."""
        df = spark.createDataFrame(sample_data)
        result = df.filter(df.active)

        assert result.count() == 3
        assert len(result.columns) == 5

    def test_filter_with_multiple_conditions(self, spark, sample_data):
        """Test filtering with multiple conditions."""
        df = spark.createDataFrame(sample_data)
        result = df.filter((df.age > 25) & (df.salary > 50000))

        assert result.count() == 3
        assert len(result.columns) == 5

    def test_filter_with_or_condition(self, spark, sample_data):
        """Test filtering with OR condition."""
        df = spark.createDataFrame(sample_data)
        result = df.filter((df.age < 26) | (df.age > 34))

        assert result.count() == 2
        assert len(result.columns) == 5

    def test_filter_with_functions(self, spark, sample_data):
        """Test filtering with functions."""
        df = spark.createDataFrame(sample_data)
        result = df.filter(F.col("age") > 30)

        assert result.count() == 1
        assert len(result.columns) == 5

    def test_filter_with_string_operations(self, spark, sample_data):
        """Test filtering with string operations."""
        df = spark.createDataFrame(sample_data)
        result = df.filter(df.name.startswith("A"))

        assert result.count() == 1
        assert len(result.columns) == 5
