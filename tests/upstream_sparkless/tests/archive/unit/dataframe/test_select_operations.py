"""
Unit tests for DataFrame select operations.
"""

import pytest
from sparkless import SparkSession, F


@pytest.mark.unit
class TestSelectOperations:
    """Test DataFrame select operations."""

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

    def test_select_single_column(self, spark, sample_data):
        """Test selecting a single column."""
        df = spark.createDataFrame(sample_data)
        result = df.select("name")

        assert result.count() == 4
        assert len(result.columns) == 1
        assert "name" in result.columns

    def test_select_multiple_columns(self, spark, sample_data):
        """Test selecting multiple columns."""
        df = spark.createDataFrame(sample_data)
        result = df.select("id", "name", "age")

        assert result.count() == 4
        assert len(result.columns) == 3
        assert "id" in result.columns
        assert "name" in result.columns
        assert "age" in result.columns

    def test_select_with_alias(self, spark, sample_data):
        """Test selecting columns with aliases."""
        df = spark.createDataFrame(sample_data)
        result = df.select(df.id.alias("user_id"), df.name.alias("full_name"))

        assert result.count() == 4
        assert len(result.columns) == 2
        assert "user_id" in result.columns
        assert "full_name" in result.columns

    def test_select_all_columns(self, spark, sample_data):
        """Test selecting all columns."""
        df = spark.createDataFrame(sample_data)
        result = df.select("*")

        assert result.count() == 4
        assert len(result.columns) == 5
        assert set(result.columns) == {"id", "name", "age", "salary", "active"}

    def test_select_with_expressions(self, spark, sample_data):
        """Test selecting with expressions."""
        df = spark.createDataFrame(sample_data)
        result = df.select("name", (df.age + 1).alias("age_plus_one"))

        assert result.count() == 4
        assert len(result.columns) == 2
        assert "name" in result.columns
        assert "age_plus_one" in result.columns

    def test_select_with_functions(self, spark, sample_data):
        """Test selecting with functions."""
        df = spark.createDataFrame(sample_data)
        result = df.select("name", F.upper("name").alias("name_upper"))

        assert result.count() == 4
        assert len(result.columns) == 2
        assert "name" in result.columns
        assert "name_upper" in result.columns
