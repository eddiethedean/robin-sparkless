"""
Compatibility tests for column lifecycle management.

This module tests that columns created in early transforms are properly
available in later transforms, especially in multi-step pipelines.
"""

import pytest
from sparkless import SparkSession, F


@pytest.mark.compatibility
class TestColumnLifecycleCompatibility:
    """Test column lifecycle and persistence compatibility."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        session = SparkSession("column_lifecycle_test")
        yield session
        session.stop()

    def test_column_created_in_withcolumn_available_later(self, spark):
        """Test that columns created with withColumn are available in subsequent operations."""
        test_data = [
            {"id": 1, "value": 10},
            {"id": 2, "value": 20},
            {"id": 3, "value": 30},
        ]

        df = spark.createDataFrame(test_data)
        # Create a column in first transform
        df = df.withColumn("doubled", F.col("value") * 2)
        # Use that column in second transform
        result = df.withColumn("quadrupled", F.col("doubled") * 2)

        rows = result.collect()
        assert len(rows) == 3
        assert rows[0].doubled == 20
        assert rows[0].quadrupled == 40
        assert rows[1].doubled == 40
        assert rows[1].quadrupled == 80

    def test_column_persistence_through_multiple_transforms(self, spark):
        """Test column persistence through multiple transform operations."""
        test_data = [
            {"name": "Alice", "age": 25},
            {"name": "Bob", "age": 30},
        ]

        df = spark.createDataFrame(test_data)
        # Multiple transforms
        df = df.withColumn("age_plus_1", F.col("age") + 1)
        df = df.withColumn("age_plus_2", F.col("age_plus_1") + 1)
        df = df.withColumn("age_plus_3", F.col("age_plus_2") + 1)

        rows = df.collect()
        assert len(rows) == 2
        assert rows[0].age == 25
        assert rows[0].age_plus_1 == 26
        assert rows[0].age_plus_2 == 27
        assert rows[0].age_plus_3 == 28

    def test_column_available_after_select(self, spark):
        """Test that columns are available after select operation."""
        test_data = [
            {"id": 1, "name": "Alice", "age": 25},
            {"id": 2, "name": "Bob", "age": 30},
        ]

        df = spark.createDataFrame(test_data)
        # Create column, then select it
        df = df.withColumn(
            "full_info", F.concat_ws(" - ", F.col("name"), F.col("age").cast("string"))
        )
        result = df.select("id", "full_info")

        rows = result.collect()
        assert len(rows) == 2
        assert rows[0].full_info == "Alice - 25"
        assert rows[1].full_info == "Bob - 30"

    def test_nested_column_references(self, spark):
        """Test that nested column references work correctly."""
        test_data = [
            {"base": 10},
            {"base": 20},
        ]

        df = spark.createDataFrame(test_data)
        # Create column that references another created column
        df = df.withColumn("level1", F.col("base") * 2)
        df = df.withColumn("level2", F.col("level1") * 2)
        df = df.withColumn("level3", F.col("level2") * 2)

        rows = df.collect()
        assert len(rows) == 2
        assert rows[0].base == 10
        assert rows[0].level1 == 20
        assert rows[0].level2 == 40
        assert rows[0].level3 == 80

    def test_column_in_complex_expression(self, spark):
        """Test column created with complex expression available later."""
        test_data = [
            {"price": 100, "tax_rate": 0.1},
            {"price": 200, "tax_rate": 0.15},
        ]

        df = spark.createDataFrame(test_data)
        # Create column with complex expression
        df = df.withColumn("tax", F.col("price") * F.col("tax_rate"))
        # Use that column in subsequent calculation
        result = df.withColumn("total", F.col("price") + F.col("tax"))

        rows = result.collect()
        assert len(rows) == 2
        assert rows[0].tax == 10.0
        assert rows[0].total == 110.0
        assert rows[1].tax == 30.0
        assert rows[1].total == 230.0
