"""
Compatibility tests for type coercion edge cases.

This module tests MockSpark's type coercion behavior against PySpark,
ensuring proper implicit type conversions in various operations.
"""

import pytest
from sparkless import SparkSession, F


@pytest.mark.compatibility
class TestTypeCoercionCompatibility:
    """Test type coercion compatibility."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        session = SparkSession("type_coercion_test")
        yield session
        session.stop()

    def test_string_numeric_coercion_in_concat(self, spark):
        """Test string + numeric type coercion in concatenation."""
        test_data = [
            {"name": "Product", "price": 100},
            {"name": "Item", "price": 200},
        ]

        df = spark.createDataFrame(test_data)
        # String + numeric should coerce numeric to string
        result = df.select(
            (F.col("name") + F.lit(": $") + F.col("price").cast("string")).alias(
                "description"
            )
        )

        rows = result.collect()
        assert len(rows) == 2
        assert rows[0].description == "Product: $100"
        assert rows[1].description == "Item: $200"

    def test_boolean_coercion_in_aggregation(self, spark):
        """Test boolean type coercion in aggregations."""
        test_data = [
            {"category": "A", "is_active": True},
            {"category": "A", "is_active": False},
            {"category": "B", "is_active": True},
        ]

        df = spark.createDataFrame(test_data)
        # Sum of boolean should work
        result = df.groupBy("category").agg(
            F.sum(F.col("is_active").cast("int")).alias("active_count")
        )

        rows = result.collect()
        assert len(rows) == 2
        # Category A: 1 active, Category B: 1 active
        assert any(row.category == "A" and row.active_count == 1 for row in rows)

    def test_numeric_type_promotion(self, spark):
        """Test numeric type promotion (int + float -> float)."""
        test_data = [
            {"int_val": 10, "float_val": 5.5},
            {"int_val": 20, "float_val": 3.25},
        ]

        df = spark.createDataFrame(test_data)
        # Int + Float should promote to Float
        result = df.select((F.col("int_val") + F.col("float_val")).alias("sum"))

        rows = result.collect()
        assert len(rows) == 2
        assert rows[0].sum == 15.5
        assert rows[1].sum == 23.25

    def test_string_coercion_with_literals(self, spark):
        """Test string coercion with numeric literals."""
        test_data = [
            {"id": 1},
            {"id": 2},
        ]

        df = spark.createDataFrame(test_data)
        # String column + numeric literal should work
        result = df.select(
            (F.col("id").cast("string") + F.lit(" items")).alias("description")
        )

        rows = result.collect()
        assert len(rows) == 2
        assert rows[0].description == "1 items"
        assert rows[1].description == "2 items"

    def test_mixed_type_comparison(self, spark):
        """Test comparison operations with mixed types."""
        test_data = [
            {"value": "100"},
            {"value": "200"},
        ]

        df = spark.createDataFrame(test_data)
        # String should be comparable (lexicographic or numeric depending on context)
        result = df.filter(F.col("value") > "150")

        rows = result.collect()
        assert len(rows) == 1
        assert rows[0].value == "200"
