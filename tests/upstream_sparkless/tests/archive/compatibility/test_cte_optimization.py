"""
Compatibility tests for CTE optimization.

This module tests that complex CTE chains with multiple operations work correctly
without falling back to table-per-operation, especially with datetime parsing.
"""

import pytest
import warnings
from sparkless import SparkSession, F


@pytest.mark.compatibility
class TestCTEOptimizationCompatibility:
    """Test CTE optimization compatibility."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        session = SparkSession("cte_optimization_test")
        yield session
        session.stop()

    def test_complex_cte_chain_with_datetime(self, spark):
        """Test complex CTE chain with datetime parsing operations."""
        from sparkless.spark_types import (
            StructType,
            StructField,
            StringType,
            LongType,
        )

        test_data = [
            {"id": 1, "timestamp_str": "2025-10-29T10:30:45.123456", "value": 100},
            {"id": 2, "timestamp_str": "2025-10-29T14:20:30", "value": 200},
        ]

        # Use explicit schema to ensure timestamp_str is StringType, not TimestampType
        schema = StructType(
            [
                StructField("id", LongType(), True),
                StructField("timestamp_str", StringType(), True),
                StructField("value", LongType(), True),
            ]
        )

        df = spark.createDataFrame(test_data, schema=schema)

        # Complex chain: filter -> withColumn (datetime) -> withColumn (calculation) -> select
        with warnings.catch_warnings():
            warnings.simplefilter("error")  # Fail on CTE fallback warnings
            try:
                result = (
                    df.filter(F.col("value") > 50)
                    .withColumn(
                        "parsed_timestamp",
                        F.to_timestamp(
                            F.col("timestamp_str"), "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]"
                        ),
                    )
                    .withColumn("doubled", F.col("value") * 2)
                    .select("id", "parsed_timestamp", "doubled")
                )

                rows = result.collect()
                assert len(rows) == 2
                # Verify results are correct
                assert all(row.doubled is not None for row in rows)
            except UserWarning as e:
                if "CTE optimization failed" in str(e):
                    pytest.fail(f"CTE optimization should succeed but fell back: {e}")
                raise

    def test_multiple_withcolumn_operations_in_cte(self, spark):
        """Test multiple withColumn operations in CTE chain."""
        test_data = [
            {"name": "Alice", "age": 25, "salary": 50000},
            {"name": "Bob", "age": 30, "salary": 60000},
        ]

        df = spark.createDataFrame(test_data)

        # Chain of 5+ operations
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            try:
                result = (
                    df.withColumn("age_plus_1", F.col("age") + 1)
                    .withColumn("age_plus_2", F.col("age_plus_1") + 1)
                    .withColumn("bonus", F.col("salary") * 0.1)
                    .withColumn("total", F.col("salary") + F.col("bonus"))
                    .select("name", "age_plus_2", "total")
                )

                rows = result.collect()
                assert len(rows) == 2
                assert rows[0].age_plus_2 == 27
                assert rows[0].total == 55000.0
            except UserWarning as e:
                if "CTE optimization failed" in str(e):
                    pytest.fail(f"CTE optimization should succeed but fell back: {e}")
                raise

    def test_cte_with_string_operations(self, spark):
        """Test CTE chain with string concatenation operations."""
        test_data = [
            {"first": "John", "last": "Doe"},
            {"first": "Jane", "last": "Smith"},
        ]

        df = spark.createDataFrame(test_data)

        # Chain with string operations
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            try:
                result = (
                    df.withColumn(
                        "full_name", F.concat_ws(" ", F.col("first"), F.col("last"))
                    )
                    .withColumn("greeting", F.lit("Hello ") + F.col("full_name"))
                    .withColumn("upper_greeting", F.upper(F.col("greeting")))
                    .select("full_name", "upper_greeting")
                )

                rows = result.collect()
                assert len(rows) == 2
                assert rows[0].full_name == "John Doe"
                assert rows[0].upper_greeting == "HELLO JOHN DOE"
            except UserWarning as e:
                if "CTE optimization failed" in str(e):
                    pytest.fail(f"CTE optimization should succeed but fell back: {e}")
                raise

    def test_cte_with_boolean_expressions(self, spark):
        """Test CTE chain with boolean expressions."""
        test_data = [
            {"value": 25, "min": 10, "max": 50},
            {"value": 5, "min": 10, "max": 50},
        ]

        df = spark.createDataFrame(test_data)

        # Chain with boolean operations
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            try:
                result = (
                    df.withColumn(
                        "is_valid",
                        (F.col("value") >= F.col("min"))
                        & (F.col("value") <= F.col("max")),
                    )
                    .withColumn(
                        "is_extreme",
                        (F.col("value") < F.col("min"))
                        | (F.col("value") > F.col("max")),
                    )
                    .filter(F.col("is_valid") == F.lit(True))
                    .select("value", "is_valid")
                )

                rows = result.collect()
                assert len(rows) == 1  # Only valid value should pass filter
                assert rows[0].is_valid is True
            except UserWarning as e:
                if "CTE optimization failed" in str(e):
                    pytest.fail(f"CTE optimization should succeed but fell back: {e}")
                raise

    def test_cte_filter_with_column_references(self, spark):
        """Test CTE filter operations use CTE aliases, not original table names."""
        test_data = [
            {"id": 1, "name": "Alice", "value": 100},
            {"id": 2, "name": "Bob", "value": None},
            {"id": 3, "name": "Charlie", "value": 200},
        ]

        df = spark.createDataFrame(test_data)

        # Filter using isNotNull - this should use CTE alias, not temp_table_0
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            try:
                result = (
                    df.filter(F.col("value").isNotNull())
                    .filter(F.col("id") > 1)
                    .select("id", "name", "value")
                )

                rows = result.collect()
                assert len(rows) == 1  # Only id=3 should pass both filters
                assert rows[0].id == 3
                assert rows[0].value == 200
            except UserWarning as e:
                if "CTE optimization failed" in str(e):
                    pytest.fail(f"CTE optimization should succeed but fell back: {e}")
                raise

    def test_cte_filter_with_complex_conditions(self, spark):
        """Test CTE filter with complex conditions referencing columns."""
        test_data = [
            {"x": 10, "y": 20, "z": 30},
            {"x": 5, "y": 15, "z": 25},
            {"x": 15, "y": 25, "z": 35},
        ]

        df = spark.createDataFrame(test_data)

        # Complex filter with multiple column references
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            try:
                result = (
                    df.filter((F.col("x") + F.col("y")) > F.col("z"))
                    .filter(F.col("x") < 20)
                    .select("x", "y", "z")
                )

                rows = result.collect()
                assert len(rows) == 1  # Only first row: 10+20=30 > 30 is False, wait...
                # Actually: 10+20=30 is not > 30, so that row fails
                # 5+15=20 is not > 25, so that row fails
                # 15+25=40 > 35 and 15 < 20, so that row passes
                assert rows[0].x == 15
            except UserWarning as e:
                if "CTE optimization failed" in str(e):
                    pytest.fail(f"CTE optimization should succeed but fell back: {e}")
                raise
