"""
Tests for column availability and materialization requirements.

This test suite verifies that columns created in transforms require
materialization before they can be accessed, matching PySpark behavior.
"""

import pytest

from sparkless.testing import get_imports, Mode, get_mode


def _is_sparkless_mode() -> bool:
    """Check if running in sparkless mode."""
    return get_mode() == Mode.SPARKLESS


class TestColumnAvailability:
    """Test column materialization requirements."""

    def test_materialized_columns_are_available(self, spark):
        """Test that materialized columns are available."""
        df = spark.createDataFrame([{"id": 1, "value": 10}], ["id", "value"])

        # Test public API - columns should be accessible
        assert "id" in df.columns
        assert "value" in df.columns

        # Test sparkless internals only when available
        if _is_sparkless_mode() and hasattr(df, "_get_available_columns"):
            available = df._get_available_columns()
            assert "id" in available
            assert "value" in available

    def test_columns_available_after_collect(self, spark):
        """Test that columns are available after collect()."""
        imports = get_imports()
        F = imports.F
        StructType = imports.StructType
        StructField = imports.StructField
        IntegerType = imports.IntegerType

        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("value", IntegerType(), True),
            ]
        )
        df = spark.createDataFrame([{"id": 1, "value": 10}], schema=schema)
        df = df.withColumn("new_col", F.col("value") + 1)

        # Materialize by collecting
        df.collect()

        # Test public API - new_col should be in columns
        assert "new_col" in df.columns

        # Test sparkless internals only when available
        if _is_sparkless_mode() and hasattr(df, "_get_available_columns"):
            available = df._get_available_columns()
            assert "new_col" in available

    def test_columns_available_after_show(self, spark):
        """Test that columns are available after show()."""
        imports = get_imports()
        F = imports.F
        StructType = imports.StructType
        StructField = imports.StructField
        IntegerType = imports.IntegerType

        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("value", IntegerType(), True),
            ]
        )
        df = spark.createDataFrame([{"id": 1, "value": 10}], schema=schema)
        df = df.withColumn("new_col", F.col("value") + 1)

        # Materialize by showing
        df.show()

        # Test public API - new_col should be in columns
        assert "new_col" in df.columns

        # Test sparkless internals only when available
        if _is_sparkless_mode() and hasattr(df, "_get_available_columns"):
            available = df._get_available_columns()
            assert "new_col" in available

    def test_dataframe_is_marked_materialized(self, spark):
        """Test that DataFrame is marked as materialized after actions."""
        imports = get_imports()
        F = imports.F
        StructType = imports.StructType
        StructField = imports.StructField
        IntegerType = imports.IntegerType

        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("value", IntegerType(), True),
            ]
        )
        df = spark.createDataFrame([{"id": 1, "value": 10}], schema=schema)

        # Test sparkless internals only when available
        if _is_sparkless_mode() and hasattr(df, "_materialized"):
            assert df._materialized is True  # Created from data, so materialized

        df2 = df.withColumn("new", F.col("value") + 1)

        # After action, should be materialized
        df2.collect()

        # Test public API - columns should be accessible
        assert "new" in df2.columns

        # Test sparkless internals only when available
        if _is_sparkless_mode() and hasattr(df2, "_materialized"):
            assert df2._materialized is True
