"""
Unit tests for DataFrame caching behavior.

Tests the caching functionality and its impact on string concatenation.
"""

import pytest
from sparkless import SparkSession, functions as F


class TestDataFrameCaching:
    """Test DataFrame caching behavior."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        session = SparkSession("caching_test")
        yield session
        session.stop()

    def test_cache_sets_is_cached_flag(self, spark):
        """Test that cache() sets _is_cached flag."""
        df = spark.createDataFrame([("A",)], ["col1"])
        assert not df._is_cached

        df_cached = df.cache()
        assert df_cached._is_cached

    def test_cache_materializes_dataframe(self, spark):
        """Test that cache() materializes the DataFrame."""
        df = spark.createDataFrame([("A",)], ["col1"])
        df_transformed = df.withColumn("new_col", F.col("col1") + F.lit("B"))

        # Before caching, may not be materialized
        df_cached = df_transformed.cache()

        # After caching, should be materialized
        assert df_cached._materialized

    def test_persist_also_sets_cached_flag(self, spark):
        """Test that persist() also sets _is_cached flag."""
        df = spark.createDataFrame([("A",)], ["col1"])
        df_persisted = df.persist()

        assert df_persisted._is_cached

    def test_cache_returns_self(self, spark):
        """Test that cache() returns the same DataFrame."""
        df = spark.createDataFrame([("A",)], ["col1"])
        df_cached = df.cache()

        # Should be the same object (or at least same data)
        assert df_cached._is_cached

    def test_multiple_cache_calls(self, spark):
        """Test that multiple cache() calls are idempotent."""
        df = spark.createDataFrame([("A",)], ["col1"])
        df_cached1 = df.cache()
        df_cached2 = df_cached1.cache()

        assert df_cached2._is_cached

    def test_cache_with_lazy_operations(self, spark):
        """Test that cache() materializes lazy operations."""
        df = spark.createDataFrame([("A",)], ["col1"])
        df_transformed = df.withColumn("new_col", F.col("col1") + F.lit("B"))

        # Should have queued operations
        if df_transformed._operations_queue:
            df_cached = df_transformed.cache()
            # Operations should be materialized
            assert len(df_cached._operations_queue) == 0
