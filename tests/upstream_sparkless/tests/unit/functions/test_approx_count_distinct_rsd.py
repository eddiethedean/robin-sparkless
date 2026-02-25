"""Unit tests for approx_count_distinct with rsd parameter (Issue #266)."""

import pytest
from sparkless.sql import SparkSession
import sparkless.sql.functions as F
from sparkless.window import Window


class TestApproxCountDistinctRsd:
    """Test approx_count_distinct with rsd parameter."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        return SparkSession.builder.appName("test").getOrCreate()

    @pytest.fixture
    def sample_data(self):
        """Sample data for testing."""
        return [
            {"type": "A", "value": 1},
            {"type": "A", "value": 10},
            {"type": "A", "value": 1},  # Duplicate
            {"type": "B", "value": 5},
            {"type": "B", "value": 5},  # Duplicate
        ]

    def test_approx_count_distinct_without_rsd(self, spark, sample_data):
        """Test backward compatibility - approx_count_distinct without rsd parameter."""
        df = spark.createDataFrame(sample_data)

        result = df.agg(F.approx_count_distinct("value"))
        rows = result.collect()

        assert len(rows) == 1
        # Should count distinct values: 1, 10, 5 = 3 distinct
        assert rows[0]["approx_count_distinct(value)"] == 3

    def test_approx_count_distinct_with_rsd(self, spark, sample_data):
        """Test approx_count_distinct with rsd parameter."""
        df = spark.createDataFrame(sample_data)

        result = df.agg(F.approx_count_distinct("value", rsd=0.01))
        rows = result.collect()

        assert len(rows) == 1
        # PySpark doesn't include rsd in column name, just uses base name
        # Should count distinct values: 1, 10, 5 = 3 distinct
        assert rows[0]["approx_count_distinct(value)"] == 3

    def test_approx_count_distinct_rsd_default_value(self, spark, sample_data):
        """Test that default rsd=None works correctly."""
        df = spark.createDataFrame(sample_data)

        # Both should produce the same result
        result1 = df.agg(F.approx_count_distinct("value"))
        result2 = df.agg(F.approx_count_distinct("value", rsd=None))

        rows1 = result1.collect()
        rows2 = result2.collect()

        assert (
            rows1[0]["approx_count_distinct(value)"]
            == rows2[0]["approx_count_distinct(value)"]
        )

    def test_approx_count_distinct_rsd_different_values(self, spark, sample_data):
        """Test various rsd values."""
        df = spark.createDataFrame(sample_data)

        # Test different rsd values - all should produce same result (exact counting in mock)
        rsd_values = [0.01, 0.05, 0.1, 0.2]
        results = []

        for rsd in rsd_values:
            result = df.agg(F.approx_count_distinct("value", rsd=rsd))
            rows = result.collect()
            results.append(rows[0])

        # All should produce the same count (3 distinct values)
        for result in results:
            # Find the approx_count_distinct column
            for col_name in result.asDict():
                if "approx_count_distinct" in col_name:
                    assert result[col_name] == 3
                    break

    def test_approx_count_distinct_in_groupby(self, spark, sample_data):
        """Test approx_count_distinct with groupBy."""
        df = spark.createDataFrame(sample_data)

        result = df.groupby("type").agg(
            F.approx_count_distinct("value", rsd=0.01).alias("distinct_count")
        )
        rows = result.collect()

        assert len(rows) == 2

        # Group A has values: 1, 10, 1 -> 2 distinct (1, 10)
        row_a = next((r for r in rows if r["type"] == "A"), None)
        assert row_a is not None
        assert row_a["distinct_count"] == 2

        # Group B has values: 5, 5 -> 1 distinct (5)
        row_b = next((r for r in rows if r["type"] == "B"), None)
        assert row_b is not None
        assert row_b["distinct_count"] == 1

    def test_approx_count_distinct_in_window(self, spark, sample_data):
        """Test approx_count_distinct with Window functions (fixes the None issue)."""
        df = spark.createDataFrame(sample_data)

        type_window = Window().partitionBy("type")

        result = df.withColumn(
            "approx_count", F.approx_count_distinct("value", rsd=0.01).over(type_window)
        )

        rows = result.collect()
        assert len(rows) == 5

        # All rows in group A should have approx_count = 2 (distinct: 1, 10)
        # All rows in group B should have approx_count = 1 (distinct: 5)
        for row in rows:
            assert row["approx_count"] is not None, "approx_count should not be None"
            if row["type"] == "A":
                assert row["approx_count"] == 2
            elif row["type"] == "B":
                assert row["approx_count"] == 1

    def test_approx_count_distinct_window_without_rsd(self, spark, sample_data):
        """Test approx_count_distinct in Window without rsd parameter."""
        df = spark.createDataFrame(sample_data)

        type_window = Window().partitionBy("type")

        result = df.withColumn(
            "approx_count", F.approx_count_distinct("value").over(type_window)
        )

        rows = result.collect()
        assert len(rows) == 5

        # Verify no None values
        for row in rows:
            assert row["approx_count"] is not None, "approx_count should not be None"
            if row["type"] == "A":
                assert row["approx_count"] == 2
            elif row["type"] == "B":
                assert row["approx_count"] == 1
