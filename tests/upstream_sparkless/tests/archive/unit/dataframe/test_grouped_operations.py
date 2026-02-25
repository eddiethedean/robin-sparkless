"""
Unit tests for grouped operations (cube, pivot, rollup).
"""

import pytest
from sparkless import SparkSession, F


@pytest.mark.unit
class TestCubeOperations:
    """Test CubeGroupedData operations."""

    def test_cube_basic(self):
        """Test basic cube operation."""
        spark = SparkSession("test")
        df = spark.createDataFrame(
            [
                {"dept": "IT", "region": "US", "sales": 100},
                {"dept": "IT", "region": "EU", "sales": 200},
                {"dept": "HR", "region": "US", "sales": 150},
            ]
        )

        cube_df = df.cube("dept", "region")
        result = cube_df.agg(F.sum("sales").alias("total_sales"))

        assert result.count() > 0
        # Cube should create 2^2 = 4 combinations plus grand total = 5 rows
        # But with actual data, we get more specific combinations
        assert all("total_sales" in row for row in result.collect())

    def test_cube_single_column(self):
        """Test cube with single column."""
        spark = SparkSession("test")
        df = spark.createDataFrame(
            [
                {"dept": "IT", "sales": 100},
                {"dept": "HR", "sales": 200},
            ]
        )

        cube_df = df.cube("dept")
        result = cube_df.agg(F.count("*").alias("count"))

        assert result.count() >= 2  # At least one group + grand total

    def test_cube_grand_total(self):
        """Test cube includes grand total (all nulls)."""
        spark = SparkSession("test")
        df = spark.createDataFrame(
            [
                {"dept": "IT", "region": "US", "sales": 100},
            ]
        )

        cube_df = df.cube("dept", "region")
        result = cube_df.agg(F.sum("sales").alias("total"))

        # Should have grand total row with nulls
        rows = result.collect()
        grand_total = [r for r in rows if r.dept is None and r.region is None]
        assert len(grand_total) > 0


@pytest.mark.unit
class TestPivotOperations:
    """Test PivotGroupedData operations."""

    def test_pivot_basic(self):
        """Test basic pivot operation."""
        spark = SparkSession("test")
        df = spark.createDataFrame(
            [
                {"dept": "IT", "quarter": "Q1", "sales": 100},
                {"dept": "IT", "quarter": "Q2", "sales": 200},
                {"dept": "HR", "quarter": "Q1", "sales": 150},
            ]
        )

        pivot_df = df.groupBy("dept").pivot("quarter", ["Q1", "Q2"])
        result = pivot_df.agg(F.sum("sales").alias("total_sales"))

        assert result.count() > 0
        rows = result.collect()
        # Should have pivot columns
        assert any(
            hasattr(row, "total_sales_Q1") or "total_sales_Q1" in row for row in rows
        )

    def test_pivot_with_multiple_aggregations(self):
        """Test pivot with multiple aggregations."""
        spark = SparkSession("test")
        df = spark.createDataFrame(
            [
                {"dept": "IT", "quarter": "Q1", "sales": 100},
                {"dept": "IT", "quarter": "Q1", "sales": 50},
            ]
        )

        pivot_df = df.groupBy("dept").pivot("quarter", ["Q1"])
        result = pivot_df.agg(
            F.sum("sales").alias("sum_sales"), F.count("*").alias("count")
        )

        assert result.count() > 0


@pytest.mark.unit
class TestRollupOperations:
    """Test RollupGroupedData operations."""

    def test_rollup_basic(self):
        """Test basic rollup operation."""
        spark = SparkSession("test")
        df = spark.createDataFrame(
            [
                {"year": 2023, "month": 1, "sales": 100},
                {"year": 2023, "month": 2, "sales": 200},
                {"year": 2024, "month": 1, "sales": 150},
            ]
        )

        rollup_df = df.rollup("year", "month")
        result = rollup_df.agg(F.sum("sales").alias("total_sales"))

        assert result.count() > 0
        # Rollup creates hierarchical groupings
        rows = result.collect()
        assert all("total_sales" in row for row in rows)

    def test_rollup_hierarchical(self):
        """Test rollup creates hierarchical groupings."""
        spark = SparkSession("test")
        df = spark.createDataFrame(
            [
                {"year": 2023, "month": 1, "day": 1, "sales": 100},
                {"year": 2023, "month": 1, "day": 2, "sales": 200},
            ]
        )

        rollup_df = df.rollup("year", "month", "day")
        result = rollup_df.agg(F.sum("sales").alias("total"))

        # Should have groupings at different levels
        assert result.count() >= 2

    def test_rollup_grand_total(self):
        """Test rollup includes grand total."""
        spark = SparkSession("test")
        df = spark.createDataFrame(
            [
                {"year": 2023, "sales": 100},
            ]
        )

        rollup_df = df.rollup("year")
        result = rollup_df.agg(F.sum("sales").alias("total"))

        rows = result.collect()
        # Should have grand total with null year
        grand_total = [r for r in rows if r.year is None]
        assert len(grand_total) > 0
