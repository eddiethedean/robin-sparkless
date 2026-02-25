"""Tests for issue #261: Column.between (range check).

Issue #261 reports that sparkless Column class does not implement the `between` method,
which is used to check a column for values within a certain range. The PySpark API supports this.

These tests verify that:
- Column.between exists on the public API.
- Its semantics match PySpark's between behavior:
  * between is inclusive on both ends: lower <= value <= upper
  * Returns True when value equals lower or upper bound
  * Returns None (NULL) when the column value is None
  * Works with numeric, string, and date types
"""

from datetime import date

import pytest

from tests.fixtures.spark_imports import get_spark_imports
from tests.fixtures.spark_backend import get_backend_type, BackendType


imports = get_spark_imports()
SparkSession = imports.SparkSession
F = imports.F
StructType = imports.StructType
StructField = imports.StructField
StringType = imports.StringType
IntegerType = imports.IntegerType
LongType = imports.LongType
DoubleType = imports.DoubleType
DateType = imports.DateType
TimestampType = imports.TimestampType


def _is_pyspark_mode() -> bool:
    """Check if running in PySpark backend mode."""
    backend = get_backend_type()
    return backend == BackendType.PYSPARK


class TestIssue261Between:
    """Regression tests for Column.between (issue #261)."""

    def test_between_example_from_issue_261(self) -> None:
        """Exact reproduction of the example from issue #261."""
        spark = SparkSession.builder.appName("Example").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"name": "Alice", "value": 1},
                    {"name": "Bob", "value": 10},
                ]
            )

            # Filter using between - should only return Alice (value=1 is between 0 and 5)
            result = df.where(F.col("value").between(0, 5))

            rows = result.collect()
            assert len(rows) == 1
            assert rows[0]["name"] == "Alice"
            assert rows[0]["value"] == 1
        finally:
            spark.stop()

    def test_between_inclusive_lower_bound(self) -> None:
        """Test that between is inclusive on the lower bound."""
        spark = SparkSession.builder.appName("TestBetween").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"value": 0},  # Should match (equals lower bound)
                    {"value": 1},
                    {"value": 5},  # Should match (equals upper bound)
                    {"value": 6},  # Should not match
                ]
            )

            result = df.filter(F.col("value").between(0, 5))

            rows = result.collect()
            assert len(rows) == 3
            values = [row["value"] for row in rows]
            assert 0 in values
            assert 1 in values
            assert 5 in values
            assert 6 not in values
        finally:
            spark.stop()

    def test_between_inclusive_upper_bound(self) -> None:
        """Test that between is inclusive on the upper bound."""
        spark = SparkSession.builder.appName("TestBetween").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"value": 4},
                    {"value": 5},  # Should match (equals upper bound)
                    {"value": 6},  # Should not match
                ]
            )

            result = df.filter(F.col("value").between(0, 5))

            rows = result.collect()
            assert len(rows) == 2
            values = [row["value"] for row in rows]
            assert 5 in values
            assert 6 not in values
        finally:
            spark.stop()

    def test_between_with_float_values(self) -> None:
        """Test between with floating point values."""
        spark = SparkSession.builder.appName("TestBetween").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"value": 0.5},
                    {"value": 2.5},
                    {"value": 4.5},
                    {"value": 5.0},  # Should match (equals upper bound)
                    {"value": 5.5},  # Should not match
                ]
            )

            result = df.filter(F.col("value").between(0.0, 5.0))

            rows = result.collect()
            assert len(rows) == 4
            values = [row["value"] for row in rows]
            assert 0.5 in values
            assert 2.5 in values
            assert 4.5 in values
            assert 5.0 in values
            assert 5.5 not in values
        finally:
            spark.stop()

    def test_between_with_null_values(self) -> None:
        """Test that between returns None (NULL) when column value is None."""
        spark = SparkSession.builder.appName("TestBetween").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"value": 1},
                    {"value": None},
                    {"value": 3},
                ]
            )

            # Filter with between - null values should be excluded (PySpark behavior)
            result = df.filter(F.col("value").between(0, 5))

            rows = result.collect()
            assert len(rows) == 2
            values = [row["value"] for row in rows]
            assert 1 in values
            assert 3 in values
            assert None not in values
        finally:
            spark.stop()

    def test_between_with_string_values(self) -> None:
        """Test between with string values (lexicographic comparison)."""
        spark = SparkSession.builder.appName("TestBetween").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"name": "Alice"},
                    {"name": "Bob"},
                    {"name": "Charlie"},
                    {"name": "David"},
                ]
            )

            # Filter names between "Bob" and "David" (inclusive)
            result = df.filter(F.col("name").between("Bob", "David"))

            rows = result.collect()
            assert len(rows) == 3
            names = [row["name"] for row in rows]
            assert "Bob" in names
            assert "Charlie" in names
            assert "David" in names
            assert "Alice" not in names
        finally:
            spark.stop()

    def test_between_with_literal_bounds(self) -> None:
        """Test between using F.lit() for bounds."""
        spark = SparkSession.builder.appName("TestBetween").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"value": 1},
                    {"value": 5},
                    {"value": 10},
                ]
            )

            # Use F.lit() for bounds
            result = df.filter(F.col("value").between(F.lit(0), F.lit(5)))

            rows = result.collect()
            assert len(rows) == 2
            values = [row["value"] for row in rows]
            assert 1 in values
            assert 5 in values
            assert 10 not in values
        finally:
            spark.stop()

    def test_between_in_select_expression(self) -> None:
        """Test between used in a select expression (not just filter)."""
        spark = SparkSession.builder.appName("TestBetween").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"value": 1},
                    {"value": 5},
                    {"value": 10},
                ]
            )

            # Use between in select to create a boolean column
            result = df.select(
                F.col("value"),
                F.col("value").between(0, 5).alias("in_range"),
            )

            rows = result.collect()
            assert len(rows) == 3

            # Check first row (value=1, should be in range)
            assert rows[0]["value"] == 1
            assert rows[0]["in_range"] is True

            # Check second row (value=5, should be in range - inclusive)
            assert rows[1]["value"] == 5
            assert rows[1]["in_range"] is True

            # Check third row (value=10, should not be in range)
            assert rows[2]["value"] == 10
            assert rows[2]["in_range"] is False
        finally:
            spark.stop()

    def test_between_with_per_row_column_bounds(self) -> None:
        """Test between where lower and upper bounds come from columns."""
        spark = SparkSession.builder.appName("TestBetweenColumnBounds").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"value": 5, "lower": 0, "upper": 10},  # in range
                    {"value": 0, "lower": 0, "upper": 10},  # in range (equals lower)
                    {"value": 10, "lower": 0, "upper": 10},  # in range (equals upper)
                    {"value": -1, "lower": 0, "upper": 10},  # out of range
                    {"value": 11, "lower": 0, "upper": 10},  # out of range
                ]
            )

            result = df.select(
                F.col("value"),
                F.col("lower"),
                F.col("upper"),
                F.col("value")
                .between(F.col("lower"), F.col("upper"))
                .alias("in_range"),
            )

            rows = result.collect()
            assert len(rows) == 5
            in_range_map = {
                (row["value"], row["lower"], row["upper"]): row["in_range"]
                for row in rows
            }
            assert in_range_map[(5, 0, 10)] is True
            assert in_range_map[(0, 0, 10)] is True
            assert in_range_map[(10, 0, 10)] is True
            assert in_range_map[(-1, 0, 10)] is False
            assert in_range_map[(11, 0, 10)] is False
        finally:
            spark.stop()

    def test_between_with_date_values(self) -> None:
        """Test between with date values (DateType columns)."""
        spark = SparkSession.builder.appName("TestBetweenDates").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"day": date(2024, 1, 1)},
                    {"day": date(2024, 1, 15)},
                    {"day": date(2024, 1, 31)},
                    {"day": date(2024, 2, 1)},
                ]
            )

            result = df.filter(
                F.col("day").between(date(2024, 1, 1), date(2024, 1, 31))
            )

            rows = result.collect()
            days = {row["day"] for row in rows}
            assert date(2024, 1, 1) in days
            assert date(2024, 1, 15) in days
            assert date(2024, 1, 31) in days
            assert date(2024, 2, 1) not in days
        finally:
            spark.stop()

    def test_between_with_reversed_bounds(self) -> None:
        """Test between with reversed bounds (lower > upper) matches PySpark semantics."""
        spark = SparkSession.builder.appName("TestBetweenReversed").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"value": -5},
                    {"value": 0},
                    {"value": 5},
                ]
            )

            # PySpark behavior: between(lower, upper) uses lower <= value <= upper.
            # When lower > upper, no rows satisfy the condition.
            result = df.filter(F.col("value").between(5, 0))
            rows = result.collect()
            assert rows == []
        finally:
            spark.stop()

    def test_between_in_when_otherwise_expression(self) -> None:
        """Test between inside a CASE WHEN style expression."""
        spark = SparkSession.builder.appName("TestBetweenWhen").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"value": 0},
                    {"value": 3},
                    {"value": 10},
                ]
            )

            result = df.select(
                F.col("value"),
                F.when(F.col("value").between(1, 5), F.lit("in-range"))
                .otherwise(F.lit("out-of-range"))
                .alias("bucket"),
            )

            rows = result.collect()
            bucket_map = {row["value"]: row["bucket"] for row in rows}
            assert bucket_map[0] == "out-of-range"
            assert bucket_map[3] == "in-range"
            assert bucket_map[10] == "out-of-range"
        finally:
            spark.stop()

    @pytest.mark.skipif(  # type: ignore[misc,untyped-decorator]
        not _is_pyspark_mode(),
        reason="PySpark parity test - only run with PySpark backend",
    )
    def test_between_pyspark_parity(self) -> None:
        """Parity test: verify between behavior matches PySpark."""
        spark = SparkSession.builder.appName("TestBetweenParity").getOrCreate()
        try:
            # Test data with edge cases
            df = spark.createDataFrame(
                [
                    {"value": 0},  # Lower bound
                    {"value": 1},
                    {"value": 5},  # Upper bound
                    {"value": 6},  # Outside range
                    {"value": None},  # NULL
                ]
            )

            # Filter with between
            result = df.filter(F.col("value").between(0, 5))

            rows = result.collect()

            # PySpark behavior: between is inclusive, NULL values are excluded
            assert len(rows) == 3
            values = [row["value"] for row in rows]
            assert 0 in values  # Lower bound included
            assert 1 in values
            assert 5 in values  # Upper bound included
            assert 6 not in values  # Outside range excluded
            # NULL values are excluded from filter results
        finally:
            spark.stop()
