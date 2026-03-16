"""
Tests for collect_set() aggregate function.

Note: PySpark's collect_set() does NOT guarantee any specific order.
The order is hash-based and non-deterministic. Tests should compare
sets for equality, not ordered lists.

Issue #1485 incorrectly claimed PySpark returns sorted values.
These tests validate correct set behavior (unique values, no duplicates).

These tests work with both sparkless and PySpark backends.
Set SPARKLESS_TEST_MODE=pyspark to run with real PySpark.
"""

from sparkless.testing import get_imports

imports = get_imports()
F = imports.F


class TestCollectSet:
    """Test collect_set() returns unique values (order not guaranteed)."""

    def test_collect_set_integer_unique(self, spark):
        """Test collect_set() returns unique integers."""
        df = spark.createDataFrame(
            [
                {"grp": "A", "val": 3},
                {"grp": "A", "val": 1},
                {"grp": "A", "val": 2},
            ]
        )
        result = df.groupBy("grp").agg(F.collect_set("val").alias("set"))
        rows = result.collect()

        assert len(rows) == 1
        assert rows[0]["grp"] == "A"
        assert set(rows[0]["set"]) == {1, 2, 3}

    def test_collect_set_string_unique(self, spark):
        """Test collect_set() returns unique strings."""
        df = spark.createDataFrame(
            [
                {"grp": "A", "val": "cherry"},
                {"grp": "A", "val": "apple"},
                {"grp": "A", "val": "banana"},
            ]
        )
        result = df.groupBy("grp").agg(F.collect_set("val").alias("set"))
        rows = result.collect()

        assert len(rows) == 1
        assert rows[0]["grp"] == "A"
        assert set(rows[0]["set"]) == {"apple", "banana", "cherry"}

    def test_collect_set_removes_duplicates(self, spark):
        """Test collect_set() removes duplicates and returns unique values."""
        df = spark.createDataFrame(
            [
                {"grp": "A", "val": 5},
                {"grp": "A", "val": 2},
                {"grp": "A", "val": 5},
                {"grp": "A", "val": 1},
                {"grp": "A", "val": 2},
            ]
        )
        result = df.groupBy("grp").agg(F.collect_set("val").alias("set"))
        rows = result.collect()

        assert len(rows) == 1
        assert set(rows[0]["set"]) == {1, 2, 5}
        assert len(rows[0]["set"]) == 3

    def test_collect_set_multiple_groups(self, spark):
        """Test collect_set() returns unique values for multiple groups."""
        df = spark.createDataFrame(
            [
                {"grp": "A", "val": 3},
                {"grp": "A", "val": 1},
                {"grp": "B", "val": 20},
                {"grp": "B", "val": 10},
                {"grp": "A", "val": 2},
                {"grp": "B", "val": 30},
            ]
        )
        result = df.groupBy("grp").agg(F.collect_set("val").alias("set"))
        rows = result.collect()

        assert len(rows) == 2
        row_a = next((r for r in rows if r["grp"] == "A"), None)
        row_b = next((r for r in rows if r["grp"] == "B"), None)

        assert row_a is not None
        assert row_b is not None
        assert set(row_a["set"]) == {1, 2, 3}
        assert set(row_b["set"]) == {10, 20, 30}

    def test_collect_set_float_unique(self, spark):
        """Test collect_set() returns unique floats."""
        df = spark.createDataFrame(
            [
                {"grp": "A", "val": 3.14},
                {"grp": "A", "val": 1.41},
                {"grp": "A", "val": 2.71},
            ]
        )
        result = df.groupBy("grp").agg(F.collect_set("val").alias("set"))
        rows = result.collect()

        assert len(rows) == 1
        assert set(rows[0]["set"]) == {1.41, 2.71, 3.14}

    def test_collect_set_single_value(self, spark):
        """Test collect_set() with single value per group."""
        df = spark.createDataFrame(
            [
                {"grp": "A", "val": 42},
            ]
        )
        result = df.groupBy("grp").agg(F.collect_set("val").alias("set"))
        rows = result.collect()

        assert len(rows) == 1
        assert set(rows[0]["set"]) == {42}

    def test_collect_set_negative_numbers(self, spark):
        """Test collect_set() with negative numbers."""
        df = spark.createDataFrame(
            [
                {"grp": "A", "val": 5},
                {"grp": "A", "val": -3},
                {"grp": "A", "val": 0},
                {"grp": "A", "val": -10},
                {"grp": "A", "val": 2},
            ]
        )
        result = df.groupBy("grp").agg(F.collect_set("val").alias("set"))
        rows = result.collect()

        assert len(rows) == 1
        assert set(rows[0]["set"]) == {-10, -3, 0, 2, 5}

    def test_collect_set_with_col_function(self, spark):
        """Test collect_set() with F.col() reference."""
        df = spark.createDataFrame(
            [
                {"grp": "A", "val": 3},
                {"grp": "A", "val": 1},
                {"grp": "A", "val": 2},
            ]
        )
        result = df.groupBy("grp").agg(F.collect_set(F.col("val")).alias("set"))
        rows = result.collect()

        assert len(rows) == 1
        assert set(rows[0]["set"]) == {1, 2, 3}

    def test_collect_set_empty_after_filter(self, spark):
        """Test collect_set() behavior with empty groups after filter."""
        df = spark.createDataFrame(
            [
                {"grp": "A", "val": 1},
                {"grp": "A", "val": 2},
            ]
        )
        filtered = df.filter(F.col("val") > 100)
        result = filtered.groupBy("grp").agg(F.collect_set("val").alias("set"))
        rows = result.collect()

        assert len(rows) == 0

    def test_collect_set_without_groupby(self, spark):
        """Test collect_set() without groupBy (global aggregation)."""
        df = spark.createDataFrame(
            [
                {"val": 3},
                {"val": 1},
                {"val": 2},
                {"val": 1},
            ]
        )
        result = df.agg(F.collect_set("val").alias("set"))
        rows = result.collect()

        assert len(rows) == 1
        assert set(rows[0]["set"]) == {1, 2, 3}

    def test_collect_set_combined_with_other_aggs(self, spark):
        """Test collect_set() combined with other aggregation functions."""
        df = spark.createDataFrame(
            [
                {"grp": "A", "val": 3},
                {"grp": "A", "val": 1},
                {"grp": "A", "val": 2},
            ]
        )
        result = df.groupBy("grp").agg(
            F.collect_set("val").alias("set"),
            F.sum("val").alias("total"),
            F.count("val").alias("cnt"),
        )
        rows = result.collect()

        assert len(rows) == 1
        assert set(rows[0]["set"]) == {1, 2, 3}
        assert rows[0]["total"] == 6
        assert rows[0]["cnt"] == 3

    def test_collect_set_vs_collect_list(self, spark):
        """Test collect_set() returns unique vs collect_list() preserves duplicates."""
        df = spark.createDataFrame(
            [
                {"grp": "A", "val": 3},
                {"grp": "A", "val": 1},
                {"grp": "A", "val": 3},
                {"grp": "A", "val": 2},
            ]
        )
        result = df.groupBy("grp").agg(
            F.collect_set("val").alias("unique_set"),
            F.collect_list("val").alias("all_list"),
        )
        rows = result.collect()

        assert len(rows) == 1
        assert set(rows[0]["unique_set"]) == {1, 2, 3}
        assert len(rows[0]["unique_set"]) == 3
        assert len(rows[0]["all_list"]) == 4
        assert set(rows[0]["all_list"]) == {1, 2, 3}

    def test_collect_set_large_dataset(self, spark):
        """Test collect_set() with larger dataset returns correct unique values."""
        data = [{"grp": "A", "val": i} for i in range(100, 0, -1)]
        df = spark.createDataFrame(data)
        result = df.groupBy("grp").agg(F.collect_set("val").alias("set"))
        rows = result.collect()

        assert len(rows) == 1
        expected = set(range(1, 101))
        assert set(rows[0]["set"]) == expected
        assert len(rows[0]["set"]) == 100
