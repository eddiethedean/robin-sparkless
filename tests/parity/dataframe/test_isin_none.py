"""
Tests for isin() with None in the list.

Issue #1483: isin() with None in list caused type coercion error instead of
returning NULL for uncertain matches like PySpark.

PySpark behavior when isin() list contains None:
- Rows matching non-null values return true
- Rows not matching any non-null value return NULL (uncertain due to None)
- Rows where column value is NULL return NULL

These tests work with both sparkless and PySpark backends.
Set SPARKLESS_TEST_MODE=pyspark to run with real PySpark.
"""

from sparkless.testing import get_imports

imports = get_imports()
F = imports.F


class TestIsinNone:
    """Test isin() handles None in list like PySpark."""

    def test_isin_with_none_in_list(self, spark):
        """Test isin() with None in list - exact reproduction from issue #1483."""
        df = spark.createDataFrame([{"val": 1}, {"val": None}, {"val": 3}])
        result = df.select(F.col("val"), F.col("val").isin([1, 2, None]).alias("in_list"))
        rows = result.collect()

        assert len(rows) == 3

        # val=1 matches, should be true
        row_1 = next((r for r in rows if r["val"] == 1), None)
        assert row_1 is not None
        assert row_1["in_list"] is True

        # val=NULL should be NULL
        row_null = next((r for r in rows if r["val"] is None), None)
        assert row_null is not None
        assert row_null["in_list"] is None

        # val=3 doesn't match any non-null value, but None is in list -> NULL
        row_3 = next((r for r in rows if r["val"] == 3), None)
        assert row_3 is not None
        assert row_3["in_list"] is None

    def test_isin_without_none(self, spark):
        """Test isin() without None still works correctly."""
        df = spark.createDataFrame([{"val": 1}, {"val": None}, {"val": 3}])
        result = df.select(F.col("val"), F.col("val").isin([1, 2]).alias("in_list"))
        rows = result.collect()

        assert len(rows) == 3

        # val=1 matches, should be true
        row_1 = next((r for r in rows if r["val"] == 1), None)
        assert row_1["in_list"] is True

        # val=NULL should be NULL (comparing NULL with anything)
        row_null = next((r for r in rows if r["val"] is None), None)
        assert row_null["in_list"] is None

        # val=3 doesn't match, should be false (no None in list)
        row_3 = next((r for r in rows if r["val"] == 3), None)
        assert row_3["in_list"] is False

    def test_isin_only_none(self, spark):
        """Test isin() with only None in list."""
        df = spark.createDataFrame([{"val": 1}, {"val": None}, {"val": 3}])
        result = df.select(F.col("val"), F.col("val").isin([None]).alias("in_list"))
        rows = result.collect()

        assert len(rows) == 3

        # All results should be NULL (only None in list = all uncertain)
        for row in rows:
            assert row["in_list"] is None

    def test_isin_with_none_and_match(self, spark):
        """Test isin() where value matches a non-null value."""
        df = spark.createDataFrame([{"val": 1}, {"val": 2}, {"val": 3}])
        result = df.select(F.col("val"), F.col("val").isin([1, 2, None]).alias("in_list"))
        rows = result.collect()

        assert len(rows) == 3

        # val=1 and val=2 should be true (match non-null values)
        row_1 = next((r for r in rows if r["val"] == 1), None)
        assert row_1["in_list"] is True

        row_2 = next((r for r in rows if r["val"] == 2), None)
        assert row_2["in_list"] is True

        # val=3 doesn't match any non-null value, None in list -> NULL
        row_3 = next((r for r in rows if r["val"] == 3), None)
        assert row_3["in_list"] is None

    def test_isin_empty_list(self, spark):
        """Test isin() with empty list still returns false."""
        df = spark.createDataFrame([{"val": 1}, {"val": 2}])
        result = df.select(F.col("val"), F.col("val").isin([]).alias("in_list"))
        rows = result.collect()

        assert len(rows) == 2
        for row in rows:
            assert row["in_list"] is False

    def test_isin_with_none_strings(self, spark):
        """Test isin() with None in string list."""
        df = spark.createDataFrame([{"val": "a"}, {"val": None}, {"val": "c"}])
        result = df.select(F.col("val"), F.col("val").isin(["a", "b", None]).alias("in_list"))
        rows = result.collect()

        assert len(rows) == 3

        # val="a" matches, should be true
        row_a = next((r for r in rows if r["val"] == "a"), None)
        assert row_a["in_list"] is True

        # val=NULL should be NULL
        row_null = next((r for r in rows if r["val"] is None), None)
        assert row_null["in_list"] is None

        # val="c" doesn't match, None in list -> NULL
        row_c = next((r for r in rows if r["val"] == "c"), None)
        assert row_c["in_list"] is None

    def test_isin_variadic_with_none(self, spark):
        """Test isin() with variadic args containing None."""
        df = spark.createDataFrame([{"val": 1}, {"val": 2}, {"val": 3}])
        result = df.select(F.col("val"), F.col("val").isin(1, None).alias("in_list"))
        rows = result.collect()

        # val=1 should be true
        row_1 = next((r for r in rows if r["val"] == 1), None)
        assert row_1["in_list"] is True

        # val=2 and val=3 don't match, None in args -> NULL
        row_2 = next((r for r in rows if r["val"] == 2), None)
        assert row_2["in_list"] is None

        row_3 = next((r for r in rows if r["val"] == 3), None)
        assert row_3["in_list"] is None

    def test_isin_filter_with_none(self, spark):
        """Test filter with isin() containing None."""
        df = spark.createDataFrame([{"val": 1}, {"val": 2}, {"val": 3}])
        # Filter where isin returns true (val in [1, 2])
        result = df.filter(F.col("val").isin([1, 2, None]))
        rows = result.collect()

        # Only val=1 and val=2 match with true, val=3 is NULL (not true)
        # Filter keeps only true rows
        assert len(rows) == 2
        vals = {r["val"] for r in rows}
        assert vals == {1, 2}
