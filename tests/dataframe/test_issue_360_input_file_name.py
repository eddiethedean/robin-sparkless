"""
Tests for issue #360: input_file_name() function.

PySpark returns the path of the file being read. Sparkless documents a JVM stub
that returns empty string for all rows (see docs/PYSPARK_DIFFERENCES.md).
"""

from __future__ import annotations

import pytest

from sparkless.testing import get_imports

pytestmark = pytest.mark.sparkless_only


class TestIssue360InputFileNameStub:
    """Sparkless JVM stub: input_file_name() always returns empty string."""

    def test_input_file_name_returns_documented_stub_value(self, spark):
        """Documented stub returns '' for every row."""
        F = get_imports().F
        df = spark.createDataFrame(
            [
                {"dataset": "dataset_a", "table": "table_1"},
                {"dataset": "dataset_b", "table": "table_2"},
            ]
        )
        result = df.withColumn("InputFileName", F.input_file_name())
        rows = result.collect()
        assert len(rows) == 2
        assert rows[0]["InputFileName"] == ""
        assert rows[1]["InputFileName"] == ""

    def test_input_file_name_exact_issue_scenario(self, spark):
        """Exact scenario from issue #360 preserves other columns; stub path is ''."""
        F = get_imports().F
        df = spark.createDataFrame(
            [
                ("dataset_a", "table_1"),
                ("dataset_b", "table_2"),
            ],
            ["dataset", "table"],
        )
        df = df.withColumn("InputFileName", F.input_file_name())
        rows = df.collect()
        assert len(rows) == 2
        assert rows[0]["dataset"] == "dataset_a"
        assert rows[0]["table"] == "table_1"
        assert rows[0]["InputFileName"] == ""
        assert rows[1]["InputFileName"] == ""

    def test_input_file_name_select_only(self, spark):
        """Select only input_file_name() as single column."""
        F = get_imports().F
        df = spark.createDataFrame([{"a": 1}, {"a": 2}])
        result = df.select(F.input_file_name().alias("path"))
        rows = result.collect()
        assert len(rows) == 2
        assert rows[0]["path"] == ""
        assert rows[1]["path"] == ""


class TestIssue360InputFileNameRobust:
    """Edge cases for documented input_file_name() stub."""

    def test_input_file_name_empty_dataframe(self, spark):
        """input_file_name() on empty DataFrame returns empty result."""
        F = get_imports().F
        df = spark.createDataFrame([], "a int")
        result = df.withColumn("path", F.input_file_name())
        rows = result.collect()
        assert len(rows) == 0

    def test_input_file_name_single_row(self, spark):
        """input_file_name() with single row returns stub empty string."""
        F = get_imports().F
        df = spark.createDataFrame([{"id": 1}])
        result = df.withColumn("file", F.input_file_name())
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]["file"] == ""

    def test_input_file_name_after_filter(self, spark):
        """input_file_name() after filter still returns stub empty string."""
        F = get_imports().F
        df = spark.createDataFrame([{"a": 1}, {"a": 2}, {"a": 3}])
        result = df.filter(F.col("a") > 1).withColumn("path", F.input_file_name())
        rows = result.collect()
        assert len(rows) == 2
        assert all(r["path"] == "" for r in rows)

    def test_input_file_name_after_select(self, spark):
        """input_file_name() after select returns stub empty string."""
        F = get_imports().F
        df = spark.createDataFrame([{"x": 1, "y": 2}, {"x": 3, "y": 4}])
        result = df.select("x").withColumn("path", F.input_file_name())
        rows = result.collect()
        assert len(rows) == 2
        assert rows[0]["path"] == ""
        assert rows[1]["path"] == ""

    def test_input_file_name_preserves_schema(self, spark):
        """withColumn(input_file_name()) adds string column; schema preserved."""
        F = get_imports().F
        df = spark.createDataFrame([{"a": 1, "b": "x"}])
        result = df.withColumn("path", F.input_file_name())
        assert "path" in result.schema.fieldNames()
        assert len(result.schema.fields) == 3
        rows = result.collect()
        assert rows[0]["a"] == 1 and rows[0]["b"] == "x" and rows[0]["path"] == ""

    def test_input_file_name_with_show(self, spark):
        """withColumn(input_file_name()).show() does not raise."""
        F = get_imports().F
        df = spark.createDataFrame([{"a": 1}])
        result = df.withColumn("path", F.input_file_name())
        result.show()

    def test_input_file_name_all_rows_same_stub(self, spark):
        """All rows get the documented stub value (empty string, never None)."""
        F = get_imports().F
        df = spark.createDataFrame([{"i": i} for i in range(5)])
        result = df.withColumn("path", F.input_file_name())
        rows = result.collect()
        for r in rows:
            assert r["path"] == ""

    def test_input_file_name_multiple_columns(self, spark):
        """input_file_name() alongside other columns in select."""
        F = get_imports().F
        df = spark.createDataFrame([{"a": 1, "b": 2}, {"a": 3, "b": 4}])
        result = df.select(
            F.col("a"),
            F.input_file_name().alias("path"),
            F.col("b"),
        )
        rows = result.collect()
        assert len(rows) == 2
        assert rows[0]["a"] == 1 and rows[0]["b"] == 2 and rows[0]["path"] == ""
        assert rows[1]["path"] == ""

    def test_input_file_name_alias(self, spark):
        """input_file_name() with alias returns stub empty string."""
        F = get_imports().F
        df = spark.createDataFrame([{"x": 1}])
        result = df.select(F.input_file_name().alias("source_file"))
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]["source_file"] == ""
