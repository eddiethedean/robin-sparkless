"""Regression test for issue #1647: Column.endswith()."""

from __future__ import annotations

from sparkless.testing import get_imports

F = get_imports().F


def _row_val(row, key):
    if hasattr(row, "asDict"):
        return row.asDict().get(key)
    return row[key]


class TestIssue1647ColumnEndswith:
    """PySpark Column.endswith() filters rows by string suffix."""

    def test_filter_col_endswith_issue_example(self, spark) -> None:
        """Exact scenario from issue #1647."""
        df = spark.createDataFrame([("hello_world",), ("test_case",)], ["col1"])
        result = df.filter(F.col("col1").endswith("world"))
        rows = result.collect()
        assert len(rows) == 1
        assert _row_val(rows[0], "col1") == "hello_world"

    def test_endswith_no_match(self, spark) -> None:
        df = spark.createDataFrame([("abc",), ("xyz",)], ["col1"])
        assert df.filter(F.col("col1").endswith("world")).count() == 0

    def test_endswith_and_startswith_together(self, spark) -> None:
        df = spark.createDataFrame([("hello_world",), ("hello_test",)], ["col1"])
        result = df.filter(
            F.col("col1").startswith("hello") & F.col("col1").endswith("world")
        )
        rows = result.collect()
        assert len(rows) == 1
        assert _row_val(rows[0], "col1") == "hello_world"
