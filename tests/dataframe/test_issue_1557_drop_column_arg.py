"""Tests for issue #1557: DataFrame.drop() accepts Column arguments (PySpark parity)."""

from __future__ import annotations

from sparkless.testing import get_imports

_imports = get_imports()
SparkSession = _imports.SparkSession
F = _imports.F


def test_drop_column_after_join_on_expression() -> None:
    """joined.drop(right.A) after join on (left.A == right.A) — coalesced key is a no-op."""
    spark = SparkSession.builder.appName("issue_1557").get_or_create()
    left = spark.createDataFrame([("1", "Alice")], ["A", "name"])
    right = spark.createDataFrame([("1", "active")], ["A", "B"])
    joined = left.join(right, on=(left.A == right.A), how="inner")

    out = joined.drop(right.A)
    rows = out.collect()
    assert out.columns == ["A", "name", "B"]
    assert len(rows) == 1
    assert rows[0]["A"] == "1"
    assert rows[0]["name"] == "Alice"
    assert rows[0]["B"] == "active"


def test_drop_string_still_drops_column() -> None:
    spark = SparkSession.builder.appName("issue_1557_str").get_or_create()
    df = spark.createDataFrame([("1", "Alice", "x")], ["A", "name", "extra"])
    out = df.drop("extra")
    assert out.columns == ["A", "name"]


def test_drop_list_of_columns_mixed() -> None:
    spark = SparkSession.builder.appName("issue_1557_list").get_or_create()
    df = spark.createDataFrame(
        [("1", "Alice", "x", "y")], ["A", "name", "extra", "tail"]
    )
    out = df.drop(["extra", F.col("tail")])
    assert out.columns == ["A", "name"]
