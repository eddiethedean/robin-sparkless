"""Tests for issue #393: filter/where accept string SQL expression."""

from __future__ import annotations

import robin_sparkless as rs


def test_filter_string_sql() -> None:
    """filter with SQL expression string (e.g. 'age > 18') filters rows."""
    spark = rs.SparkSession.builder().app_name("issue_393").get_or_create()
    df = spark.createDataFrame(
        [(1, 17, "a"), (2, 18, "b"), (3, 19, "c")],
        ["id", "age", "name"],
    )
    out = df.filter("age > 18")
    rows = out.collect()
    assert len(rows) == 1
    assert rows[0]["age"] == 19
    assert rows[0]["name"] == "c"


def test_where_string_sql() -> None:
    """where() is alias for filter(); accepts same SQL string."""
    spark = rs.SparkSession.builder().app_name("issue_393").get_or_create()
    df = spark.createDataFrame(
        [(1, "x"), (2, "y"), (3, "x")],
        ["id", "label"],
    )
    out = df.where("label = 'x'")
    rows = out.collect()
    assert len(rows) == 2
    assert {r["id"] for r in rows} == {1, 3}


def test_filter_column_unchanged() -> None:
    """filter with Column still works (e.g. col('age') > 18)."""
    spark = rs.SparkSession.builder().app_name("issue_393").get_or_create()
    df = spark.createDataFrame([(1, 10), (2, 20)], ["a", "b"])
    out = df.filter(rs.col("b") > 15)
    rows = out.collect()
    assert len(rows) == 1
    assert rows[0]["b"] == 20
