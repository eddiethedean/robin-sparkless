"""Tests for issue #384: DataFrame describe, show, and explain(mode) API."""

from __future__ import annotations

import robin_sparkless as rs


def test_describe_returns_dataframe() -> None:
    """describe() returns a DataFrame with summary stats (count, mean, stddev, min, max)."""
    spark = rs.SparkSession.builder().app_name("issue_384").get_or_create()
    df = spark.createDataFrame([(1, 10.0), (2, 20.0), (3, 30.0)], ["a", "b"])
    desc = df.describe()
    assert desc is not None
    rows = desc.collect()
    assert len(rows) >= 1
    # First column is summary (stat name)
    names = [r for r in rows[0].keys()]
    assert "summary" in names or any("a" in k or "b" in k for k in names)


def test_summary_alias_for_describe() -> None:
    """summary() is alias for describe(); returns same shape."""
    spark = rs.SparkSession.builder().app_name("issue_384").get_or_create()
    df = spark.createDataFrame([(1, "x"), (2, "y")], ["id", "label"])
    desc = df.describe()
    summ = df.summary()
    assert len(desc.collect()) == len(summ.collect())


def test_show_no_error() -> None:
    """show() and show(n) run without error."""
    spark = rs.SparkSession.builder().app_name("issue_384").get_or_create()
    df = spark.createDataFrame([(1,), (2,)], ["x"])
    df.show()
    df.show(1)


def test_explain_accepts_mode() -> None:
    """explain() and explain(mode=...) return a string (mode accepted for API parity)."""
    spark = rs.SparkSession.builder().app_name("issue_384").get_or_create()
    df = spark.createDataFrame([(1,)], ["x"])
    s = df.explain()
    assert isinstance(s, str)
    assert len(s) > 0
    s2 = df.explain(mode="extended")
    assert isinstance(s2, str)
