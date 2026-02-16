"""Tests for issue #385: union() accept DataFrame-like or wrapped type."""

from __future__ import annotations

import robin_sparkless as rs


def test_union_accepts_dataframe() -> None:
    """union(other) with a plain DataFrame works."""
    spark = rs.SparkSession.builder().app_name("issue_385").get_or_create()
    a = spark.createDataFrame([(1, "x")], ["id", "label"])
    b = spark.createDataFrame([(2, "y")], ["id", "label"])
    out = a.union(b)
    rows = out.collect()
    assert len(rows) == 2
    assert rows[0]["id"] == 1 and rows[0]["label"] == "x"
    assert rows[1]["id"] == 2 and rows[1]["label"] == "y"


def test_union_accepts_wrapper_with_inner() -> None:
    """union(other) accepts an object with .inner that is a DataFrame (DataFrame-like)."""
    spark = rs.SparkSession.builder().app_name("issue_385").get_or_create()
    a = spark.createDataFrame([(1,)], ["x"])
    b = spark.createDataFrame([(2,)], ["x"])

    class Wrapper:
        def __init__(self, df: rs.DataFrame) -> None:
            self.inner = df

    wrapped = Wrapper(b)
    out = a.union(wrapped)
    rows = out.collect()
    assert len(rows) == 2
    assert rows[0]["x"] == 1 and rows[1]["x"] == 2


def test_union_all_accepts_dataframe_like() -> None:
    """unionAll(other) accepts DataFrame-like with .inner."""
    spark = rs.SparkSession.builder().app_name("issue_385").get_or_create()
    a = spark.createDataFrame([(1,)], ["v"])
    b = spark.createDataFrame([(2,)], ["v"])
    wrapped = type("W", (), {"inner": b})()
    out = a.unionAll(wrapped)
    assert len(out.collect()) == 2
