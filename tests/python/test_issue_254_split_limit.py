"""
Tests for issue #254: F.split(column, pattern, limit) — limit parameter not supported.

PySpark supports F.split(col, pattern, limit) with an optional third argument to limit
the number of splits. Robin now accepts the same signature; e.g. split(col("s"), ",", 2)
on "a,b,c" yields ['a', 'b,c'].
"""

from __future__ import annotations

import robin_sparkless as rs


def test_split_with_limit_two_parts() -> None:
    """F.split(col('s'), ',', 2) on 'a,b,c' yields ['a', 'b,c']."""
    spark = rs.SparkSession.builder().app_name("split_limit").get_or_create()
    df = spark.createDataFrame([{"s": "a,b,c"}], [("s", "string")])
    out = df.select(rs.split(rs.col("s"), ",", 2)).collect()
    assert len(out) == 1
    row = out[0]
    # Use items() for Row/dict compatibility
    parts = [v for _, v in row.items() if isinstance(v, list)]
    assert len(parts) == 1
    assert parts[0] == ["a", "b,c"]


def test_split_without_limit_unchanged() -> None:
    """F.split(col('s'), ',') without limit yields all parts."""
    spark = rs.SparkSession.builder().app_name("split_limit").get_or_create()
    df = spark.createDataFrame([{"s": "a,b,c"}], [("s", "string")])
    out = df.select(rs.split(rs.col("s"), ",")).collect()
    assert len(out) == 1
    parts = [v for _, v in out[0].items() if isinstance(v, list)]
    assert len(parts) == 1
    assert parts[0] == ["a", "b", "c"]


def test_column_split_with_limit() -> None:
    """col('s').split(',', 2) yields ['a', 'b,c'] for 'a,b,c'."""
    spark = rs.SparkSession.builder().app_name("split_limit").get_or_create()
    df = spark.createDataFrame([{"s": "a,b,c"}], [("s", "string")])
    out = df.select(rs.col("s").split(",", 2)).collect()
    assert len(out) == 1
    parts = [v for _, v in out[0].items() if isinstance(v, list)]
    assert len(parts) == 1
    assert parts[0] == ["a", "b,c"]
