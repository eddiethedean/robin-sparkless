"""Tests for #405: Column pow (**) and bitwise NOT (~)."""

from __future__ import annotations

import robin_sparkless as rs


def _spark():
    return rs.SparkSession.builder().app_name("issue_405").get_or_create()


def test_pow_column_literal() -> None:
    """col("x") ** 2 and pow(col("x"), 2) give squared values."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"x": 3}, {"x": 5}],
        schema=[("x", "int")],
    )
    out = df.select((rs.col("x") ** 2).alias("sq")).collect()
    rows = list(out)
    assert len(rows) == 2
    assert rows[0]["sq"] == 9
    assert rows[1]["sq"] == 25

    out2 = df.select(rs.power(rs.col("x"), 2).alias("sq")).collect()
    rows2 = list(out2)
    assert rows2[0]["sq"] == 9
    assert rows2[1]["sq"] == 25


def test_bitwise_not() -> None:
    """~col("x") gives bitwise NOT for integer column."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"x": 0}, {"x": 1}, {"x": 5}],
        schema=[("x", "int")],
    )
    out = df.select((~rs.col("x")).alias("inv")).collect()
    rows = list(out)
    assert len(rows) == 3
    # In Python two's complement: ~0 = -1, ~1 = -2, ~5 = -6
    assert rows[0]["inv"] == -1
    assert rows[1]["inv"] == -2
    assert rows[2]["inv"] == -6
