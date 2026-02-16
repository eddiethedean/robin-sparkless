"""Tests for #381: DataFrame.agg() multiple expressions global (PySpark parity)."""

from __future__ import annotations

import robin_sparkless as rs
from robin_sparkless import col, sum as rs_sum, avg, min as rs_min, max as rs_max


def _spark():
    return rs.SparkSession.builder().app_name("issue_381").get_or_create()


def test_agg_multiple_positional_args() -> None:
    """df.agg(sum(col("a")), avg(col("b"))) accepts multiple expressions as *args."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"a": 1, "b": 10}, {"a": 2, "b": 20}, {"a": 3, "b": 30}],
        schema=[("a", "int"), ("b", "int")],
    )
    out = df.agg(rs_sum(col("a")), avg(col("b")))
    rows = out.collect()
    assert len(rows) == 1
    row = rows[0]
    # sum(a)=6, avg(b)=20.0
    vals = [v for v in row.values() if isinstance(v, (int, float))]
    assert 6 in vals
    assert 20.0 in vals


def test_agg_single_expr_unchanged() -> None:
    """df.agg(expr) still works."""
    spark = _spark()
    df = spark.createDataFrame([{"x": 1}, {"x": 2}], schema=[("x", "int")])
    out = df.agg(rs_sum(col("x")))
    rows = out.collect()
    assert len(rows) == 1
    assert list(rows[0].values()) == [3]


def test_agg_list_unchanged() -> None:
    """df.agg([expr1, expr2]) still works."""
    spark = _spark()
    df = spark.createDataFrame([{"a": 1, "b": 10}], schema=[("a", "int"), ("b", "int")])
    out = df.agg([rs_min(col("a")), rs_max(col("b"))])
    rows = out.collect()
    assert len(rows) == 1
    vals = list(rows[0].values())
    assert 1 in vals and 10 in vals
