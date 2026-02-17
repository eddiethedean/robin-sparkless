"""Tests for issue #400: Window.orderBy accept F.desc F.asc."""

from __future__ import annotations

import robin_sparkless as rs


def test_window_order_by_desc_column() -> None:
    """Window.orderBy(desc(col)) - PySpark F.desc(col('v'))."""
    spark = rs.SparkSession.builder().app_name("issue_400").get_or_create()
    df = spark.createDataFrame(
        [("a", 10), ("a", 20), ("b", 5)],
        ["k", "v"],
    )
    w = rs.Window.partitionBy("k").orderBy(rs.desc(rs.col("v")))
    result = df.select("k", "v", rs.row_number().over(w).alias("rn")).collect()
    # Partition a: v 20 gets rn=1 (desc), v 10 gets rn=2. Check by (k,v).
    by_kv = {(r["k"], r["v"]): r["rn"] for r in result}
    assert by_kv[("a", 20)] == 1
    assert by_kv[("a", 10)] == 2
    assert by_kv[("b", 5)] == 1


def test_window_order_by_desc_string() -> None:
    """Window.orderBy(desc("v")) - PySpark F.desc("v")."""
    spark = rs.SparkSession.builder().app_name("issue_400").get_or_create()
    df = spark.createDataFrame(
        [("a", 10), ("a", 20)],
        ["k", "v"],
    )
    w = rs.Window.partitionBy("k").orderBy(rs.desc("v"))
    result = df.select("k", "v", rs.row_number().over(w).alias("rn")).collect()
    by_kv = {(r["k"], r["v"]): r["rn"] for r in result}
    assert by_kv[("a", 20)] == 1
    assert by_kv[("a", 10)] == 2


def test_window_order_by_asc() -> None:
    """Window.orderBy(asc("v")) - ascending."""
    spark = rs.SparkSession.builder().app_name("issue_400").get_or_create()
    df = spark.createDataFrame(
        [("a", 20), ("a", 10)],
        ["k", "v"],
    )
    w = rs.Window.partitionBy("k").orderBy(rs.asc("v"))
    result = df.select("k", "v", rs.row_number().over(w).alias("rn")).collect()
    by_kv = {(r["k"], r["v"]): r["rn"] for r in result}
    assert by_kv[("a", 10)] == 1
    assert by_kv[("a", 20)] == 2
