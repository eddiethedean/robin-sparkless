"""Tests for issue #400: Window.orderBy accept F.desc F.asc."""

from __future__ import annotations

from tests.fixtures.spark_imports import get_spark_imports


_imports = get_spark_imports()
SparkSession = _imports.SparkSession
F = _imports.F
Window = _imports.Window


def test_window_order_by_desc_column() -> None:
    """Window.orderBy(desc(col)) - PySpark F.desc(col('v'))."""
    spark = SparkSession.builder.appName("issue_400").getOrCreate()
    df = spark.createDataFrame(
        [("a", 10), ("a", 20), ("b", 5)],
        ["k", "v"],
    )
    w = Window.partitionBy("k").orderBy(F.desc(F.col("v")))
    result = df.select("k", "v", F.row_number().over(w).alias("rn")).collect()
    # Partition a: v 20 gets rn=1 (desc), v 10 gets rn=2. Check by (k,v).
    by_kv = {(r["k"], r["v"]): r["rn"] for r in result}
    assert by_kv[("a", 20)] == 1
    assert by_kv[("a", 10)] == 2
    assert by_kv[("b", 5)] == 1


def test_window_order_by_desc_string() -> None:
    """Window.orderBy(desc("v")) - PySpark F.desc("v")."""
    spark = SparkSession.builder.appName("issue_400").getOrCreate()
    df = spark.createDataFrame(
        [("a", 10), ("a", 20)],
        ["k", "v"],
    )
    w = Window.partitionBy("k").orderBy(F.desc("v"))
    result = df.select("k", "v", F.row_number().over(w).alias("rn")).collect()
    by_kv = {(r["k"], r["v"]): r["rn"] for r in result}
    assert by_kv[("a", 20)] == 1
    assert by_kv[("a", 10)] == 2


def test_window_order_by_asc() -> None:
    """Window.orderBy(asc("v")) - ascending."""
    spark = SparkSession.builder.appName("issue_400").getOrCreate()
    df = spark.createDataFrame(
        [("a", 20), ("a", 10)],
        ["k", "v"],
    )
    w = Window.partitionBy("k").orderBy(F.asc("v"))
    result = df.select("k", "v", F.row_number().over(w).alias("rn")).collect()
    by_kv = {(r["k"], r["v"]): r["rn"] for r in result}
    assert by_kv[("a", 10)] == 1
    assert by_kv[("a", 20)] == 2
