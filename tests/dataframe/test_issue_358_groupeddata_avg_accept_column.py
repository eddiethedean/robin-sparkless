"""
Tests for #358: GroupedData.avg() (and sum, min, max) accept Column expression (PySpark parity).

PySpark: df.groupBy("k").avg(F.col("v")) or avg("v"). Robin-sparkless now accepts both.
"""

from __future__ import annotations

from tests.fixtures.spark_imports import get_spark_imports

_imports = get_spark_imports()
F = _imports.F


def test_group_by_avg_column_expression(spark) -> None:
    """df.groupBy(\"k\").avg(col(\"v\")) works (issue repro)."""
    df = spark.createDataFrame(
        [{"k": "a", "v": 10}, {"k": "a", "v": 20}],
        ["k", "v"],
    )
    out = df.groupBy("k").avg("v").collect()
    assert len(out) == 1
    assert out[0]["k"] == "a"
    assert out[0]["avg(v)"] == 15.0


def test_group_by_avg_string_still_works(spark) -> None:
    """df.groupBy(\"k\").avg(\"v\") still works."""
    df = spark.createDataFrame(
        [{"k": "a", "v": 10}, {"k": "a", "v": 20}],
        ["k", "v"],
    )
    out = df.groupBy("k").avg("v").collect()
    assert len(out) == 1
    assert out[0]["avg(v)"] == 15.0


def test_group_by_sum_column_expression(spark) -> None:
    """df.groupBy(\"k\").sum(col(\"v\")) works."""
    df = spark.createDataFrame(
        [{"k": "a", "v": 10}, {"k": "a", "v": 20}],
        ["k", "v"],
    )
    out = df.groupBy("k").sum("v").collect()
    assert len(out) == 1
    assert out[0]["sum(v)"] == 30


def test_group_by_min_max_column_expression(spark) -> None:
    """df.groupBy(\"k\").min(col(\"v\")) and .max(col(\"v\")) work."""
    df = spark.createDataFrame(
        [{"k": "a", "v": 10}, {"k": "a", "v": 20}, {"k": "a", "v": 15}],
        ["k", "v"],
    )
    out_min = df.groupBy("k").min("v").collect()
    out_max = df.groupBy("k").max("v").collect()
    assert out_min[0]["min(v)"] == 10
    assert out_max[0]["max(v)"] == 20
