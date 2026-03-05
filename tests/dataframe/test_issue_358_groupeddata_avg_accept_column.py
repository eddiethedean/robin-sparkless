"""
Tests for #358: GroupedData.avg() (and sum, min, max) accept Column expression (PySpark parity).

PySpark: df.groupBy("k").avg(F.col("v")) or avg("v"). Robin-sparkless now accepts both.
"""

from __future__ import annotations

from tests.utils import get_functions, get_spark

F = get_functions()


def _spark():
    return get_spark("issue_358")


def test_group_by_avg_column_expression() -> None:
    """df.groupBy(\"k\").avg(col(\"v\")) works (issue repro)."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"k": "a", "v": 10}, {"k": "a", "v": 20}],
        ["k", "v"],
    )
    out = df.groupBy("k").avg("v").collect()
    assert len(out) == 1
    assert out[0]["k"] == "a"
    assert out[0]["avg(v)"] == 15.0


def test_group_by_avg_string_still_works() -> None:
    """df.groupBy(\"k\").avg(\"v\") still works."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"k": "a", "v": 10}, {"k": "a", "v": 20}],
        ["k", "v"],
    )
    out = df.groupBy("k").avg("v").collect()
    assert len(out) == 1
    assert out[0]["avg(v)"] == 15.0


def test_group_by_sum_column_expression() -> None:
    """df.groupBy(\"k\").sum(col(\"v\")) works."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"k": "a", "v": 10}, {"k": "a", "v": 20}],
        ["k", "v"],
    )
    out = df.groupBy("k").sum("v").collect()
    assert len(out) == 1
    assert out[0]["sum(v)"] == 30


def test_group_by_min_max_column_expression() -> None:
    """df.groupBy(\"k\").min(col(\"v\")) and .max(col(\"v\")) work."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"k": "a", "v": 10}, {"k": "a", "v": 20}, {"k": "a", "v": 15}],
        ["k", "v"],
    )
    out_min = df.groupBy("k").min("v").collect()
    out_max = df.groupBy("k").max("v").collect()
    assert out_min[0]["min(v)"] == 10
    assert out_max[0]["max(v)"] == 20
