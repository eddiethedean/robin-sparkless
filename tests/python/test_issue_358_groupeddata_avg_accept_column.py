"""
Tests for #358: GroupedData.avg() (and sum, min, max) accept Column expression (PySpark parity).

PySpark: df.groupBy("k").avg(F.col("v")) or avg("v"). Robin-sparkless now accepts both.
"""

from __future__ import annotations

import robin_sparkless as rs


def _spark() -> rs.SparkSession:
    return rs.SparkSession.builder().app_name("issue_358").get_or_create()


def test_group_by_avg_column_expression() -> None:
    """df.group_by("k").avg(col("v")) works (issue repro)."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"k": "a", "v": 10}, {"k": "a", "v": 20}],
        [("k", "string"), ("v", "int")],
    )
    out = df.group_by("k").avg(rs.col("v")).collect()
    assert len(out) == 1
    assert out[0]["k"] == "a"
    assert out[0]["avg(v)"] == 15.0


def test_group_by_avg_string_still_works() -> None:
    """df.group_by("k").avg("v") still works."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"k": "a", "v": 10}, {"k": "a", "v": 20}],
        [("k", "string"), ("v", "int")],
    )
    out = df.group_by("k").avg("v").collect()
    assert len(out) == 1
    assert out[0]["avg(v)"] == 15.0


def test_group_by_sum_column_expression() -> None:
    """df.group_by("k").sum(col("v")) works."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"k": "a", "v": 10}, {"k": "a", "v": 20}],
        [("k", "string"), ("v", "int")],
    )
    out = df.group_by("k").sum(rs.col("v")).collect()
    assert len(out) == 1
    assert out[0]["sum(v)"] == 30


def test_group_by_min_max_column_expression() -> None:
    """df.group_by("k").min(col("v")) and .max(col("v")) work."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"k": "a", "v": 10}, {"k": "a", "v": 20}, {"k": "a", "v": 15}],
        [("k", "string"), ("v", "int")],
    )
    out_min = df.group_by("k").min(rs.col("v")).collect()
    out_max = df.group_by("k").max(rs.col("v")).collect()
    assert out_min[0]["min(v)"] == 10
    assert out_max[0]["max(v)"] == 20
