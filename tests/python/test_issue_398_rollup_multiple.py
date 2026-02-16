"""
Tests for #398: rollup() multiple columns (PySpark parity).

PySpark df.rollup("a", "b") and df.rollup(*cols) accept multiple column names.
"""

from __future__ import annotations

import robin_sparkless as rs


def _spark() -> rs.SparkSession:
    return rs.SparkSession.builder().app_name("issue_398").get_or_create()


def test_rollup_two_columns_variadic() -> None:
    """df.rollup("a", "b").count() works with variadic args."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"a": 1, "b": 2, "v": 10}, {"a": 1, "b": 2, "v": 20}],
        [("a", "int"), ("b", "int"), ("v", "int")],
    )
    out = df.rollup("a", "b").agg([rs.count(rs.col("v")).alias("count")]).collect()
    assert len(out) >= 1
    assert all("count" in r for r in out)


def test_rollup_single_list() -> None:
    """df.rollup(["a", "b"]) still works."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"a": 1, "b": 2, "v": 10}],
        [("a", "int"), ("b", "int"), ("v", "int")],
    )
    out = df.rollup(["a", "b"]).agg([rs.count(rs.col("v")).alias("count")]).collect()
    assert len(out) >= 1
