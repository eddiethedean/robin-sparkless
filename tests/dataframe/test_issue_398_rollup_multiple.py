"""
Tests for #398: rollup() multiple columns (PySpark parity).

PySpark df.rollup("a", "b") and df.rollup(*cols) accept multiple column names.
"""

from __future__ import annotations

from tests.utils import get_functions, get_spark

F = get_functions()


def _spark():
    return get_spark("issue_398")


def test_rollup_two_columns_variadic() -> None:
    """df.rollup(\"a\", \"b\").count() works with variadic args."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"a": 1, "b": 2, "v": 10}, {"a": 1, "b": 2, "v": 20}],
        ["a", "b", "v"],
    )
    out = df.rollup("a", "b").agg(F.count(F.col("v")).alias("count")).collect()
    assert len(out) >= 1
    assert all("count" in r for r in out)


def test_rollup_single_list() -> None:
    """df.rollup([\"a\", \"b\"]) still works."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"a": 1, "b": 2, "v": 10}],
        ["a", "b", "v"],
    )
    out = df.rollup(["a", "b"]).agg(F.count(F.col("v")).alias("count")).collect()
    assert len(out) >= 1
