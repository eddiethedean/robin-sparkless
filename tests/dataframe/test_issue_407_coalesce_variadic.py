"""
Tests for #407: coalesce() variadic arguments (PySpark parity).

PySpark F.coalesce(col1, col2, ...) accepts multiple Column arguments.
"""

from __future__ import annotations

from tests.utils import get_functions, get_spark

F = get_functions()


def _spark():
    return get_spark("issue_407")


def test_coalesce_two_columns() -> None:
    """coalesce(col("salary"), lit(0)) returns first non-null."""
    spark = _spark()
    df = spark.createDataFrame([(None,), (100,)], ["salary"])
    out = df.select(
        F.coalesce(F.col("salary"), F.lit(0)).alias("coalesced")
    ).collect()
    assert len(out) == 2
    assert out[0]["coalesced"] == 0
    assert out[1]["coalesced"] == 100


def test_coalesce_three_arguments() -> None:
    """coalesce(col1, col2, col3) with three columns."""
    spark = _spark()
    df = spark.createDataFrame(
        [(None, None, 3), (None, 2, 3), (1, 2, 3)],
        ["a", "b", "c"],
    )
    out = df.select(
        F.coalesce(F.col("a"), F.col("b"), F.col("c")).alias("first")
    ).collect()
    assert [r["first"] for r in out] == [3, 2, 1]
