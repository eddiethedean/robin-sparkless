"""Tests for F.sumDistinct (PySpark parity)."""

from __future__ import annotations

from sparkless.testing import get_imports

F = get_imports().F


def test_sum_distinct_groupby(spark) -> None:
    """sumDistinct sums unique values per group."""
    df = spark.createDataFrame(
        [
            ("a", 1),
            ("a", 1),
            ("a", 2),
            ("b", 10),
            ("b", 20),
        ],
        ["k", "v"],
    )
    out = df.groupBy("k").agg(F.sumDistinct("v").alias("total"))
    rows = {r["k"]: r["total"] for r in out.collect()}
    assert rows["a"] == 3  # distinct 1, 2
    assert rows["b"] == 30  # distinct 10, 20
