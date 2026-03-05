"""
Tests for #388: DataFrame.unionAll() (PySpark parity).

PySpark DataFrame has unionAll(other) as alias for union(other).
"""

from __future__ import annotations


def test_union_all_alias(spark) -> None:
    """df1.unionAll(df2) behaves like union and stacks rows."""
    df1 = spark.createDataFrame([(1,), (2,)], ["a"])
    df2 = spark.createDataFrame([(3,), (4,)], ["a"])
    out = df1.unionAll(df2).collect()
    assert len(out) == 4
    assert [r["a"] for r in out] == [1, 2, 3, 4]
