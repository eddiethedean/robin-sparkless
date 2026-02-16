"""
Tests for #388: DataFrame.unionAll() (PySpark parity).

PySpark DataFrame has unionAll(other) as alias for union(other).
"""

from __future__ import annotations

import robin_sparkless as rs


def _spark() -> rs.SparkSession:
    return rs.SparkSession.builder().app_name("issue_388").get_or_create()


def test_union_all_alias() -> None:
    """df1.unionAll(df2) behaves like union and stacks rows."""
    spark = _spark()
    df1 = spark.createDataFrame([(1,), (2,)], ["a"])
    df2 = spark.createDataFrame([(3,), (4,)], ["a"])
    out = df1.unionAll(df2).collect()
    assert len(out) == 4
    assert [r["a"] for r in out] == [1, 2, 3, 4]
