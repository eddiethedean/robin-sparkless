"""Tests for issue #385: union() with DataFrame (PySpark parity).

PySpark union/unionAll require real DataFrames; custom DataFrame-like wrappers are sparkless-only.
"""

from __future__ import annotations

from tests.utils import get_spark


def test_union_accepts_dataframe() -> None:
    """union(other) with a plain DataFrame works."""
    spark = get_spark("issue_385")
    a = spark.createDataFrame([(1, "x")], ["id", "label"])
    b = spark.createDataFrame([(2, "y")], ["id", "label"])
    out = a.union(b)
    rows = out.collect()
    assert len(rows) == 2
    assert rows[0]["id"] == 1 and rows[0]["label"] == "x"
    assert rows[1]["id"] == 2 and rows[1]["label"] == "y"


def test_union_all_accepts_dataframe() -> None:
    """unionAll(other) with a plain DataFrame works."""
    spark = get_spark("issue_385")
    a = spark.createDataFrame([(1,)], ["v"])
    b = spark.createDataFrame([(2,)], ["v"])
    out = a.unionAll(b)
    rows = out.collect()
    assert len(rows) == 2
    assert rows[0]["v"] == 1 and rows[1]["v"] == 2
