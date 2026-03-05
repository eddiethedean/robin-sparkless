"""Tests for issue #389: DataFrame.intersect() and exceptAll()."""

from __future__ import annotations


def test_intersect(spark) -> None:
    """intersect(other) returns rows that appear in both DataFrames (distinct)."""
    left = spark.createDataFrame([(1, "a"), (2, "b"), (2, "b")], ["id", "x"])
    right = spark.createDataFrame([(2, "b"), (3, "c")], ["id", "x"])
    out = left.intersect(right)
    rows = out.collect()
    assert len(rows) == 1
    assert rows[0]["id"] == 2 and rows[0]["x"] == "b"


def test_except_all(spark) -> None:
    """exceptAll(other) returns rows in self not in other (API parity; current impl same as subtract)."""
    left = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["id", "x"])
    right = spark.createDataFrame([(2, "b")], ["id", "x"])
    out = left.exceptAll(right)
    rows = out.collect()
    assert len(rows) == 2
    ids = sorted(r["id"] for r in rows)
    assert ids == [1, 3]
