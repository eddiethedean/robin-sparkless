"""
Tests for issue #263: F.array() with no args (PySpark parity).

PySpark supports F.array() with no arguments, returning an empty array column (one [] per row).
Robin previously raised RuntimeError: array requires at least one column.
"""

from __future__ import annotations

import robin_sparkless as rs

F = rs


def test_array_empty_returns_empty_array_column() -> None:
    """with_column with F.array() (no args) succeeds and yields empty list per row."""
    spark = F.SparkSession.builder().app_name("test_263").get_or_create()
    df = spark.createDataFrame(
        [{"Name": "Alice"}, {"Name": "Bob"}],
        [("Name", "string")],
    )
    df = df.with_column("NewArray", F.array())
    out = df.collect()
    assert len(out) == 2
    assert out[0]["Name"] == "Alice"
    assert out[0]["NewArray"] == []
    assert out[1]["Name"] == "Bob"
    assert out[1]["NewArray"] == []


def test_array_empty_with_other_columns() -> None:
    """F.array() with no args works alongside other with_column expressions."""
    spark = F.SparkSession.builder().app_name("test_263").get_or_create()
    df = spark.createDataFrame([{"x": 1}, {"x": 2}], [("x", "int")])
    df = df.with_column("empty_arr", F.array()).with_column("double", F.col("x") * 2)
    out = df.collect()
    assert [r["empty_arr"] for r in out] == [[], []]
    assert [r["double"] for r in out] == [2, 4]
