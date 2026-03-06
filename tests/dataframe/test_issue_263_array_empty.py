"""
Tests for issue #263: F.array() with no args (PySpark parity).

PySpark supports F.array() with no arguments, returning an empty array column (one [] per row).
Robin previously raised RuntimeError: array requires at least one column.
"""

from __future__ import annotations
import pytest

from tests.fixtures.spark_imports import get_spark_imports


_imports = get_spark_imports()
SparkSession = _imports.SparkSession
F = _imports.F


@pytest.mark.skip(reason="Issue #1203: unskip when fixing")
def test_array_empty_returns_empty_array_column() -> None:
    """with_column with F.array() (no args) succeeds and yields empty list per row."""
    spark = SparkSession.builder.appName("test_263").getOrCreate()
    df = spark.createDataFrame(
        [{"Name": "Alice"}, {"Name": "Bob"}],
        ["Name"],
    )
    df = df.withColumn("NewArray", F.array())
    out = df.collect()
    assert len(out) == 2
    assert out[0]["Name"] == "Alice"
    assert out[0]["NewArray"] == []
    assert out[1]["Name"] == "Bob"
    assert out[1]["NewArray"] == []


def test_array_empty_with_other_columns() -> None:
    """F.array() with no args works alongside other with_column expressions."""
    spark = SparkSession.builder.appName("test_263").getOrCreate()
    df = spark.createDataFrame([{"x": 1}, {"x": 2}], ["x"])
    df = df.withColumn("empty_arr", F.array()).withColumn("double", F.col("x") * 2)
    out = df.collect()
    assert [r["empty_arr"] for r in out] == [[], []]
    assert [r["double"] for r in out] == [2, 4]
