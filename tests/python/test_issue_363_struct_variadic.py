"""
Tests for #363: struct() accept multiple Column arguments (PySpark parity).

PySpark F.struct(col1, col2, ...) accepts variadic Column arguments.
Robin-sparkless struct() now accepts the same: struct(col1, col2, ...).
"""

from __future__ import annotations

from tests.fixtures.spark_imports import get_spark_imports


_imports = get_spark_imports()
SparkSession = _imports.SparkSession
F = _imports.F


def test_struct_multiple_columns_issue_repro() -> None:
    """struct(col("a"), col("b")) works (issue repro)."""
    from tests.python.utils import _row_to_dict

    spark = SparkSession.builder.appName("issue_363").getOrCreate()
    df = spark.createDataFrame([{"a": 1, "b": 2}], ["a", "b"])
    rows = df.select(F.struct(F.col("a"), F.col("b")).alias("s")).collect()
    row0 = _row_to_dict(rows[0])
    assert list(row0.keys()) == ["s"]
    nested = rows[0]["s"]  # PySpark struct column returns Row
    assert nested["a"] == 1 and nested["b"] == 2


def test_struct_single_column() -> None:
    """struct(col("x")) produces struct with one field."""
    spark = SparkSession.builder.appName("issue_363").getOrCreate()
    df = spark.createDataFrame([{"x": 10}], ["x"])
    rows = df.select(F.struct(F.col("x")).alias("s")).collect()
    nested = rows[0]["s"]
    assert nested["x"] == 10


def test_struct_three_columns() -> None:
    """struct(col1, col2, col3) produces struct with three fields."""
    from tests.python.utils import _row_to_dict

    spark = SparkSession.builder.appName("issue_363").getOrCreate()
    df = spark.createDataFrame([(1, "a", 3.0)], ["i", "s", "f"])
    rows = df.select(
        F.struct(F.col("i"), F.col("s"), F.col("f")).alias("s")
    ).collect()
    nested = rows[0]["s"]
    d = _row_to_dict(nested) if not isinstance(nested, dict) else nested
    assert d["i"] == 1 and d["s"] == "a" and d["f"] == 3.0
