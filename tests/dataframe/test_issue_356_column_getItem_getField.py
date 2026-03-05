"""
Tests for #356: Column getItem, getField, and subscript (PySpark parity).

PySpark Column has .getItem(i) for array index (0-based), .getField(name) for struct field,
and col[key] / col[i] subscript. Robin-sparkless now implements these.
"""

from __future__ import annotations

from tests.fixtures.spark_imports import get_spark_imports

_imports = get_spark_imports()
F = _imports.F


def test_column_getItem_array_index(spark) -> None:
    """col(\"arr\").getItem(0) returns first element (issue repro)."""
    df = spark.createDataFrame(
        [{"arr": [1, 2, 3]}, {"arr": [10, 20]}],
        "arr array<bigint>",
    )
    out = df.select(F.col("arr").getItem(0).alias("first")).collect()
    assert len(out) == 2
    assert out[0]["first"] == 1
    assert out[1]["first"] == 10


def test_column_getItem_out_of_bounds_null(spark) -> None:
    """getItem out of bounds returns null."""
    df = spark.createDataFrame(
        [{"arr": [1, 2, 3]}],
        "arr array<bigint>",
    )
    out = df.select(F.col("arr").getItem(10).alias("x")).collect()
    assert len(out) == 1
    assert out[0]["x"] is None


def test_column_getField_struct(spark) -> None:
    """col(\"s\").getField(\"f\") extracts struct field."""
    df = spark.createDataFrame(
        [{"s": {"f": "hello", "g": 42}}, {"s": {"f": "world", "g": 0}}],
        "s struct<f:string,g:bigint>",
    )
    out = df.select(F.col("s").getField("f").alias("f_val")).collect()
    assert len(out) == 2
    assert out[0]["f_val"] == "hello"
    assert out[1]["f_val"] == "world"


def test_column_subscript_int_getItem(spark) -> None:
    """col[i] behaves like getItem(i)."""
    df = spark.createDataFrame(
        [{"arr": [1, 2, 3]}],
        "arr array<bigint>",
    )
    out = df.select(F.col("arr")[0].alias("first")).collect()
    assert out[0]["first"] == 1


def test_column_subscript_str_getField(spark) -> None:
    """col[\"name\"] behaves like getField(\"name\")."""
    df = spark.createDataFrame(
        [{"s": {"x": 100}}],
        "s struct<x:bigint>",
    )
    out = df.select(F.col("s")["x"].alias("x_val")).collect()
    assert out[0]["x_val"] == 100
