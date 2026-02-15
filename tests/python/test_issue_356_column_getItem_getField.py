"""
Tests for #356: Column getItem, getField, and subscript (PySpark parity).

PySpark Column has .getItem(i) for array index (0-based), .getField(name) for struct field,
and col[key] / col[i] subscript. Robin-sparkless now implements these.
"""

from __future__ import annotations

import robin_sparkless as rs


def _spark() -> rs.SparkSession:
    return rs.SparkSession.builder().app_name("issue_356").get_or_create()


def test_column_getItem_array_index() -> None:
    """col("arr").getItem(0) returns first element (issue repro)."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"arr": [1, 2, 3]}, {"arr": [10, 20]}],
        [("arr", "array<bigint>")],
    )
    out = df.select(rs.col("arr").getItem(0).alias("first")).collect()
    assert len(out) == 2
    assert out[0]["first"] == 1
    assert out[1]["first"] == 10


def test_column_getItem_out_of_bounds_null() -> None:
    """getItem out of bounds returns null."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"arr": [1, 2, 3]}],
        [("arr", "array<bigint>")],
    )
    out = df.select(rs.col("arr").getItem(10).alias("x")).collect()
    assert len(out) == 1
    assert out[0]["x"] is None


def test_column_getField_struct() -> None:
    """col("s").getField("f") extracts struct field."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"s": {"f": "hello", "g": 42}}, {"s": {"f": "world", "g": 0}}],
        [("s", "struct<f:string,g:bigint>")],
    )
    out = df.select(rs.col("s").getField("f").alias("f_val")).collect()
    assert len(out) == 2
    assert out[0]["f_val"] == "hello"
    assert out[1]["f_val"] == "world"


def test_column_subscript_int_getItem() -> None:
    """col[i] behaves like getItem(i)."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"arr": [1, 2, 3]}],
        [("arr", "array<bigint>")],
    )
    out = df.select(rs.col("arr")[0].alias("first")).collect()
    assert out[0]["first"] == 1


def test_column_subscript_str_getField() -> None:
    """col["name"] behaves like getField("name")."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"s": {"x": 100}}],
        [("s", "struct<x:bigint>")],
    )
    out = df.select(rs.col("s")["x"].alias("x_val")).collect()
    assert out[0]["x_val"] == 100
