"""
Tests for #370: Column.withField() for struct update (PySpark parity).

PySpark Column has .withField(name, value) to add or replace a struct field.
Robin-sparkless now implements withField.
"""

from __future__ import annotations

import robin_sparkless as rs


def _spark() -> rs.SparkSession:
    return rs.SparkSession.builder().app_name("issue_370").get_or_create()


def test_column_with_field_add_struct_field() -> None:
    """col("s").withField("c", lit(3)) adds field c to struct (issue repro)."""
    spark = _spark()
    create_df = getattr(spark, "create_dataframe_from_rows", spark.createDataFrame)
    df = create_df([{"s": {"a": 1, "b": 2}}], [("s", "struct<a:int,b:int>")])
    out = df.select(rs.col("s").withField("c", rs.lit(3)).alias("s")).collect()
    assert len(out) == 1
    row = out[0]
    assert "s" in row
    s = row["s"]
    assert s == {"a": 1, "b": 2, "c": 3}


def test_column_with_field_replace_struct_field() -> None:
    """withField with existing name replaces that field."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"s": {"a": 1, "b": 2}}],
        [("s", "struct<a:bigint,b:bigint>")],
    )
    out = df.select(rs.col("s").withField("b", rs.lit(99)).alias("s")).collect()
    assert len(out) == 1
    assert out[0]["s"] == {"a": 1, "b": 99}


def test_column_with_field_literal_value() -> None:
    """withField accepts lit( value ) for the new field."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"s": {"x": "hello"}}],
        [("s", "struct<x:string>")],
    )
    out = df.select(rs.col("s").withField("y", rs.lit(42)).alias("s")).collect()
    assert out[0]["s"] == {"x": "hello", "y": 42}
