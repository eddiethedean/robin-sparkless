"""
Tests for #370: Column.withField() for struct update (PySpark parity).

PySpark Column has .withField(name, value) to add or replace a struct field.
Robin-sparkless now implements withField.
"""

from __future__ import annotations

from tests.fixtures.spark_imports import get_spark_imports

_imports = get_spark_imports()
F = _imports.F

from tests.utils import _row_to_dict


def test_column_with_field_add_struct_field(spark) -> None:
    """col(\"s\").withField(\"c\", lit(3)) adds field c to struct (issue repro)."""
    create_df = getattr(spark, "create_dataframe_from_rows", spark.createDataFrame)
    df = create_df([{"s": {"a": 1, "b": 2}}], "s struct<a:int,b:int>")
    out = df.select(F.col("s").withField("c", F.lit(3)).alias("s")).collect()
    assert len(out) == 1
    row = out[0]
    assert "s" in row
    s = row["s"]
    assert _row_to_dict(s) == {"a": 1, "b": 2, "c": 3}


def test_column_with_field_replace_struct_field(spark) -> None:
    """withField with existing name replaces that field."""
    df = spark.createDataFrame(
        [{"s": {"a": 1, "b": 2}}],
        "s struct<a:bigint,b:bigint>",
    )
    out = df.select(F.col("s").withField("b", F.lit(99)).alias("s")).collect()
    assert len(out) == 1
    assert _row_to_dict(out[0]["s"]) == {"a": 1, "b": 99}


def test_column_with_field_literal_value(spark) -> None:
    """withField accepts lit( value ) for the new field."""
    df = spark.createDataFrame(
        [{"s": {"x": "hello"}}],
        "s struct<x:string>",
    )
    out = df.select(F.col("s").withField("y", F.lit(42)).alias("s")).collect()
    assert _row_to_dict(out[0]["s"]) == {"x": "hello", "y": 42}
