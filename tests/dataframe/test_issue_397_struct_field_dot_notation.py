"""
Tests for #397: struct field dot notation (PySpark parity).

PySpark supports col("struct_col.field") and select("struct_col.field") when
struct_col is a struct column with that field. createDataFrame lowercases
struct field names in the schema, so we use lowercase field names in tests.
"""

from __future__ import annotations

from sparkless.testing import get_imports

_imports = get_imports()
F = _imports.F


def test_select_struct_field_dot_notation_string(spark) -> None:
    """select("StructValue.e1") selects struct field (issue repro)."""
    df = spark.createDataFrame(
        [
            {"StructValue": {"e1": 42, "e2": "a"}},
            {"StructValue": {"e1": 10, "e2": "b"}},
        ],
        "StructValue struct<e1:int,e2:string>",
    )
    out = df.select("StructValue.e1").collect()
    assert len(out) == 2
    assert out[0]["e1"] == 42
    assert out[1]["e1"] == 10


def test_select_struct_field_dot_notation_col(spark) -> None:
    """select(col(\"StructValue.e1\")) works like string form."""
    df = spark.createDataFrame(
        [{"StructValue": {"e1": 100, "e2": "x"}}],
        "StructValue struct<e1:int,e2:string>",
    )
    out = df.select(F.col("StructValue.e1")).collect()
    assert len(out) == 1
    assert out[0]["e1"] == 100


def test_select_struct_field_multiple_dots(spark) -> None:
    """select("outer.inner.leaf") for nested struct."""
    # Nested struct: outer has field inner which is struct<leaf:int>
    df = spark.createDataFrame(
        [{"outer": {"inner": {"leaf": 7}}}],
        "outer struct<inner:struct<leaf:int>>",
    )
    out = df.select("outer.inner.leaf").collect()
    assert len(out) == 1
    assert out[0]["leaf"] == 7
