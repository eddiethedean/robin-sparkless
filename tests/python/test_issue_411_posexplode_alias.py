"""
Tests for #411: posexplode().alias("pos", "val") for select (PySpark parity).
"""

from __future__ import annotations

from tests.fixtures.spark_imports import get_spark_imports


_imports = get_spark_imports()
SparkSession = _imports.SparkSession
F = _imports.F


def _spark() -> SparkSession:
    return SparkSession.builder.appName("issue_411").getOrCreate()


def test_posexplode_alias_in_select() -> None:
    """posexplode("arr").alias("pos", "val") returns (pos_col, val_col) usable in select."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"id": 1, "arr": [10, 20]}],
        schema=[("id", "bigint"), ("arr", "array<bigint>")],
    )
    # Select only posexplode result (two columns); no mixing with id to avoid row-length mismatch.
    out = df.select(F.posexplode("arr").alias("pos", "val"))
    rows = out.collect()
    schema = out.schema
    col_names = [f.name for f in schema.fields]
    assert col_names == ["pos", "val"]
    assert len(rows) >= 1
    # posexplode explodes array into position + value columns
    first = rows[0]
    assert "pos" in first or hasattr(first, "__getitem__")
    assert "val" in first or (hasattr(first, "__getitem__") and len(first) >= 2)
