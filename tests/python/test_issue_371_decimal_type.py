"""
Tests for #371: Decimal type support (PySpark parity).

PySpark supports DecimalType (e.g. Decimal(10,0), Decimal(10,2)) in schema.
Robin-sparkless now accepts Decimal(p,s) in schema and maps to Float64 for storage.
"""

from __future__ import annotations

import robin_sparkless as rs


def _spark() -> rs.SparkSession:
    return rs.SparkSession.builder().app_name("issue_371").get_or_create()


def test_decimal_schema_create_dataframe() -> None:
    """createDataFrame with Decimal(10,2) schema works (issue repro)."""
    spark = _spark()
    create_df = getattr(spark, "create_dataframe_from_rows", spark.createDataFrame)
    df = create_df([{"d": 1.5}], [("d", "Decimal(10,2)")])
    out = df.collect()
    assert len(out) == 1
    assert out[0]["d"] == 1.5


def test_decimal_schema_lowercase() -> None:
    """decimal(10,0) (lowercase) is accepted."""
    spark = _spark()
    df = spark.createDataFrame([{"x": 42.0}], [("x", "decimal(10,0)")])
    rows = df.collect()
    assert rows[0]["x"] == 42.0


def test_decimal_multiple_rows() -> None:
    """Decimal column with multiple rows."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"d": 1.5}, {"d": 2.25}, {"d": None}],
        [("d", "Decimal(10,2)")],
    )
    out = df.collect()
    assert len(out) == 3
    assert out[0]["d"] == 1.5
    assert out[1]["d"] == 2.25
    assert out[2]["d"] is None
