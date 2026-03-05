"""
Tests for #371: Decimal type support (PySpark parity).

PySpark supports DecimalType (e.g. Decimal(10,0), Decimal(10,2)) in schema.
Robin-sparkless now accepts Decimal(p,s) in schema and maps to Float64 for storage.
"""

from __future__ import annotations

from decimal import Decimal


def test_decimal_schema_create_dataframe(spark) -> None:
    """createDataFrame with Decimal(10,2) schema works (issue repro)."""
    create_df = getattr(spark, "create_dataframe_from_rows", spark.createDataFrame)
    df = create_df([{"d": Decimal("1.5")}], "d decimal(10,2)")
    out = df.collect()
    assert len(out) == 1
    val = out[0]["d"]
    assert isinstance(val, (Decimal, float))
    assert float(val) == 1.5


def test_decimal_schema_lowercase(spark) -> None:
    """decimal(10,0) (lowercase) is accepted."""
    df = spark.createDataFrame([{"x": Decimal("42")}], "x decimal(10,0)")
    rows = df.collect()
    val = rows[0]["x"]
    assert isinstance(val, (Decimal, float, int))
    assert float(val) == 42.0


def test_decimal_multiple_rows(spark) -> None:
    """Decimal column with multiple rows."""
    df = spark.createDataFrame(
        [{"d": Decimal("1.5")}, {"d": Decimal("2.25")}, {"d": None}],
        "d decimal(10,2)",
    )
    out = df.collect()
    assert len(out) == 3
    assert float(out[0]["d"]) == 1.5
    assert float(out[1]["d"]) == 2.25
    assert out[2]["d"] is None
