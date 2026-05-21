"""Tests for issue #1544: DDL schema decimal(p,s) token parsing."""

from __future__ import annotations

from decimal import Decimal


def test_create_dataframe_decimal_ddl_with_tuple_rows(spark) -> None:
    """schema='A decimal(38,0), B string' with tuple rows (issue #1544 repro)."""
    df = spark.createDataFrame(
        [(Decimal(1), "ok")],
        schema="A decimal(38,0), B string",
    )
    rows = df.collect()
    assert len(rows) == 1
    assert rows[0]["B"] == "ok"
    val = rows[0]["A"]
    assert isinstance(val, (Decimal, float, int))
    assert float(val) == 1.0


def test_create_dataframe_decimal_ddl_colon_syntax(spark) -> None:
    """Colon DDL syntax: 'A: decimal(10,2), B: string'."""
    df = spark.createDataFrame(
        [{"A": Decimal("3.14"), "B": "x"}],
        schema="A: decimal(10,2), B: string",
    )
    rows = df.collect()
    assert float(rows[0]["A"]) == 3.14
