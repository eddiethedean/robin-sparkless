"""
Tests for #355: Column.eqNullSafe (PySpark parity).

PySpark Column has .eqNullSafe(other) for null-safe equality.
"""

from __future__ import annotations

from tests.python.utils import get_functions, get_spark

F = get_functions()


def test_column_eqNullSafe_select_lit() -> None:
    """df.select(col(\"a\").eqNullSafe(lit(1))) returns boolean column."""
    spark = get_spark("issue_355")
    df = spark.createDataFrame([{"a": 1}, {"a": None}], ["a"])
    out = df.select(F.col("a").eqNullSafe(F.lit(1)).alias("eq")).collect()
    assert len(out) == 2
    assert out[0]["eq"] is True
    assert out[1]["eq"] is False
