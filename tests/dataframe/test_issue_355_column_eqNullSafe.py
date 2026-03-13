"""
Tests for #355: Column.eqNullSafe (PySpark parity).

PySpark Column has .eqNullSafe(other) for null-safe equality.
"""

from __future__ import annotations

from sparkless.testing import get_imports

_imports = get_imports()
F = _imports.F


def test_column_eqNullSafe_select_lit(spark) -> None:
    """df.select(col(\"a\").eqNullSafe(lit(1))) returns boolean column."""
    df = spark.createDataFrame([{"a": 1}, {"a": None}], ["a"])
    out = df.select(F.col("a").eqNullSafe(F.lit(1)).alias("eq")).collect()
    assert len(out) == 2
    assert out[0]["eq"] is True
    assert out[1]["eq"] is False
