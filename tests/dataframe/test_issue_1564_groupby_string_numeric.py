"""Tests for issue #1564: string column vs numeric literal in groupBy().agg()."""

from __future__ import annotations

from sparkless.testing import get_imports

_imports = get_imports()
F = _imports.F


def test_groupby_agg_when_string_column_eq_int_literal(spark) -> None:
    """PySpark casts string C to int for comparison with lit(1)."""
    df = spark.createDataFrame(
        [("A", "C", "1", 100.0)],
        "A STRING, B STRING, C STRING, D DOUBLE",
    )
    condition = (F.col("B") == "C") & (F.col("C") == 1)
    result = df.groupby("A").agg(
        F.sum(F.when(condition, F.col("D")).otherwise(F.lit(0))).alias("Result")
    )
    rows = result.collect()

    assert len(rows) == 1
    assert rows[0]["A"] == "A"
    assert rows[0]["Result"] == 100.0
