"""
Tests for issue #1472: count(lit(1)) should return row count (PySpark parity).

Sparkless previously returned the literal value (1) instead of counting all rows.
"""

from __future__ import annotations

from sparkless.testing import get_imports


imports = get_imports()
SparkSession = imports.SparkSession
F = imports.F


def test_count_lit_one_counts_all_rows(spark) -> None:
    """F.count(F.lit(1)) counts all rows, including nulls."""
    df = spark.createDataFrame(
        [
            {"val": 1},
            {"val": None},
            {"val": 3},
            {"val": None},
        ]
    )

    result = df.agg(F.count(F.lit(1)).alias("count_all")).collect()
    assert len(result) == 1
    assert result[0]["count_all"] == 4

