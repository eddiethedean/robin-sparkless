"""
Tests for issue #1599: percentile_approx with string column name.

PySpark accepts str or Column for the first argument; sparkless previously
required PyColumn and raised TypeError for strings.
"""

from __future__ import annotations

from sparkless.testing import get_imports

F = get_imports().F


def test_percentile_approx_string_column_name(spark) -> None:
    """Exact scenario from issue #1599."""
    df = spark.createDataFrame([(1, 10.0), (1, 20.0), (2, 30.0)], ["col1", "col2"])
    rows = {
        r["col1"]: r["col2_median"]
        for r in df.groupBy("col1")
        .agg(F.percentile_approx("col2", 0.5).alias("col2_median"))
        .collect()
    }
    assert rows[1] == 15.0
    assert rows[2] == 30.0


def test_percentile_approx_column_object(spark) -> None:
    """Column object argument should still work."""
    df = spark.createDataFrame([(1, 10.0), (1, 20.0)], ["col1", "col2"])
    rows = {
        r["col1"]: r["col2_median"]
        for r in df.groupBy("col1")
        .agg(F.percentile_approx(F.col("col2"), 0.5).alias("col2_median"))
        .collect()
    }
    assert rows[1] == 15.0
