"""
Tests for issue #1602: expr() inside agg().

PySpark accepts F.expr() SQL strings as aggregation expressions; sparkless
previously raised TypeError: agg() expects Column expressions or dict.
"""

from __future__ import annotations

from sparkless.testing import get_imports

F = get_imports().F


def test_expr_percentile_approx_in_groupby_agg(spark) -> None:
    """Exact scenario from issue #1602."""
    df = spark.createDataFrame([(1, 10.0), (1, 20.0), (2, 30.0)], ["col1", "col2"])
    rows = {
        r["col1"]: r["col2_median"]
        for r in df.groupBy("col1")
        .agg(F.expr("percentile_approx(col2, 0.5)").alias("col2_median"))
        .collect()
    }
    assert rows[1] == 15.0
    assert rows[2] == 30.0
