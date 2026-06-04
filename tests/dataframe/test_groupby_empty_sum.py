"""Regression: empty groupBy().sum() must not leak _gb_global ghost column."""

from __future__ import annotations

from sparkless.testing import get_imports

imports = get_imports()
F = imports.F


def test_empty_groupby_sum_no_ghost_column(spark):
    df = spark.createDataFrame([(1, 10), (2, 20)], ["k", "v"])
    out = df.groupBy().sum("v")
    cols = out.columns
    assert "_gb_global" not in cols
    assert any(c.startswith("sum(") for c in cols)


def test_empty_groupby_sum_value(spark):
    df = spark.createDataFrame([(1, 10), (2, 20)], ["k", "v"])
    row = df.groupBy().sum("v").collect()[0]
    sum_col = next(k for k in row.__fields__ if k.startswith("sum("))
    assert row[sum_col] == 30
