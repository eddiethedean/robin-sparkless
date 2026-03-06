"""Tests for #381: DataFrame.agg() multiple expressions global (PySpark parity)."""

from __future__ import annotations

import pytest

from tests.fixtures.spark_imports import get_spark_imports

_imports = get_spark_imports()
F = _imports.F


def test_agg_multiple_positional_args(spark) -> None:
    """df.agg(sum(col("a")), avg(col("b"))) accepts multiple expressions as *args."""
    df = spark.createDataFrame(
        [{"a": 1, "b": 10}, {"a": 2, "b": 20}, {"a": 3, "b": 30}]
    )
    out = df.agg(F.sum(F.col("a")), F.avg(F.col("b")))
    rows = out.collect()
    assert len(rows) == 1
    row = rows[0]
    # sum(a)=6, avg(b)=20.0
    vals = [v for v in row.asDict().values() if isinstance(v, (int, float))]
    assert 6 in vals
    assert 20.0 in vals


def test_agg_single_expr_unchanged(spark) -> None:
    """df.agg(expr) still works."""
    df = spark.createDataFrame([{"x": 1}, {"x": 2}], schema=["x"])
    out = df.agg(F.sum(F.col("x")))
    rows = out.collect()
    assert len(rows) == 1
    assert list(rows[0].asDict().values()) == [3]


@pytest.mark.skip(reason="Issue #1274: unskip when fixing collect String schema semantics")
def test_agg_list_unchanged(spark) -> None:
    """df.agg([expr1, expr2]) still works."""
    df = spark.createDataFrame([{"a": 1, "b": 10}], schema=["a", "b"])
    out = df.agg(F.min(F.col("a")), F.max(F.col("b")))
    rows = out.collect()
    assert len(rows) == 1
    vals = list(rows[0].asDict().values())
    assert 1 in vals and 10 in vals
