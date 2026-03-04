"""
Tests for issue #236: CaseWhen Column.otherwise() missing (AttributeError).

With robin-sparkless 0.6.0, PySpark-style when(cond, val).otherwise(default) failed with
AttributeError: 'builtins.Column' object has no attribute 'otherwise'.
"""

from __future__ import annotations

from tests.python.utils import _row_to_dict, get_functions


def test_when_cond_val_otherwise_returns_column(spark) -> None:
    """when(cond, val).otherwise(default) returns a Column and works in with_column (PySpark parity)."""
    F = get_functions()
    data = [{"a": 1}, {"a": -1}, {"a": 0}]
    df = spark.createDataFrame(data, ["a"])

    expr = F.when(F.col("a") > F.lit(0), F.lit(1)).otherwise(F.lit(0))
    out = [ _row_to_dict(r) for r in df.withColumn("x", expr).collect() ]

    assert out == [
        {"a": 1, "x": 1},
        {"a": -1, "x": 0},
        {"a": 0, "x": 0},
    ]


def test_when_cond_val_otherwise_operator_syntax(spark) -> None:
    """when(cond, val).otherwise(default) works with Column __gt__ (F.col('a') > F.lit(0))."""
    F = get_functions()
    data = [{"a": 1}, {"a": -1}, {"a": 0}]
    df = spark.createDataFrame(data, ["a"])
    expr = F.when(F.col("a") > F.lit(0), F.lit(1)).otherwise(F.lit(0))
    out = [ _row_to_dict(r) for r in df.withColumn("x", expr).collect() ]

    assert out == [
        {"a": 1, "x": 1},
        {"a": -1, "x": 0},
        {"a": 0, "x": 0},
    ]
