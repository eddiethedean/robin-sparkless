"""
Tests for issue #236: CaseWhen Column.otherwise() missing (AttributeError).

With robin-sparkless 0.6.0, PySpark-style when(cond, val).otherwise(default) failed with
AttributeError: 'builtins.Column' object has no attribute 'otherwise'.
"""

from __future__ import annotations


def test_when_cond_val_otherwise_returns_column() -> None:
    """when(cond, val).otherwise(default) returns a Column and works in with_column (PySpark parity)."""
    import robin_sparkless as rs

    F = rs
    spark = F.SparkSession.builder().app_name("repro").get_or_create()
    data = [{"a": 1}, {"a": -1}, {"a": 0}]
    schema = [("a", "int")]
    df = spark._create_dataframe_from_rows(data, schema)

    expr = F.when(F.col("a").gt(F.lit(0)), F.lit(1)).otherwise(F.lit(0))
    out = df.with_column("x", expr).collect()

    assert out == [
        {"a": 1, "x": 1},
        {"a": -1, "x": 0},
        {"a": 0, "x": 0},
    ]


def test_when_cond_val_otherwise_operator_syntax() -> None:
    """when(cond, val).otherwise(default) works with Column __gt__ (F.col('a') > F.lit(0))."""
    import robin_sparkless as rs

    F = rs
    spark = F.SparkSession.builder().app_name("repro").get_or_create()
    data = [{"a": 1}, {"a": -1}, {"a": 0}]
    schema = [("a", "int")]
    df = spark._create_dataframe_from_rows(data, schema)
    expr = F.when(F.col("a") > F.lit(0), F.lit(1)).otherwise(F.lit(0))
    out = df.with_column("x", expr).collect()

    assert out == [
        {"a": 1, "x": 1},
        {"a": -1, "x": 0},
        {"a": 0, "x": 0},
    ]
