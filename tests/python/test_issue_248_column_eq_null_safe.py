"""
Tests for issue #248: Column.eq_null_safe (eqNullSafe) for null-safe equality in filter.

PySpark's Column has eqNullSafe(other): NULL <=> NULL is true. Robin Column now exposes
eq_null_safe(other) so filter expressions using null-safe equality work.
"""

from __future__ import annotations

import robin_sparkless as rs


def test_column_has_eq_null_safe() -> None:
    """Column has eq_null_safe method (PySpark parity)."""
    c = rs.col("a")
    assert hasattr(c, "eq_null_safe")


def test_filter_eq_null_safe_lit_none_returns_null_row() -> None:
    """df.filter(col("a").eq_null_safe(lit(None))) returns only the row where a is null."""
    spark = rs.SparkSession.builder().app_name("eq_null_safe").get_or_create()
    data = [{"a": 1}, {"a": None}, {"a": 3}]
    schema = [("a", "int")]
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    df = create_df(data, schema)
    out = df.filter(rs.col("a").eq_null_safe(rs.lit(None))).collect()
    assert len(out) == 1
    assert out[0]["a"] is None


def test_filter_eq_null_safe_column_both_null_true() -> None:
    """Two columns: where both are null, eq_null_safe is true."""
    spark = rs.SparkSession.builder().app_name("eq_null_safe").get_or_create()
    data = [
        {"a": 1, "b": 2},
        {"a": None, "b": None},
        {"a": 3, "b": 4},
    ]
    schema = [("a", "int"), ("b", "int")]
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    df = create_df(data, schema)
    out = df.filter(rs.col("a").eq_null_safe(rs.col("b"))).collect()
    # Rows where a and b are equal (including both null): (1,2) no, (null,null) yes, (3,4) no
    assert len(out) == 1
    assert out[0]["a"] is None and out[0]["b"] is None
