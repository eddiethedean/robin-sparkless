"""
Tests for issue #248: Column.eq_null_safe (eqNullSafe) for null-safe equality in filter.

PySpark's Column has eqNullSafe(other): NULL <=> NULL is true. Robin Column now exposes
eq_null_safe(other) so filter expressions using null-safe equality work.
"""

from __future__ import annotations

from tests.python.utils import get_functions, get_spark

F = get_functions()


def test_column_has_eq_null_safe() -> None:
    """Column has eqNullSafe / eq_null_safe (PySpark parity)."""
    c = F.col("a")
    assert hasattr(c, "eqNullSafe") or hasattr(c, "eq_null_safe")


def test_filter_eq_null_safe_lit_none_returns_null_row() -> None:
    """df.filter(col("a").eq_null_safe(lit(None))) returns only the row where a is null."""
    spark = get_spark("eq_null_safe")
    data = [{"a": 1}, {"a": None}, {"a": 3}]
    df = spark.createDataFrame(data, ["a"])
    col_a = F.col("a")
    eq_safe = getattr(col_a, "eqNullSafe", None) or getattr(col_a, "eq_null_safe")
    out = df.filter(eq_safe(F.lit(None))).collect()
    assert len(out) == 1
    assert out[0]["a"] is None


def test_filter_eq_null_safe_column_both_null_true() -> None:
    """Two columns: where both are null, eq_null_safe is true."""
    spark = get_spark("eq_null_safe")
    data = [
        {"a": 1, "b": 2},
        {"a": None, "b": None},
        {"a": 3, "b": 4},
    ]
    df = spark.createDataFrame(data, ["a", "b"])
    col_a = F.col("a")
    eq_safe = getattr(col_a, "eqNullSafe", None) or getattr(col_a, "eq_null_safe")
    out = df.filter(eq_safe(F.col("b"))).collect()
    # Rows where a and b are equal (including both null): (1,2) no, (null,null) yes, (3,4) no
    assert len(out) == 1
    assert out[0]["a"] is None and out[0]["b"] is None
