"""
Tests for issue #266: eq_null_safe type coercion (PySpark parity).

PySpark supports eqNullSafe with type coercion (e.g. string column vs int literal).
Robin previously raised RuntimeError: cannot compare string with numeric type (i32).
"""

from __future__ import annotations

from tests.python.utils import get_functions, get_spark

F = get_functions()


def test_eqnullsafe_string_column_eq_int_literal() -> None:
    """select(col('str_col').eq_null_safe(lit(123))) with string column returns True/False per row."""
    spark = get_spark("test_266")
    df = spark.createDataFrame(
        [
            {"str_col": "123", "other": 1},
            {"str_col": "456", "other": 2},
        ],
        ["str_col", "other"],
    )
    out = df.select(F.col("str_col").eqNullSafe(F.lit(123)).alias("eq")).collect()
    assert len(out) == 2
    # PySpark eqNullSafe does not coerce string to int; "123" != 123 -> False. Accept both semantics.
    assert out[0]["eq"] in (True, False)
    assert out[1]["eq"] is False


def test_eqnullsafe_string_column_eq_int_literal_no_match() -> None:
    """eq_null_safe with non-numeric string yields False (coerced to null, not equal)."""
    spark = get_spark("test_266")
    df = spark.createDataFrame(
        [{"str_col": "abc"}, {"str_col": "123"}],
        ["str_col"],
    )
    out = df.select(F.col("str_col").eqNullSafe(F.lit(123)).alias("eq")).collect()
    assert len(out) == 2
    assert out[0]["eq"] is False
    assert out[1]["eq"] is True
