"""
Tests for issue #266: eq_null_safe type coercion (PySpark parity).

PySpark supports eqNullSafe with type coercion (e.g. string column vs int literal).
Robin previously raised RuntimeError: cannot compare string with numeric type (i32).
"""

from __future__ import annotations

import robin_sparkless as rs

F = rs


def test_eqnullsafe_string_column_eq_int_literal() -> None:
    """select(col('str_col').eq_null_safe(lit(123))) with string column returns True/False per row."""
    spark = F.SparkSession.builder().app_name("test_266").get_or_create()
    df = spark.createDataFrame(
        [
            {"str_col": "123", "other": 1},
            {"str_col": "456", "other": 2},
        ],
        [("str_col", "string"), ("other", "int")],
    )
    out = df.select(F.col("str_col").eq_null_safe(F.lit(123)).alias("eq")).collect()
    assert len(out) == 2
    # "123" == 123 -> True; "456" != 123 -> False (PySpark coerces string to number)
    assert out[0]["eq"] is True
    assert out[1]["eq"] is False


def test_eqnullsafe_string_column_eq_int_literal_no_match() -> None:
    """eq_null_safe with non-numeric string yields False (coerced to null, not equal)."""
    spark = F.SparkSession.builder().app_name("test_266").get_or_create()
    df = spark.createDataFrame(
        [{"str_col": "abc"}, {"str_col": "123"}],
        [("str_col", "string")],
    )
    out = df.select(F.col("str_col").eq_null_safe(F.lit(123)).alias("eq")).collect()
    assert len(out) == 2
    # "abc" -> null when parsed as number, null eq 123 is False (eq_null_safe: one null -> False)
    assert out[0]["eq"] is False
    assert out[1]["eq"] is True
