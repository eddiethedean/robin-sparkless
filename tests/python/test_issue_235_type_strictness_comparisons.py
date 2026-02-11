"""
Tests for issue #235: Type strictness in string vs numeric comparisons.

With robin-sparkless 0.6.0, comparing a string column to a numeric literal in
filter raised `RuntimeError: cannot compare string with numeric type (i32)`.
PySpark coerces types and returns matching rows instead.
"""

from __future__ import annotations


def test_string_eq_numeric_literal_in_filter() -> None:
    """col('str_col') == lit(123) in filter returns matching row (PySpark parity)."""
    import robin_sparkless as rs

    F = rs
    spark = F.SparkSession.builder().app_name("repro").get_or_create()
    data = [{"str_col": "123"}, {"str_col": "456"}]
    schema = [("str_col", "string")]
    df = spark._create_dataframe_from_rows(data, schema)

    out = df.filter(F.col("str_col") == F.lit(123)).collect()
    assert out == [{"str_col": "123"}]


def test_string_gt_numeric_literal_uses_numeric_semantics() -> None:
    """Ordering comparison uses numeric semantics, not string lexicographic order."""
    import robin_sparkless as rs

    F = rs
    spark = F.SparkSession.builder().app_name("repro").get_or_create()
    data = [{"str_col": "123"}, {"str_col": "456"}]
    schema = [("str_col", "string")]
    df = spark._create_dataframe_from_rows(data, schema)

    out = df.filter(F.col("str_col") > F.lit(200)).collect()
    assert out == [{"str_col": "456"}]
