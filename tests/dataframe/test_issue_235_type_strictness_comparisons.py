"""
Tests for issue #235: Type strictness in string vs numeric comparisons.

These tests now assert native PySpark behavior directly using the ``spark``
fixture and backend-agnostic get_functions().
"""

from __future__ import annotations
import pytest

from tests.fixtures.spark_imports import get_spark_imports

_imports = get_spark_imports()
F = _imports.F
def test_string_eq_numeric_literal_in_filter(spark) -> None:
    """col('str_col') == lit(123) in filter returns matching row (PySpark parity)."""
    data = [{"str_col": "123"}, {"str_col": "456"}]
    df = spark.createDataFrame(data, schema=["str_col"])

    out = df.filter(F.col("str_col") == F.lit(123)).collect()
    assert [r.asDict() for r in out] == [{"str_col": "123"}]
def test_string_gt_numeric_literal_uses_numeric_semantics(spark) -> None:
    """Ordering comparison uses numeric semantics, not string lexicographic order."""
    data = [{"str_col": "123"}, {"str_col": "456"}]
    df = spark.createDataFrame(data, schema=["str_col"])

    out = df.filter(F.col("str_col") > F.lit(200)).collect()
    assert [r.asDict() for r in out] == [{"str_col": "456"}]
def test_string_eq_numeric_literal_with_invalid_string_is_non_matching(spark) -> None:
    """Invalid numeric strings behave as non-matching (null) under numeric comparison."""
    data = [{"str_col": "abc"}, {"str_col": "123"}]
    df = spark.createDataFrame(data, schema=["str_col"])

    out = df.filter(F.col("str_col") == F.lit(123)).collect()
    assert [r.asDict() for r in out] == [{"str_col": "123"}]
def test_literal_eq_string_column_symmetric_form(spark) -> None:
    """Symmetric literal == column form also uses numeric coercion."""
    data = [{"str_col": "123"}, {"str_col": "456"}]
    df = spark.createDataFrame(data, schema=["str_col"])

    out = df.filter(F.lit(123) == F.col("str_col")).collect()
    assert [r.asDict() for r in out] == [{"str_col": "123"}]
