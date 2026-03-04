"""Tests for issue #401: filter condition accept Column expression."""

from __future__ import annotations

from tests.python.utils import get_functions, get_spark, _row_to_dict, assert_rows_equal

F = get_functions()


def test_filter_column_gt_literal() -> None:
    """df.filter(col('x') > 1) - PySpark accepts Column expression."""
    spark = get_spark("issue_401")
    df = spark.createDataFrame([(1,), (2,)], ["x"])
    result = df.filter(F.col("x") > 1).collect()
    assert_rows_equal([_row_to_dict(r) for r in result], [{"x": 2}], order_matters=True)


def test_filter_column_lt_literal() -> None:
    """df.filter(col('x') < 2)."""
    spark = get_spark("issue_401")
    df = spark.createDataFrame([(1,), (2,)], ["x"])
    result = df.filter(F.col("x") < 2).collect()
    assert_rows_equal([_row_to_dict(r) for r in result], [{"x": 1}], order_matters=True)


def test_filter_column_eq_literal() -> None:
    """df.filter(col('x') == 2)."""
    spark = get_spark("issue_401")
    df = spark.createDataFrame([(1,), (2,)], ["x"])
    result = df.filter(F.col("x") == 2).collect()
    assert_rows_equal([_row_to_dict(r) for r in result], [{"x": 2}], order_matters=True)
