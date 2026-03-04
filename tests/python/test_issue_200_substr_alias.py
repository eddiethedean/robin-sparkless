"""
Tests for issue #200: substr/substring with alias — expression name not found.

Select with col("name").substr(1, 3).alias("partial") must not raise
RuntimeError: not found: partial. The alias name is an output column name,
not an input column to resolve.
"""

from __future__ import annotations

from tests.python.utils import get_functions, get_spark, _row_to_dict, assert_rows_equal

F = get_functions()


def test_substr_alias_select_collect() -> None:
    """select(col('name').substr(1, 3).alias('partial')) returns column 'partial' (Sparkless parity)."""
    spark = get_spark("test_issue_200")
    df = spark.createDataFrame(
        [{"name": "hello"}],
        ["name"],
    )
    result = df.select(F.col("name").substr(1, 3).alias("partial"))
    rows = result.collect()
    cols = result.columns if isinstance(getattr(result, "columns", None), list) else result.columns()
    assert cols == ["partial"]
    assert_rows_equal([_row_to_dict(r) for r in rows], [{"partial": "hel"}])


def test_substr_alias_multiple_rows() -> None:
    """substr with alias over multiple rows."""
    spark = get_spark("test_issue_200")
    df = spark.createDataFrame(
        [{"name": "abc"}, {"name": "xyz"}, {"name": "hi"}],
        ["name"],
    )
    rows = df.select(F.col("name").substr(1, 2).alias("partial")).collect()
    assert_rows_equal([_row_to_dict(r) for r in rows], [{"partial": "ab"}, {"partial": "xy"}, {"partial": "hi"}])


def test_substr_alias_chained_with_other_expr() -> None:
    """select with substr alias and another column (chained operations)."""
    spark = get_spark("test_issue_200")
    df = spark.createDataFrame(
        [("hello", 1)],
        ["s", "n"],
    )
    rows = df.select(
        F.col("s").substr(1, 3).alias("partial"),
        F.col("n"),
    ).collect()
    assert_rows_equal([_row_to_dict(r) for r in rows], [{"partial": "hel", "n": 1}])
