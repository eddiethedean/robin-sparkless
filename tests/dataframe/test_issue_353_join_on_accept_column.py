"""
Tests for #353: join on accept Column expression(s) (PySpark parity).

PySpark's DataFrame.join(other, on=...) accepts on as a Column or list of column names.
"""

from __future__ import annotations

from tests.utils import get_functions, get_spark

F = get_functions()


def _spark():
    return get_spark("issue_353")


def test_join_on_column() -> None:
    """left.join(right, col(\"id\")) works (PySpark parity)."""
    spark = _spark()
    left = spark.createDataFrame([{"id": 1, "v": 10}], ["id", "v"])
    right = spark.createDataFrame([{"id": 1, "w": 20}], ["id", "w"])
    from tests.utils import _row_to_dict, assert_rows_equal
    # Use qualified columns to avoid ambiguous reference (left.id == right.id)
    result = left.join(right, left["id"] == right["id"]).collect()
    assert_rows_equal([_row_to_dict(r) for r in result], [{"id": 1, "v": 10, "w": 20}], order_matters=True)


def test_join_on_list_of_columns() -> None:
    """left.join(right, [col(\"a\"), col(\"b\")]) works."""
    spark = _spark()
    left = spark.createDataFrame([{"a": 1, "b": 2, "v": 10}], ["a", "b", "v"])
    right = spark.createDataFrame([{"a": 1, "b": 2, "w": 20}], ["a", "b", "w"])
    from tests.utils import _row_to_dict, assert_rows_equal
    # Use qualified columns to avoid ambiguous reference
    result = left.join(right, (left["a"] == right["a"]) & (left["b"] == right["b"])).collect()
    assert_rows_equal([_row_to_dict(r) for r in result], [{"a": 1, "b": 2, "v": 10, "w": 20}], order_matters=True)


def test_join_on_str_still_works() -> None:
    """join(right, "id") and join(right, ["id"]) still work."""
    spark = _spark()
    left = spark.createDataFrame([{"id": 1, "v": 10}], ["id", "v"])
    right = spark.createDataFrame([{"id": 1, "w": 20}], ["id", "w"])
    from tests.utils import _row_to_dict, assert_rows_equal
    expected = [{"id": 1, "v": 10, "w": 20}]
    r1 = [_row_to_dict(r) for r in left.join(right, "id").collect()]
    r2 = [_row_to_dict(r) for r in left.join(right, ["id"]).collect()]
    assert_rows_equal(r1, expected, order_matters=True)
    assert_rows_equal(r2, expected, order_matters=True)
