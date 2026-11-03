"""
Tests for #353: joins accept Column expression conditions (PySpark parity).

PySpark's DataFrame.join(other, on=...) accepts a Column condition. Condition joins
keep both key columns; passing key names uses the separate coalescing behavior.
"""

from __future__ import annotations

from sparkless_testing import get_imports

_imports = get_imports()
F = _imports.F


def test_join_on_column(spark) -> None:
    """A Column condition works and preserves both key values."""
    left = spark.createDataFrame([{"id": 1, "v": 10}], ["id", "v"])
    right = spark.createDataFrame([{"id": 1, "w": 20}], ["id", "w"])
    # Use qualified columns to avoid ambiguous reference (left.id == right.id)
    result = left.join(right, left["id"] == right["id"]).collect()
    assert [tuple(row.values()) if hasattr(row, "values") else tuple(row) for row in result] == [
        (1, 10, 1, 20)
    ]


def test_join_on_list_of_columns(spark) -> None:
    """A compound Column condition retains both sides of each join key."""
    left = spark.createDataFrame([{"a": 1, "b": 2, "v": 10}], ["a", "b", "v"])
    right = spark.createDataFrame([{"a": 1, "b": 2, "w": 20}], ["a", "b", "w"])
    # Use qualified columns to avoid ambiguous reference
    result = left.join(
        right, (left["a"] == right["a"]) & (left["b"] == right["b"])
    ).collect()
    assert [tuple(row.values()) if hasattr(row, "values") else tuple(row) for row in result] == [
        (1, 2, 10, 1, 2, 20)
    ]


def test_join_on_str_still_works(spark) -> None:
    """join(right, "id") and join(right, ["id"]) still work."""
    left = spark.createDataFrame([{"id": 1, "v": 10}], ["id", "v"])
    right = spark.createDataFrame([{"id": 1, "w": 20}], ["id", "w"])
    from tests.utils import _row_to_dict, assert_rows_equal

    expected = [{"id": 1, "v": 10, "w": 20}]
    r1 = [_row_to_dict(r) for r in left.join(right, "id").collect()]
    r2 = [_row_to_dict(r) for r in left.join(right, ["id"]).collect()]
    assert_rows_equal(r1, expected, order_matters=True)
    assert_rows_equal(r2, expected, order_matters=True)
