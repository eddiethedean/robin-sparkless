"""
Tests for issue #257: order_by does not accept Column.desc_nulls_last() result (PySortOrder).

PySpark supports df.orderBy(F.col("value").desc_nulls_last()). Robin's order_by previously
only accepted list[str]; it now accepts a single SortOrder or list of SortOrder for parity.
"""

from __future__ import annotations
import pytest

from tests.fixtures.spark_imports import get_spark_imports

_imports = get_spark_imports()
F = _imports.F


@pytest.mark.skip(reason="Issue #1200: unskip when fixing")
def test_order_by_single_desc_nulls_last(spark) -> None:
    """df.order_by(col("value").desc_nulls_last()) works and puts nulls last (PySpark parity)."""
    data = [
        {"value": "A"},
        {"value": "B"},
        {"value": None},
        {"value": "C"},
        {"value": "D"},
    ]
    df = spark.createDataFrame(data, ["value"])
    out = df.orderBy(F.col("value").desc_nulls_last()).collect()
    assert len(out) == 5
    values = [r["value"] for r in out]
    # Desc with nulls last: D, C, B, A, null
    assert values == ["D", "C", "B", "A", None]


def test_order_by_list_of_sort_orders(spark) -> None:
    """df.order_by([col("a").asc(), col("b").desc_nulls_last()]) works."""
    data = [{"a": 1, "b": 10}, {"a": 1, "b": 20}, {"a": 2, "b": 5}]
    df = spark.createDataFrame(data, ["a", "b"])
    out = df.orderBy([F.col("a").asc(), F.col("b").desc_nulls_last()]).collect()
    assert len(out) == 3
    # a asc, then b desc: (1,20), (1,10), (2,5)
    assert [r["a"] for r in out] == [1, 1, 2]
    assert [r["b"] for r in out] == [20, 10, 5]


def test_order_by_column_names_unchanged(spark) -> None:
    """df.order_by([\"col1\", \"col2\"]) and order_by(list, ascending) still work."""
    data = [{"x": 3}, {"x": 1}, {"x": 2}]
    df = spark.createDataFrame(data, ["x"])
    out = df.orderBy(["x"]).collect()
    assert [r["x"] for r in out] == [1, 2, 3]
    out_desc = df.orderBy(["x"], ascending=False).collect()
    assert [r["x"] for r in out_desc] == [3, 2, 1]
