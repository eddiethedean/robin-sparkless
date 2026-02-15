"""
Tests for issue #257: order_by does not accept Column.desc_nulls_last() result (PySortOrder).

PySpark supports df.orderBy(F.col("value").desc_nulls_last()). Robin's order_by previously
only accepted list[str]; it now accepts a single SortOrder or list of SortOrder for parity.
"""

from __future__ import annotations

import robin_sparkless as rs


def test_order_by_single_desc_nulls_last() -> None:
    """df.order_by(col("value").desc_nulls_last()) works and puts nulls last (PySpark parity)."""
    spark = rs.SparkSession.builder().app_name("order_by_sort_order").get_or_create()
    data = [
        {"value": "A"},
        {"value": "B"},
        {"value": None},
        {"value": "C"},
        {"value": "D"},
    ]
    df = spark.createDataFrame(data, [("value", "string")])
    out = df.order_by(rs.col("value").desc_nulls_last()).collect()
    assert len(out) == 5
    values = [r["value"] for r in out]
    # Desc with nulls last: D, C, B, A, null
    assert values == ["D", "C", "B", "A", None]


def test_order_by_list_of_sort_orders() -> None:
    """df.order_by([col("a").asc(), col("b").desc_nulls_last()]) works."""
    spark = rs.SparkSession.builder().app_name("order_by_sort_order").get_or_create()
    data = [{"a": 1, "b": 10}, {"a": 1, "b": 20}, {"a": 2, "b": 5}]
    df = spark.createDataFrame(data, [("a", "int"), ("b", "int")])
    out = df.order_by([rs.col("a").asc(), rs.col("b").desc_nulls_last()]).collect()
    assert len(out) == 3
    # a asc, then b desc: (1,20), (1,10), (2,5)
    assert [r["a"] for r in out] == [1, 1, 2]
    assert [r["b"] for r in out] == [20, 10, 5]


def test_order_by_column_names_unchanged() -> None:
    """df.order_by(["col1", "col2"]) and order_by(list, ascending) still work."""
    spark = rs.SparkSession.builder().app_name("order_by_sort_order").get_or_create()
    data = [{"x": 3}, {"x": 1}, {"x": 2}]
    df = spark.createDataFrame(data, [("x", "int")])
    out = df.order_by(["x"]).collect()
    assert [r["x"] for r in out] == [1, 2, 3]
    out_desc = df.order_by(["x"], ascending=[False]).collect()
    assert [r["x"] for r in out_desc] == [3, 2, 1]
