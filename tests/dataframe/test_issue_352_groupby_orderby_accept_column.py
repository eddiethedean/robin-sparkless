"""
Tests for #352: group_by and order_by accept Column expressions (PySpark parity).

PySpark accepts both str and Column for groupBy(), orderBy(), and agg column arguments.
"""

from __future__ import annotations
import pytest

from tests.fixtures.spark_imports import get_spark_imports

_imports = get_spark_imports()
F = _imports.F
def test_group_by_column_then_agg_sum(spark) -> None:
    """group_by(col(\"dept\")) and agg(sum(col(\"salary\"))) works (PySpark parity)."""
    data = [
        {"dept": "A", "salary": 100},
        {"dept": "A", "salary": 200},
        {"dept": "B", "salary": 150},
    ]
    df = spark.createDataFrame(data, ["dept", "salary"])
    gd = df.groupBy(F.col("dept"))
    result = gd.agg(F.sum(F.col("salary")).alias("total")).collect()
    result.sort(key=lambda r: r["dept"])
    assert len(result) == 2
    assert result[0]["dept"] == "A" and result[0]["total"] == 300
    assert result[1]["dept"] == "B" and result[1]["total"] == 150


def test_group_by_list_of_columns(spark) -> None:
    """group_by([col(\"a\"), col(\"b\")]) works."""
    data = [
        {"a": 1, "b": 10, "v": 100},
        {"a": 1, "b": 10, "v": 200},
        {"a": 1, "b": 20, "v": 50},
    ]
    df = spark.createDataFrame(data)
    gd = df.groupBy([F.col("a"), F.col("b")])
    result = gd.agg(F.sum(F.col("v")).alias("total")).collect()
    assert len(result) == 2
    totals = {(r["a"], r["b"]): r["total"] for r in result}
    assert totals[(1, 10)] == 300 and totals[(1, 20)] == 50


def test_group_by_single_str_still_works(spark) -> None:
    """group_by(\"dept\") and group_by([\"dept\"]) still work."""
    data = [{"dept": "A", "n": 1}, {"dept": "A", "n": 2}]
    df = spark.createDataFrame(data)
    gd1 = df.groupBy("dept")
    from tests.utils import _row_to_dict, assert_rows_equal

    rows1 = [_row_to_dict(r) for r in gd1.agg(F.sum(F.col("n")).alias("s")).collect()]
    assert_rows_equal(rows1, [{"dept": "A", "s": 3}], order_matters=True)
    gd2 = df.groupBy(["dept"])
    rows2 = [_row_to_dict(r) for r in gd2.agg(F.sum(F.col("n")).alias("s")).collect()]
    assert_rows_equal(rows2, [{"dept": "A", "s": 3}], order_matters=True)


def test_order_by_column_ascending(spark) -> None:
    """order_by(col(\"x\")) sorts by x ascending (PySpark orderBy(col(\"x\")) parity)."""
    data = [{"x": 3}, {"x": 1}, {"x": 2}]
    df = spark.createDataFrame(data)
    out = df.orderBy(F.col("x")).collect()
    assert [r["x"] for r in out] == [1, 2, 3]


def test_order_by_list_of_columns(spark) -> None:
    """order_by([col(\"a\"), col(\"b\")]) sorts by a then b ascending."""
    data = [{"a": 2, "b": 1}, {"a": 1, "b": 2}, {"a": 1, "b": 1}]
    df = spark.createDataFrame(data)
    out = df.orderBy([F.col("a"), F.col("b")]).collect()
    assert (out[0]["a"], out[0]["b"]) == (1, 1)
    assert (out[1]["a"], out[1]["b"]) == (1, 2)
    assert (out[2]["a"], out[2]["b"]) == (2, 1)
