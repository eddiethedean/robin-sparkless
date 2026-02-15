"""
Tests for #352: group_by and order_by accept Column expressions (PySpark parity).

PySpark accepts both str and Column for groupBy(), orderBy(), and agg column arguments.
"""

from __future__ import annotations

import robin_sparkless as rs


def _spark() -> rs.SparkSession:
    return rs.SparkSession.builder().app_name("issue_352").get_or_create()


def test_group_by_column_then_agg_sum() -> None:
    """group_by(col("dept")) and agg(sum(col("salary"))) works (PySpark parity)."""
    spark = _spark()
    data = [
        {"dept": "A", "salary": 100},
        {"dept": "A", "salary": 200},
        {"dept": "B", "salary": 150},
    ]
    schema = [("dept", "string"), ("salary", "int")]
    df = spark.createDataFrame(data, schema)
    gd = df.group_by(rs.col("dept"))
    result = gd.agg([rs.sum(rs.col("salary")).alias("total")]).collect()
    result.sort(key=lambda r: r["dept"])
    assert len(result) == 2
    assert result[0]["dept"] == "A" and result[0]["total"] == 300
    assert result[1]["dept"] == "B" and result[1]["total"] == 150


def test_group_by_list_of_columns() -> None:
    """group_by([col("a"), col("b")]) works."""
    spark = _spark()
    data = [
        {"a": 1, "b": 10, "v": 100},
        {"a": 1, "b": 10, "v": 200},
        {"a": 1, "b": 20, "v": 50},
    ]
    df = spark.createDataFrame(data)
    gd = df.group_by([rs.col("a"), rs.col("b")])
    result = gd.agg([rs.sum(rs.col("v")).alias("total")]).collect()
    assert len(result) == 2
    totals = {(r["a"], r["b"]): r["total"] for r in result}
    assert totals[(1, 10)] == 300 and totals[(1, 20)] == 50


def test_group_by_single_str_still_works() -> None:
    """group_by("dept") and group_by(["dept"]) still work."""
    spark = _spark()
    data = [{"dept": "A", "n": 1}, {"dept": "A", "n": 2}]
    df = spark.createDataFrame(data)
    gd1 = df.group_by("dept")
    assert gd1.agg([rs.sum(rs.col("n")).alias("s")]).collect() == [
        {"dept": "A", "s": 3}
    ]
    gd2 = df.group_by(["dept"])
    assert gd2.agg([rs.sum(rs.col("n")).alias("s")]).collect() == [
        {"dept": "A", "s": 3}
    ]


def test_order_by_column_ascending() -> None:
    """order_by(col("x")) sorts by x ascending (PySpark orderBy(col("x")) parity)."""
    spark = _spark()
    data = [{"x": 3}, {"x": 1}, {"x": 2}]
    df = spark.createDataFrame(data)
    out = df.order_by(rs.col("x")).collect()
    assert [r["x"] for r in out] == [1, 2, 3]


def test_order_by_list_of_columns() -> None:
    """order_by([col("a"), col("b")]) sorts by a then b ascending."""
    spark = _spark()
    data = [{"a": 2, "b": 1}, {"a": 1, "b": 2}, {"a": 1, "b": 1}]
    df = spark.createDataFrame(data)
    out = df.order_by([rs.col("a"), rs.col("b")]).collect()
    assert (out[0]["a"], out[0]["b"]) == (1, 1)
    assert (out[1]["a"], out[1]["b"]) == (1, 2)
    assert (out[2]["a"], out[2]["b"]) == (2, 1)
