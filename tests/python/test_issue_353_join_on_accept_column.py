"""
Tests for #353: join on accept Column expression(s) (PySpark parity).

PySpark's DataFrame.join(other, on=...) accepts on as a Column or list of column names.
"""

from __future__ import annotations

import robin_sparkless as rs


def _spark() -> rs.SparkSession:
    return rs.SparkSession.builder().app_name("issue_353").get_or_create()


def test_join_on_column() -> None:
    """left.join(right, col("id")) works (PySpark parity)."""
    spark = _spark()
    left = spark.createDataFrame([{"id": 1, "v": 10}], [("id", "int"), ("v", "int")])
    right = spark.createDataFrame([{"id": 1, "w": 20}], [("id", "int"), ("w", "int")])
    result = left.join(right, rs.col("id")).collect()
    assert result == [{"id": 1, "v": 10, "w": 20}]


def test_join_on_list_of_columns() -> None:
    """left.join(right, [col("a"), col("b")]) works."""
    spark = _spark()
    left = spark.createDataFrame(
        [{"a": 1, "b": 2, "v": 10}],
        [("a", "int"), ("b", "int"), ("v", "int")],
    )
    right = spark.createDataFrame(
        [{"a": 1, "b": 2, "w": 20}],
        [("a", "int"), ("b", "int"), ("w", "int")],
    )
    result = left.join(right, [rs.col("a"), rs.col("b")]).collect()
    assert result == [{"a": 1, "b": 2, "v": 10, "w": 20}]


def test_join_on_str_still_works() -> None:
    """join(right, "id") and join(right, ["id"]) still work."""
    spark = _spark()
    left = spark.createDataFrame([{"id": 1, "v": 10}], [("id", "int"), ("v", "int")])
    right = spark.createDataFrame([{"id": 1, "w": 20}], [("id", "int"), ("w", "int")])
    r1 = left.join(right, "id").collect()
    r2 = left.join(right, ["id"]).collect()
    assert r1 == r2 == [{"id": 1, "v": 10, "w": 20}]
