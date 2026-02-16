"""
Tests for #417: createDataFrame accept Row and namedtuple as data (PySpark parity).
"""

from __future__ import annotations

import collections

import robin_sparkless as rs


def _spark() -> rs.SparkSession:
    return rs.SparkSession.builder().app_name("issue_417").get_or_create()


def test_create_dataframe_from_namedtuple() -> None:
    """spark.createDataFrame(list of namedtuple) works."""
    spark = _spark()
    RowLike = collections.namedtuple("RowLike", "a b")
    rows = [RowLike(1, 3), RowLike(2, 4)]
    df = spark.createDataFrame(rows)
    out = df.collect()
    assert len(out) == 2
    assert out[0] == {"a": 1, "b": 3}
    assert out[1] == {"a": 2, "b": 4}


def test_create_dataframe_from_row_like_with_schema() -> None:
    """createDataFrame with schema and Row-like (get_item by name) works."""
    spark = _spark()
    RowLike = collections.namedtuple("RowLike", "x y")
    rows = [RowLike(10, "foo"), RowLike(20, "bar")]
    df = spark.createDataFrame(rows, schema=[("x", "bigint"), ("y", "string")])
    out = df.collect()
    assert len(out) == 2
    assert out[0] == {"x": 10, "y": "foo"}
    assert out[1] == {"x": 20, "y": "bar"}
