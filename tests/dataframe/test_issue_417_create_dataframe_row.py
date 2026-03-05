"""
Tests for #417: createDataFrame accept Row and namedtuple as data (PySpark parity).
"""

from __future__ import annotations

import collections

from tests.utils import get_spark


def _spark():
    return get_spark("issue_417")


def test_create_dataframe_from_namedtuple() -> None:
    """spark.createDataFrame(list of namedtuple) works."""
    spark = _spark()
    RowLike = collections.namedtuple("RowLike", "a b")
    rows = [RowLike(1, 3), RowLike(2, 4)]
    df = spark.createDataFrame(rows)
    out = df.collect()
    assert len(out) == 2
    assert out[0]["a"] == 1 and out[0]["b"] == 3
    assert out[1]["a"] == 2 and out[1]["b"] == 4


def test_create_dataframe_from_row_like_with_schema() -> None:
    """createDataFrame with schema and Row-like (get_item by name) works."""
    spark = _spark()
    RowLike = collections.namedtuple("RowLike", "x y")
    rows = [RowLike(10, "foo"), RowLike(20, "bar")]
    df = spark.createDataFrame(rows, schema=["x", "y"])
    out = df.collect()
    assert len(out) == 2
    assert out[0]["x"] == 10 and out[0]["y"] == "foo"
    assert out[1]["x"] == 20 and out[1]["y"] == "bar"
