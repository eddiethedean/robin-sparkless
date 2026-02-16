"""Tests for #379: Column.replace() accept dict and list (PySpark parity)."""

from __future__ import annotations

import robin_sparkless as rs
from robin_sparkless import col


def _spark():
    return rs.SparkSession.builder().app_name("issue_379").get_or_create()


def test_replace_single_pair() -> None:
    """replace(search, replacement) works as before."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"x": "a-b-c"}],
        schema=[("x", "string")],
    )
    out = df.with_column("y", col("x").replace("-", "_"))
    rows = out.collect()
    assert rows[0]["y"] == "a_b_c"


def test_replace_dict() -> None:
    """replace({old1: new1, old2: new2}) applies multiple replacements."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"x": "foo bar baz"}],
        schema=[("x", "string")],
    )
    out = df.with_column("y", col("x").replace({"foo": "a", "bar": "b", "baz": "c"}))
    rows = out.collect()
    assert rows[0]["y"] == "a b c"


def test_replace_list_of_tuples() -> None:
    """replace([(old1, new1), (old2, new2)]) applies replacements in order."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"x": "hello world"}],
        schema=[("x", "string")],
    )
    out = df.with_column("y", col("x").replace([("hello", "hi"), ("world", "earth")]))
    rows = out.collect()
    assert rows[0]["y"] == "hi earth"
