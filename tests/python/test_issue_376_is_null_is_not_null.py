"""Tests for #376: Column.isNull() and isNotNull() (PySpark naming)."""

from __future__ import annotations

from tests.python.utils import get_functions, get_spark

F = get_functions()


def _spark():
    return get_spark("issue_376")


def test_is_null_and_is_not_null_snake() -> None:
    """is_null() and is_not_null() work and can be used in filter."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"a": 1, "b": "x"}, {"a": None, "b": "y"}, {"a": 3, "b": None}],
        schema="a int, b string",
    )
    null_a = df.filter(F.col("a").isNull())
    rows = null_a.collect()
    assert len(rows) == 1
    assert rows[0]["a"] is None
    non_null_a = df.filter(F.col("a").isNotNull())
    assert len(non_null_a.collect()) == 2


def test_is_null_and_is_not_null_camel() -> None:
    """isNull() and isNotNull() (PySpark naming) work the same."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"a": 1}, {"a": None}],
        schema=["a"],
    )
    null_a = df.filter(F.col("a").isNull())
    assert len(null_a.collect()) == 1
    non_null_a = df.filter(F.col("a").isNotNull())
    assert len(non_null_a.collect()) == 1
