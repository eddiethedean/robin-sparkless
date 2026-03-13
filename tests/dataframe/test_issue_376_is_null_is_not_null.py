"""Tests for #376: Column.isNull() and isNotNull() (PySpark naming)."""

from __future__ import annotations

from sparkless.testing import get_imports

_imports = get_imports()
F = _imports.F


def test_is_null_and_is_not_null_snake(spark) -> None:
    """is_null() and is_not_null() work and can be used in filter."""
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


def test_is_null_and_is_not_null_camel(spark) -> None:
    """isNull() and isNotNull() (PySpark naming) work the same."""
    df = spark.createDataFrame(
        [{"a": 1}, {"a": None}],
        schema=["a"],
    )
    null_a = df.filter(F.col("a").isNull())
    assert len(null_a.collect()) == 1
    non_null_a = df.filter(F.col("a").isNotNull())
    assert len(non_null_a.collect()) == 1
