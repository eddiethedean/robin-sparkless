"""
Tests for issue #254: F.split(column, pattern, limit) — limit parameter not supported.

PySpark supports F.split(col, pattern, limit) with an optional third argument to limit
the number of splits. Robin now accepts the same signature; e.g. split(col("s"), ",", 2)
on "a,b,c" yields ['a', 'b,c'].
"""

from __future__ import annotations

from tests.fixtures.spark_imports import get_spark_imports
from tests.python.utils import _row_to_dict


_imports = get_spark_imports()
SparkSession = _imports.SparkSession
F = _imports.F


def test_split_with_limit_two_parts() -> None:
    """F.split(col('s'), ',', 2) on 'a,b,c' yields ['a', 'b,c']."""
    spark = SparkSession.builder.appName("split_limit").getOrCreate()
    df = spark.createDataFrame([{"s": "a,b,c"}], ["s"])
    out = df.select(F.split(F.col("s"), ",", 2)).collect()
    assert len(out) == 1
    row = _row_to_dict(out[0])
    parts = [v for _, v in row.items() if isinstance(v, list)]
    assert len(parts) == 1
    assert parts[0] == ["a", "b,c"]


def test_split_without_limit_unchanged() -> None:
    """F.split(col('s'), ',') without limit yields all parts."""
    spark = SparkSession.builder.appName("split_limit").getOrCreate()
    df = spark.createDataFrame([{"s": "a,b,c"}], ["s"])
    out = df.select(F.split(F.col("s"), ",")).collect()
    assert len(out) == 1
    row = _row_to_dict(out[0])
    parts = [v for _, v in row.items() if isinstance(v, list)]
    assert len(parts) == 1
    assert parts[0] == ["a", "b", "c"]


