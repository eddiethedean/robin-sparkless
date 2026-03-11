"""Tests for #1359: split(col, pattern, limit) PySpark parity.

F.split() accepts an optional limit argument; limit=-1 means no limit.
Verifies that sparkless matches PySpark behavior for split with limit.
"""

from __future__ import annotations

from tests.fixtures.spark_imports import get_spark_imports
from tests.utils import _row_to_dict

_imports = get_spark_imports()
SparkSession = _imports.SparkSession
F = _imports.F


def test_split_with_limit_parity_1359(spark) -> None:
    """split(col, ',', limit=2) yields at most 2 parts; remainder in last (PySpark parity #1359)."""
    df = spark.createDataFrame([{"s": "a,b,c,d"}], ["s"])
    out = df.select(F.split(F.col("s"), ",", 2).alias("arr")).collect()
    assert len(out) == 1
    row = _row_to_dict(out[0])
    assert row["arr"] == ["a", "b,c,d"]


def test_split_limit_minus_one_no_limit_1359(spark) -> None:
    """split(col, ',', limit=-1) yields all parts (no limit, PySpark parity #1359)."""
    df = spark.createDataFrame([{"s": "x,y,z"}], ["s"])
    out = df.select(F.split(F.col("s"), ",", -1).alias("arr")).collect()
    assert len(out) == 1
    row = _row_to_dict(out[0])
    assert row["arr"] == ["x", "y", "z"]
