"""Tests for #405: Column pow (**) and bitwise NOT (~) (PySpark)."""

from __future__ import annotations

import pytest
from pyspark.sql import functions as F


def test_pow_column_literal(spark) -> None:
    """col("x") ** 2 and pow(col("x"), 2) give squared values."""
    df = spark.createDataFrame([(3,), (5,)], ["x"])
    out = df.select((F.col("x") ** 2).alias("sq")).collect()
    rows = list(out)
    assert len(rows) == 2
    assert rows[0]["sq"] == 9
    assert rows[1]["sq"] == 25

    out2 = df.select(F.pow(F.col("x"), F.lit(2)).alias("sq")).collect()
    rows2 = list(out2)
    assert rows2[0]["sq"] == 9
    assert rows2[1]["sq"] == 25


def test_bitwise_not(spark) -> None:
    """PySpark: `~col` is boolean NOT; use SQL `~` for bitwise."""
    df = spark.createDataFrame([(0,), (1,), (5,)], ["x"])

    # In PySpark, `~` on a Column maps to boolean NOT, so ints are a type error.
    from pyspark.errors.exceptions.captured import AnalysisException

    with pytest.raises(AnalysisException):
        df.select((~F.col("x")).alias("inv")).collect()

    # Bitwise NOT for integers is available via SQL expression.
    rows = df.select(F.expr("~x").alias("inv")).collect()
    assert [r["inv"] for r in rows] == [-1, -2, -6]
