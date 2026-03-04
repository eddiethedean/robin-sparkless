"""Tests for issue #394: cast() accept DataType object."""

from __future__ import annotations

import pytest

from tests.fixtures.spark_imports import get_spark_imports


_imports = get_spark_imports()
SparkSession = _imports.SparkSession
F = _imports.F
IntegerType = _imports.IntegerType
StringType = _imports.StringType
LongType = _imports.LongType


def test_cast_string_unchanged() -> None:
    """cast with string (e.g. 'int') still works."""
    spark = SparkSession.builder.appName("issue_394").getOrCreate()
    df = spark.createDataFrame([("42",)], ["x"])
    out = df.select(F.col("x").cast("int").alias("x"))
    row = out.collect()[0]
    assert row["x"] == 42


def test_cast_data_type_object() -> None:
    """cast with DataType-like object (typeName() -> 'int') works."""
    spark = SparkSession.builder.appName("issue_394").getOrCreate()
    df = spark.createDataFrame([("42",)], ["x"])
    out = df.select(F.col("x").cast(IntegerType()).alias("x"))
    row = out.collect()[0]
    assert row["x"] == 42


def test_cast_string_type_and_try_cast() -> None:
    """try_cast and astype accept string and DataType object."""
    spark = SparkSession.builder.appName("issue_394").getOrCreate()
    df = spark.createDataFrame([("42",)], ["a"])
    out = df.select(
        F.col("a").try_cast("int").alias("i"),
        F.col("a").astype(LongType()).alias("j"),
    )
    row = out.collect()[0]
    assert row["i"] == 42
    assert row["j"] == 42


def test_module_cast_accepts_data_type() -> None:
    """Standalone cast(col, dtype) accepts DataType object."""
    spark = SparkSession.builder.appName("issue_394").getOrCreate()
    df = spark.createDataFrame([("x",)], ["c"])
    out = df.select(F.cast(F.col("c"), StringType()).alias("c"))
    row = out.collect()[0]
    assert row["c"] == "x"
