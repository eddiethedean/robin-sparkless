"""Tests for issue #394: cast() accept DataType object."""

from __future__ import annotations

import robin_sparkless as rs


class IntegerType:
    """Minimal DataType-like: PySpark IntegerType() has typeName() -> 'int'."""

    def typeName(self) -> str:
        return "int"


class StringType:
    def typeName(self) -> str:
        return "string"


class LongType:
    def typeName(self) -> str:
        return "long"


def test_cast_string_unchanged() -> None:
    """cast with string (e.g. 'int') still works."""
    spark = rs.SparkSession.builder().app_name("issue_394").get_or_create()
    df = spark.createDataFrame([("42",)], ["x"])
    out = df.select(rs.col("x").cast("int").alias("x"))
    row = out.collect()[0]
    assert row["x"] == 42


def test_cast_data_type_object() -> None:
    """cast with DataType-like object (typeName() -> 'int') works."""
    spark = rs.SparkSession.builder().app_name("issue_394").get_or_create()
    df = spark.createDataFrame([("42",)], ["x"])
    out = df.select(rs.col("x").cast(IntegerType()).alias("x"))
    row = out.collect()[0]
    assert row["x"] == 42


def test_cast_string_type_and_try_cast() -> None:
    """try_cast and astype accept string and DataType object."""
    spark = rs.SparkSession.builder().app_name("issue_394").get_or_create()
    df = spark.createDataFrame([("42",)], ["a"])
    out = df.select(
        rs.col("a").try_cast("int").alias("i"),
        rs.col("a").astype(LongType()).alias("j"),
    )
    row = out.collect()[0]
    assert row["i"] == 42
    assert row["j"] == 42


def test_module_cast_accepts_data_type() -> None:
    """Standalone cast(col, dtype) accepts DataType object."""
    spark = rs.SparkSession.builder().app_name("issue_394").get_or_create()
    df = spark.createDataFrame([("x",)], ["c"])
    out = df.select(rs.cast(rs.col("c"), StringType()).alias("c"))
    row = out.collect()[0]
    assert row["c"] == "x"
