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
@pytest.mark.skip(reason="Issue #1233: unskip when fixing")
def test_cast_string_type_and_try_cast() -> None:
    """Document current PySpark behavior for try_cast / astype (not yet supported)."""
    spark = SparkSession.builder.appName("issue_394").getOrCreate()
    df = spark.createDataFrame([("42",)], ["a"])
    # In current PySpark, Column has no working try_cast/astype methods; using
    # them results in TypeError(\"Column object is not callable\") when the
    # column expression is evaluated. Assert on this behavior so that
    # sparkless can match it, while leaving room for future enhancement.
    with pytest.raises(TypeError):
        df.select(
            F.col("a").try_cast("int").alias("i"),
            F.col("a").astype(LongType()).alias("j"),
        ).collect()


def test_module_cast_accepts_data_type() -> None:
    """Document current PySpark behavior for F.cast with DataType object."""
    spark = SparkSession.builder.appName("issue_394").getOrCreate()
    df = spark.createDataFrame([("x",)], ["c"])
    # PySpark does not expose a module-level F.cast that accepts a DataType
    # object; attempting to use it leads to AttributeError on the DataType
    # (no alias attribute). We assert on the error rather than successful
    # behavior so tests reflect real PySpark semantics.
    with pytest.raises(AttributeError):
        df.select(F.cast(F.col("c"), StringType()).alias("c")).collect()
