"""ANSI semantics tests for PySpark 4.0 compat profile."""

from __future__ import annotations

import pytest

pytest_plugins = ["sparkless.testing"]


def _set_compat(spark, profile: str) -> None:
    spark.conf.set("sparkless.pyspark.compat", profile)


@pytest.mark.sparkless_only
def test_divide_by_zero_null_on_3_5(spark):
    _set_compat(spark, "3.5")
    df = spark.createDataFrame([(10, 0)], ["a", "b"])
    row = df.select((df.a / df.b).alias("q")).collect()[0]
    assert row["q"] is None


@pytest.mark.pyspark4_only
@pytest.mark.sparkless_only
def test_divide_by_zero_raises_on_4_0(spark):
    _set_compat(spark, "4.0")
    df = spark.createDataFrame([(10, 0)], ["a", "b"])
    with pytest.raises(Exception) as exc_info:
        df.select((df.a / df.b).alias("q")).collect()
    assert "divide" in str(exc_info.value).lower() or "ARITHMETIC" in str(
        exc_info.value
    )


@pytest.mark.sparkless_only
def test_invalid_cast_null_on_3_5(spark, spark_imports):
    _set_compat(spark, "3.5")
    col = spark_imports.F.col

    df = spark.createDataFrame([("not-a-number",)], ["s"])
    row = df.select(col("s").cast("int").alias("n")).collect()[0]
    assert row["n"] is None


@pytest.mark.pyspark4_only
@pytest.mark.sparkless_only
def test_invalid_cast_raises_on_4_0(spark, spark_imports):
    _set_compat(spark, "4.0")
    col = spark_imports.F.col

    df = spark.createDataFrame([("not-a-number",)], ["s"])
    with pytest.raises(Exception):
        df.select(col("s").cast("int").alias("n")).collect()
