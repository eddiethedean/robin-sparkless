"""DayTimeInterval collect shapes (PYSPARK_YM_INTERVAL_LEGACY / compat profile)."""

from __future__ import annotations

from datetime import timedelta

import pytest
from sparkless.sql.types import DayTimeIntervalType, StructField, StructType

pytest_plugins = ["sparkless.testing"]

# 1 day in microseconds (PySpark internal representation)
ONE_DAY_MICROS = 86_400_000_000


@pytest.mark.sparkless_only
def test_dt_interval_legacy_int_on_3_5(spark):
    spark.conf.set("sparkless.pyspark.compat", "3.5")
    schema = StructType([StructField("dt", DayTimeIntervalType())])
    df = spark.createDataFrame([(ONE_DAY_MICROS,)], schema)
    val = df.collect()[0]["dt"]
    assert val == ONE_DAY_MICROS
    assert not isinstance(val, timedelta)


@pytest.mark.sparkless_only
def test_dt_interval_timedelta_on_4_0(spark):
    spark.conf.set("sparkless.pyspark.compat", "4.0")
    schema = StructType([StructField("dt", DayTimeIntervalType())])
    df = spark.createDataFrame([(ONE_DAY_MICROS,)], schema)
    val = df.collect()[0]["dt"]
    assert isinstance(val, timedelta)
    assert val == timedelta(days=1)


@pytest.mark.sparkless_only
def test_dt_interval_legacy_env_overrides_4_0(spark, monkeypatch):
    monkeypatch.setenv("PYSPARK_YM_INTERVAL_LEGACY", "1")
    spark.conf.set("sparkless.pyspark.compat", "4.0")
    schema = StructType([StructField("dt", DayTimeIntervalType())])
    df = spark.createDataFrame([(ONE_DAY_MICROS,)], schema)
    val = df.collect()[0]["dt"]
    assert val == ONE_DAY_MICROS
