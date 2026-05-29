"""YearMonthInterval collect shapes (PYSPARK_YM_INTERVAL_LEGACY / compat profile)."""

from __future__ import annotations

import pytest
from sparkless.sql.types import (
    StructField,
    StructType,
    YearMonthInterval,
    YearMonthIntervalType,
)

pytest_plugins = ["sparkless.testing"]


@pytest.mark.sparkless_only
def test_ym_interval_legacy_int_on_3_5(spark):
    spark.conf.set("sparkless.pyspark.compat", "3.5")
    schema = StructType([StructField("ym", YearMonthIntervalType())])
    df = spark.createDataFrame([(14,)], schema)
    val = df.collect()[0]["ym"]
    assert val == 14
    assert not isinstance(val, YearMonthInterval)


@pytest.mark.sparkless_only
def test_ym_interval_object_on_4_0(spark):
    spark.conf.set("sparkless.pyspark.compat", "4.0")
    schema = StructType([StructField("ym", YearMonthIntervalType())])
    df = spark.createDataFrame([(14,)], schema)
    val = df.collect()[0]["ym"]
    assert isinstance(val, YearMonthInterval)
    assert val.months == 14


@pytest.mark.sparkless_only
def test_ym_interval_legacy_env_overrides_4_0(spark, monkeypatch):
    monkeypatch.setenv("PYSPARK_YM_INTERVAL_LEGACY", "1")
    spark.conf.set("sparkless.pyspark.compat", "4.0")
    schema = StructType([StructField("ym", YearMonthIntervalType())])
    df = spark.createDataFrame([(14,)], schema)
    val = df.collect()[0]["ym"]
    assert val == 14
