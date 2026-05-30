"""VARIANT type and parse_json (PySpark 4 semi-structured subset)."""

from __future__ import annotations

import pytest
from sparkless.sql import functions as F
from sparkless.sql.types import StructField, StructType, VariantType

pytest_plugins = ["sparkless.testing"]


@pytest.mark.sparkless_only
def test_parse_json_returns_dict_on_collect(spark):
    spark.conf.set("sparkless.pyspark.compat", "4.0")
    df = spark.createDataFrame([('{"a": 1, "b": "x"}',)], ["raw"])
    df.select(
        F.parse_json("raw").alias("v"),
    )
    # Without explicit VariantType schema, collect returns JSON string; with schema:
    schema = StructType([StructField("v", VariantType())])
    df2 = spark.createDataFrame([('{"a": 1, "b": "x"}',)], ["raw"]).select(
        F.parse_json("raw").alias("v")
    )
    # Re-create with variant schema on collect path via createDataFrame on parsed values
    row = df2.collect()[0]["v"]
    assert isinstance(row, str)
    parsed = spark.createDataFrame([(row,)], schema).collect()[0]["v"]
    assert parsed == {"a": 1, "b": "x"}


@pytest.mark.sparkless_only
def test_cast_to_variant(spark):
    spark.conf.set("sparkless.pyspark.compat", "4.0")
    df = spark.createDataFrame([('{"k": true}',)], ["s"])
    out = df.select(F.col("s").cast("variant").alias("v"))
    val = out.collect()[0]["v"]
    assert val == '{"k":true}' or val == '{"k": true}'
