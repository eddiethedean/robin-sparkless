"""Regression test for issue #1399: numeric.ceil schema parity.

Scenario (from the issue, paraphrased):

    df = session.createDataFrame([(1.1,), (-1.9,), (None,)], ["x"])
    df.select(F.ceil("x").alias("out"))

PySpark schema:

    struct<out:bigint>

Sparkless schema (before fix):

    struct<out:double>

This test locks in PySpark parity for the output schema: ceil() should yield
an integral type (bigint in PySpark, LongType in Sparkless).
"""

from __future__ import annotations

from sparkless.sql import SparkSession, functions as F
from sparkless.sql.types import LongType


def test_issue_1399_numeric_ceil_schema_is_long() -> None:
    spark = SparkSession.builder.appName("issue_1399_numeric_ceil_schema").getOrCreate()
    try:
        df = spark.createDataFrame(
            [(1.1,), (-1.9,), (None,)],
            ["x"],
        )
        out = df.select(F.ceil("x").alias("out"))

        # Value semantics: ceil matches PySpark (sanity check).
        rows = [r["out"] for r in out.collect()]
        assert rows == [2, -1, None]

        # Schema parity: ceil outputs an integral type (LongType).
        field = out.schema.fields[0]
        assert isinstance(field.dataType, LongType)
    finally:
        spark.stop()
