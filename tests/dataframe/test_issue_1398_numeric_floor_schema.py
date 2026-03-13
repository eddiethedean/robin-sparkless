"""Regression test for issue #1398: numeric.floor schema parity.

Scenario (from the issue, paraphrased):

    df = session.createDataFrame([(1.9,), (-1.1,), (None,)], ["x"])
    df.select(F.floor("x").alias("out"))

PySpark schema:

    struct<out:bigint>

Sparkless schema (before fix):

    struct<out:double>

This test locks in PySpark parity for the output schema: floor() should yield
an integral type (bigint in PySpark, LongType in Sparkless).
"""

from __future__ import annotations


def test_issue_1398_numeric_floor_schema_is_long(spark, spark_imports) -> None:
    F = spark_imports.F
    LongType = spark_imports.LongType

    df = spark.createDataFrame(
        [(1.9,), (-1.1,), (None,)],
        ["x"],
    )
    out = df.select(F.floor("x").alias("out"))

    # Value semantics: floor matches PySpark (sanity check).
    rows = [r["out"] for r in out.collect()]
    assert rows == [1, -2, None]

    # Schema parity: floor outputs an integral type. Sparkless uses LongType()
    # with simpleString \"long\"; PySpark reports bigint. We assert LongType
    # here, and higher-level parity harness can remap \"long\" vs \"bigint\".
    field = out.schema.fields[0]
    assert isinstance(field.dataType, LongType)
