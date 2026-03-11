"""Regression test for #1411: array literal of simple values after range()."""

from tests.fixtures.spark_imports import get_spark_imports


def test_array_literal_after_range(spark):
    imports = get_spark_imports()
    F = imports.F

    df = spark.range(1, 2).select(F.array(F.lit("a"), F.lit("b")).alias("arr"))

    rows = df.collect()
    assert len(rows) == 1
    arr = rows[0]["arr"]
    assert arr == ["a", "b"]
