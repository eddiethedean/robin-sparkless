"""Regression test for #1412: array of struct built from literals inside select."""

from sparkless.testing import get_imports


def test_array_struct_in_array_with_range(spark):
    imports = get_imports()
    F = imports.F

    df = spark.range(1, 2).select(
        F.array(F.struct(F.lit(1).alias("x"), F.lit("a").alias("s"))).alias("arr")
    )

    rows = df.collect()
    assert len(rows) == 1
    arr = rows[0]["arr"]
    assert len(arr) == 1
    assert arr[0]["x"] == 1
    assert arr[0]["s"] == "a"
