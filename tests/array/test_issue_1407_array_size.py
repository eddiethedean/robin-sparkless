"""Regression test for #1407: array_size / size parity on null and empty arrays."""


def test_array_size_parity(spark, spark_imports):
    F = spark_imports.F

    df = spark.createDataFrame(
        [(["a", "b"],), ([],), (None,)],
        ["arr"],
    )

    out = df.select(F.size("arr").alias("out")).collect()
    vals = [row["out"] for row in out]

    # Current Sparkless behavior (and PySpark for size(null)): 2, 0, None.
    assert vals == [2, 0, None]
