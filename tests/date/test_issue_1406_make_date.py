"""Regression test for #1406: date.make_date parity."""


def test_make_date_parity(spark, spark_imports):
    F = spark_imports.F

    df = spark.createDataFrame(
        [(2020, 1, 2), (2020, 2, 30), (None, 1, 2)],
        ["y", "m", "d"],
    )

    out = df.select(F.make_date("y", "m", "d").alias("dt")).collect()
    vals = [row["dt"] for row in out]

    # PySpark behavior:
    # - make_date(2020,1,2)   -> 2020-01-02
    # - make_date(2020,2,30)  -> null (invalid day for month)
    # - make_date(None,1,2)   -> null
    assert vals[0].strftime("%Y-%m-%d") == "2020-01-02"
    assert vals[1] is None
    assert vals[2] is None
