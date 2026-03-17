"""Regression test for #1405: date.add_months parity."""


def test_add_months_parity(spark, spark_imports):
    F = spark_imports.F

    df = spark.createDataFrame(
        [("2020-01-31",), ("2020-02-29",), (None,)],
        ["s"],
    )

    # Cast string to date, then add one month as PySpark does.
    out = df.select(F.add_months(F.to_date("s"), 1).alias("dt")).collect()
    vals = [row["dt"] for row in out]

    # PySpark behavior:
    # - add_months(date("2020-01-31"), 1) -> 2020-02-29
    # - add_months(date("2020-02-29"), 1) -> 2020-03-29
    # - add_months(None, 1)              -> null
    assert vals[0].strftime("%Y-%m-%d") == "2020-02-29"
    assert vals[1].strftime("%Y-%m-%d") == "2020-03-29"
    assert vals[2] is None
