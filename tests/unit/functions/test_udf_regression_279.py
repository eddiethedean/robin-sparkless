from sparkless.testing import get_imports

_imports = get_imports()
F = _imports.F
T = _imports


def test_udf_with_withColumn_regression_279(spark):
    data = [
        {"Name": "Alice", "Value": "abc"},
        {"Name": "Bob", "Value": "def"},
    ]

    df = spark.createDataFrame(data=data)

    my_udf = F.udf(lambda x: x.upper(), T.StringType())
    df2 = df.withColumn("Value", my_udf(F.col("Value")))

    rows = df2.collect()
    assert [r["Value"] for r in rows] == ["ABC", "DEF"]
