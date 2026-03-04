from tests.fixtures.spark_imports import get_spark_imports

_imports = get_spark_imports()
SparkSession = _imports.SparkSession
F = _imports.F
T = _imports


def test_udf_with_withColumn_regression_279():
    spark = SparkSession.builder.appName("Example").getOrCreate()

    data = [
        {"Name": "Alice", "Value": "abc"},
        {"Name": "Bob", "Value": "def"},
    ]

    df = spark.createDataFrame(data=data)

    my_udf = F.udf(lambda x: x.upper(), T.StringType())
    df2 = df.withColumn("Value", my_udf(F.col("Value")))

    rows = df2.collect()
    assert [r["Value"] for r in rows] == ["ABC", "DEF"]
