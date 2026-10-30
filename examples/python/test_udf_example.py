"""Public typed scalar UDF contract; no pandas/vectorized API is promised."""
def test_scalar_udf(spark, spark_imports):
    F = spark_imports.F
    double = F.udf(lambda value: None if value is None else value * 2, spark_imports.IntegerType())
    df = spark.createDataFrame([(2,), (None,)], spark_imports.StructType([spark_imports.StructField("x", spark_imports.IntegerType(), True)]))
    assert [r["doubled"] for r in df.withColumn("doubled", double(F.col("x"))).collect()] == [4, None]
    assert df.filter(double(F.col("x")) >= 4).count() == 1
