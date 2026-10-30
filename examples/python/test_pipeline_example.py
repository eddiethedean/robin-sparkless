"""Executable companion to docs/PIPELINE_TUTORIAL.md, on either backend."""


def summarize(orders, customers, F):
    clean = orders.withColumn("amount", F.col("amount").cast("double"))
    return (
        clean.filter(F.col("amount") > 0)
        .join(customers, on="customer_id", how="inner")
        .groupBy("region")
        .agg(F.sum("amount").alias("revenue"))
    )


def test_pipeline(spark, spark_imports, tmp_path):
    source = tmp_path / "orders.csv"
    source.write_text(
        "customer_id,amount\n1,12.5\n1,7.5\n2,5.0\n2,-1.0\n", encoding="utf-8"
    )
    orders = spark.read.option("header", "true").csv(str(source))
    orders = orders.withColumn(
        "customer_id", spark_imports.F.col("customer_id").cast("long")
    )
    customers = spark.createDataFrame(
        [(1, "East"), (2, "West")], ["customer_id", "region"]
    )
    result = summarize(orders, customers, spark_imports.F)
    target = str(tmp_path / "revenue.parquet")
    result.write.mode("overwrite").parquet(target)
    actual = spark.read.parquet(target)
    assert {r["region"]: r["revenue"] for r in actual.collect()} == {
        "East": 20.0,
        "West": 5.0,
    }
