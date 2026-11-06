"""Executable examples for the LinkedIn graphic. Python 3.11+."""

# BEFORE: pip install "sparkless==4.18.1"
from sparkless.sql import SparkSession

with SparkSession.builder.app_name("test").get_or_create() as spark:
    orders = spark.createDataFrame(
        [(1, 25), (2, 150), (3, 200)],
        "order_id LONG, total LONG",
    )
    assert orders.filter("total > 99").count() == 2

print("BEFORE: PASS")

# AFTER: pip install "sparkless[server]==4.18.1" "pyspark-client==4.0.5"
from pyspark.sql import SparkSession  # noqa: E402
from sparkless import SparklessServer  # noqa: E402

with SparklessServer() as server:
    spark = SparkSession.builder.remote(server.spark_connect_url).create()
    try:
        orders = spark.createDataFrame(
            [(1, 25), (2, 150), (3, 200)],
            "order_id LONG, total LONG",
        )
        assert orders.filter("total > 99").count() == 2
    finally:
        spark.stop()

print("AFTER: PASS")
