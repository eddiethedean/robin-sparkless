"""All three snippets for the benchmark graphic; run local in its own environment."""
import sys


def check_orders(spark):
    orders = spark.createDataFrame(
        [(i, i % 200) for i in range(1000)],
        "order_id LONG, total LONG",
    )
    assert orders.filter("total > 99").count() == 500


if sys.argv[1] == 'direct':
    from sparkless.sql import SparkSession
    with (
        SparkSession.builder.app_name("test")
        .get_or_create()
    ) as spark:
        check_orders(spark)
elif sys.argv[1] == 'connect':
    from pyspark.sql import SparkSession
    from sparkless import SparklessServer
    with SparklessServer() as server:
        spark = SparkSession.builder.remote(
            server.spark_connect_url
        ).create()
        try:
            check_orders(spark)
        finally:
            spark.stop()
elif sys.argv[1] == 'local':
    from pyspark.sql import SparkSession
    with (
        SparkSession.builder.master("local[4]")
        .appName("test").getOrCreate()
    ) as spark:
        check_orders(spark)
else:
    raise ValueError(sys.argv[1])
print(sys.argv[1], 'PASS')
