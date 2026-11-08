"""Exercise common authored contracts with nulls, joins, windows and schemas."""

from sparkless.sql import SparkSession, functions as F, types as T
from sparkless.sql.window import Window


def main():
    spark = SparkSession.builder.app_name("api-contracts").get_or_create()
    try:
        schema = T.StructType(
            [
                T.StructField("id", T.LongType(), True),
                T.StructField("amount", T.LongType(), True),
            ]
        )
        orders = spark.createDataFrame([(1, 10), (2, 30), (3, None)], schema)
        assert orders.filter(F.col("amount").isNull()).count() == 1
        classified = orders.withColumn(
            "size",
            F.when(F.col("amount") >= 20, F.lit("large")).otherwise(F.lit("small")),
        )
        assert {r["id"]: r["size"] for r in classified.collect()} == {
            1: "small",
            2: "large",
            3: "small",
        }
        assert orders.filter(F.col("id").isin([1, 2])).count() == 2
        customers = spark.createDataFrame(
            [(1, "East"), (2, "East"), (3, "West")], ["id", "region"]
        )
        joined = orders.join(customers, on="id", how="inner")
        total = (
            joined.filter(F.col("amount").isNotNull())
            .groupBy("region")
            .agg(F.sum("amount").alias("total"))
        )
        assert [(r["region"], r["total"]) for r in total.collect()] == [("East", 40)]
        assert F.col("id").name == "id"
        grouped = joined.filter(F.col("amount").isNotNull()).group_by(["region"])
        assert grouped.min("amount").collect()[0][1] == 10
        assert grouped.max("amount").collect()[0][1] == 30
        assert grouped.avg("amount").collect()[0][1] == 20
        window = Window.partitionBy("region").orderBy(F.col("id").asc())
        ranked = joined.withColumn("position", F.row_number().over(window))
        assert {r["id"]: r["position"] for r in ranked.collect()} == {1: 1, 2: 2, 3: 1}
        assert isinstance(orders.schema["id"].dataType, T.LongType)
        assert spark.createDataFrame([], schema).count() == 0
        assert orders.select("id").unionByName(orders.select("id")).count() == 6
        with orders.toArrowBatches(max_rows=2) as batches:
            assert sum(batch.num_rows for batch in batches) == 3
        print(
            "API null, conditional, join, aggregate, window and Arrow contracts passed"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
