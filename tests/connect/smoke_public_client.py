"""Run against examples/connect-dev-server.rs with the locked PySpark client venv."""

from datetime import date, datetime, timezone
from decimal import Decimal
import os

from pyspark.sql.connect.session import SparkSession
from pyspark.sql.functions import col


def main() -> None:
    connect_url = os.environ.get("SPARKLESS_CONNECT_URL", "sc://127.0.0.1:15002")
    if ";user_id=" not in connect_url:
        connect_url = connect_url.rstrip("/") + "/;user_id=sparkless-public-smoke"
    spark = SparkSession.builder.remote(connect_url).getOrCreate()
    try:
        assert spark.range(10).filter("id > 5").count() == 4
        print("range/filter/count passed", flush=True)

        schema = (
            "flag BOOLEAN, small SMALLINT, number INT, wide BIGINT, score DOUBLE, "
            "text STRING, day DATE, instant TIMESTAMP, amount DECIMAL(30, 6)"
        )
        before_epoch = datetime(1969, 12, 31, 23, 59, 59, 123456, tzinfo=timezone.utc)
        rows = [
            (
                True,
                -32768,
                -2147483648,
                -9223372036854775807,
                1.25,
                "",
                date(1970, 1, 1),
                before_epoch,
                Decimal("-12345678901234567890.123456"),
            ),
            (
                None,
                None,
                None,
                None,
                None,
                None,
                date(1969, 12, 31),
                before_epoch,
                None,
            ),
        ]
        local = spark.createDataFrame(rows, schema)
        decoded = local.collect()
        assert decoded[0].flag is True
        assert decoded[0].small == -32768
        assert decoded[0].number == -2147483648
        assert decoded[0].wide == -9223372036854775807
        assert decoded[0].score == 1.25
        assert decoded[0].text == ""
        assert decoded[0].day == date(1970, 1, 1)
        assert decoded[0].instant == before_epoch.astimezone().replace(tzinfo=None)
        assert decoded[0].amount == Decimal("-12345678901234567890.123456")
        assert all(value is None for value in decoded[1][:6])
        assert decoded[1].day == date(1969, 12, 31)
        print("typed local relation passed", flush=True)
        local_arrow = local.toArrow()
        assert local_arrow.schema.names == [
            "flag",
            "small",
            "number",
            "wide",
            "score",
            "text",
            "day",
            "instant",
            "amount",
        ]
        assert [str(field.type) for field in local_arrow.schema] == [
            "bool",
            "int16",
            "int32",
            "int64",
            "double",
            "string",
            "date32[day]",
            "timestamp[us, tz=UTC]",
            "decimal128(30, 6)",
        ]
        assert local_arrow.column("instant")[0].as_py() == before_epoch
        assert local_arrow.column("amount")[0].as_py() == Decimal(
            "-12345678901234567890.123456"
        )

        empty = spark.createDataFrame([], "id BIGINT, label STRING")
        assert empty.collect() == []
        empty_arrow = empty.toArrow()
        assert empty_arrow.schema.names == ["id", "label"]
        assert empty_arrow.num_rows == 0
        print("schema-only empty local relation passed", flush=True)
        assert len(spark.range(20_000).collect()) == 20_000
        print("multi-batch range passed", flush=True)

        duplicate = (
            spark.range(3).select(col("id").alias("x"), col("id").alias("x")).collect()
        )
        assert [tuple(row) for row in duplicate] == [(0, 0), (1, 1), (2, 2)]
        duplicate_arrow = (
            spark.range(3).select(col("id").alias("x"), col("id").alias("x")).toArrow()
        )
        assert duplicate_arrow.schema.names == ["x", "x"]
        assert duplicate_arrow.column(0).to_pylist() == [0, 1, 2]
        assert duplicate_arrow.column(1).to_pylist() == [0, 1, 2]
        print("duplicate aliases passed", flush=True)
        print("public Spark Connect client smoke passed")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
