"""Exercise the 4.19 public client additions and helper against a native server."""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.connect.session import SparkSession as ConnectSession

from sparkless_connect import SparklessConnect


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--server-bin", type=Path)
    args = parser.parse_args()
    server_options = (
        {"executable_path": str(args.server_bin.absolute())}
        if args.server_bin is not None
        else {}
    )

    with SparklessConnect(server_options=server_options) as spark:
        assert isinstance(spark, ConnectSession)

        range_rows = spark.range(10_000_000)
        assert range_rows.filter("id > 5").count() == 9_999_994
        assert range_rows.filter(F.col("id") >= 5).count() == 9_999_995
        assert spark.range(1_000).filter("id > 1000").collect() == []
        assert spark.range(10_000).filter("id > 10000").count() == 0

        bounded_local = spark.createDataFrame(
            [(index,) for index in range(10_000)], "id BIGINT"
        )
        assert bounded_local.count() == 10_000

        wide_value = "w" * 300_000
        variable_width = spark.createDataFrame(
            [(1, ""), (2, "x"), (3, "界" * 4_096), (4, wide_value)],
            "id INT, payload STRING",
        )
        variable_rows = variable_width.limit(3).collect()
        assert [(row.id, row.payload) for row in variable_rows] == [
            (1, ""),
            (2, "x"),
            (3, "界" * 4_096),
        ]
        wide_rows = variable_width.collect()
        assert len(wide_rows) == 4 and wide_rows[-1].payload == wide_value

        for label, oversized in (
            (
                "local row limit",
                spark.createDataFrame(
                    [(index,) for index in range(10_001)], "id BIGINT"
                ),
            ),
            (
                "local scalar limit",
                spark.createDataFrame(
                    [(1, "x" * (512 * 1024 + 1))], "id INT, payload STRING"
                ),
            ),
            (
                "local IPC byte limit",
                spark.createDataFrame(
                    [(1, "a" * 400_000), (2, "b" * 400_000), (3, "c" * 400_000)],
                    "id INT, payload STRING",
                ),
            ),
        ):
            try:
                oversized.collect()
            except Exception as error:
                message = str(error).lower()
                assert "connect local" in message, (label, error)
                assert "limit" in message or "exceed" in message, (label, error)
            else:
                raise AssertionError(f"{label} was accepted")

        left = spark.createDataFrame(
            [(1, 10), (1, None), (2, 20), (4, 40)],
            "id INT, amount INT",
        )
        right = spark.createDataFrame(
            [(1, "one"), (2, "two"), (3, "three")],
            "id INT, label STRING",
        )

        left_alias = left.alias("l")
        right_alias = right.alias("r")
        condition_join = left_alias.join(
            right_alias, left_alias["id"] == right_alias["id"], "inner"
        ).select(left_alias["id"].alias("key"), right_alias["label"].alias("label"))
        condition_rows = sorted(tuple(row) for row in condition_join.collect())
        assert condition_rows == [(1, "one"), (1, "one"), (2, "two")], condition_rows
        assert condition_join.schema.fieldNames() == ["key", "label"]

        using_rows = sorted(
            (tuple(row) for row in left.join(right, on="id", how="inner").collect()),
            key=lambda row: (row[0], row[1] is not None, row[1] or 0),
        )
        assert using_rows == [
            (1, None, "one"),
            (1, 10, "one"),
            (2, 20, "two"),
        ], using_rows

        grouped = left.groupBy("id").agg(
            F.count(F.lit(1)).alias("rows"),
            F.count("amount").alias("non_null_amounts"),
            F.sum("amount").alias("total"),
        )
        grouped_rows = sorted(tuple(row) for row in grouped.collect())
        assert grouped_rows == [(1, 2, 1, 10), (2, 1, 1, 20), (4, 1, 1, 40)]
        assert grouped.schema.fieldNames() == [
            "id",
            "rows",
            "non_null_amounts",
            "total",
        ]
        aggregate_types = [
            type(grouped.schema[name].dataType).__name__
            for name in ("rows", "non_null_amounts", "total")
        ]
        assert aggregate_types == [
            "LongType",
            "LongType",
            "LongType",
        ]
        assert not grouped.schema["rows"].nullable
        assert not grouped.schema["non_null_amounts"].nullable
        assert grouped.schema["total"].nullable

        print(
            "4.19 public-client helper, range, bounded local relation, "
            "join, and aggregate smoke passed"
        )


if __name__ == "__main__":
    main()
