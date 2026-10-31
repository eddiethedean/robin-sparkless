"""Exercise the 4.19 public client additions and helper against a native server."""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.connect.session import SparkSession as ConnectSession

from sparkless.server import SparklessConnect


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
        assert using_rows == [(1, None, "one"), (1, 10, "one"), (2, 20, "two")], using_rows

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

        print("4.19 public-client helper, range-count, join, and aggregate smoke passed")


if __name__ == "__main__":
    main()
