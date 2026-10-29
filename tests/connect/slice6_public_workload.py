"""Run the frozen public DataFrame witness set and emit comparable JSON results."""

from __future__ import annotations

import argparse
import hashlib
import json
import struct
from datetime import date, datetime
from decimal import Decimal

from pyspark.sql.connect.session import SparkSession
from pyspark.sql.connect.functions import col, count, lit


def normalize(value):
    if isinstance(value, Decimal):
        return {"decimal": str(value)}
    if isinstance(value, (date, datetime)):
        return {value.__class__.__name__: value.isoformat()}
    if isinstance(value, tuple):
        return [normalize(item) for item in value]
    if isinstance(value, list):
        return [normalize(item) for item in value]
    return value


def table_witness(dataframe):
    arrow = dataframe.toArrow()
    return {
        "schema": [
            {
                "name": field.name,
                "type": field.dataType.simpleString(),
                "nullable": field.nullable,
            }
            for field in dataframe.schema.fields
        ],
        "arrow_schema": [
            {"name": field.name, "type": str(field.type), "nullable": field.nullable}
            for field in arrow.schema
        ],
        "rows": [normalize(tuple(row)) for row in dataframe.collect()],
        "arrow_rows": [
            [
                normalize(arrow.column(index)[row].as_py())
                for index in range(arrow.num_columns)
            ]
            for row in range(arrow.num_rows)
        ],
    }


def byte_sized_batch_witness(spark):
    column_count = 64
    row_count = 20_000
    dataframe = spark.range(row_count).select(
        *(col("id").alias(f"c{index}") for index in range(column_count))
    )
    arrow = dataframe.toArrow()
    expected_hash = hashlib.sha256(
        b"".join(struct.pack("<q", value) for value in range(row_count))
    ).hexdigest()
    column_hashes = [
        hashlib.sha256(
            b"".join(
                struct.pack("<q", value) for value in arrow.column(index).to_pylist()
            )
        ).hexdigest()
        for index in range(column_count)
    ]
    assert arrow.num_rows == row_count
    assert column_hashes == [expected_hash] * column_count
    return {
        "row_count": arrow.num_rows,
        "schema": [
            {
                "name": field.name,
                "type": field.dataType.simpleString(),
                "nullable": field.nullable,
            }
            for field in dataframe.schema.fields
        ],
        "arrow_schema": [
            {"name": field.name, "type": str(field.type), "nullable": field.nullable}
            for field in arrow.schema
        ],
        "column_sha256": column_hashes,
        "first_last": [
            arrow.column(0)[0].as_py(),
            arrow.column(0)[row_count - 1].as_py(),
        ],
    }


def near_response_limit_local_relation_witness(spark):
    values = ("a" * 300_000, "b" * 300_000, "c" * 300_000)
    dataframe = spark.createDataFrame(
        [values], "first STRING, second STRING, third STRING"
    )
    arrow = dataframe.toArrow()
    expected_hashes = [hashlib.sha256(value.encode()).hexdigest() for value in values]
    actual_values = [arrow.column(index)[0].as_py() for index in range(3)]
    assert [len(value) for value in actual_values] == [300_000] * 3
    actual_hashes = [
        hashlib.sha256(value.encode()).hexdigest() for value in actual_values
    ]
    assert actual_hashes == expected_hashes
    return {
        "schema": [
            {
                "name": field.name,
                "type": field.dataType.simpleString(),
                "nullable": field.nullable,
            }
            for field in dataframe.schema.fields
        ],
        "arrow_schema": [
            {"name": field.name, "type": str(field.type), "nullable": field.nullable}
            for field in arrow.schema
        ],
        "value_lengths": [len(value) for value in actual_values],
        "value_sha256": actual_hashes,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.remote(args.url).getOrCreate()
    try:
        before_epoch = datetime.fromisoformat("1969-12-31T23:59:59.123456+00:00")
        typed_rows = [
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
        typed_schema = (
            "flag BOOLEAN, small SMALLINT, number INT, wide BIGINT, score DOUBLE, "
            "text STRING, day DATE, instant TIMESTAMP, amount DECIMAL(30, 6)"
        )

        results = {
            "server_reported_spark_version": spark.version,
            "range_filter_count": spark.range(10).filter("id > 5").count(),
            "global_count_schema": table_witness(
                spark.range(10).filter("id > 5").agg(count(lit(1)))
            ),
            "empty_global_count": table_witness(spark.range(0).agg(count(lit(1)))),
            "projection_limit": table_witness(
                spark.range(10).select(col("id").alias("value")).limit(3)
            ),
            "negative_step_range": table_witness(spark.range(5, -6, -2, 1)),
            "checked_expressions": table_witness(
                spark.range(5)
                .filter(col("id") >= 2)
                .select((col("id") + 1).alias("next"), col("id").alias("source"))
                .limit(2)
            ),
            "typed_local_relation": table_witness(
                spark.createDataFrame(typed_rows, typed_schema)
            ),
            "empty_local_relation": table_witness(
                spark.createDataFrame([], "id BIGINT, label STRING")
            ),
            "duplicate_aliases": table_witness(
                spark.range(3).select(col("id").alias("x"), col("id").alias("x"))
            ),
            "byte_sized_batches": byte_sized_batch_witness(spark),
            "near_response_limit_local_relation": near_response_limit_local_relation_witness(
                spark
            ),
        }
        with open(args.output, "w", encoding="utf-8") as output:
            json.dump(results, output, ensure_ascii=False, sort_keys=True, indent=2)
            output.write("\n")
        print(f"public workload passed for {args.url}", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
