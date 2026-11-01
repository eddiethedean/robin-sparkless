"""Exercise configured DuckDB sources through an unmodified public Connect client.

Requires the optional ``duckdb`` Python package only to create the fixture file. The native server
under test uses its separately pinned bundled DuckDB driver.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import importlib.metadata
import json
import os
import tempfile
from pathlib import Path

import duckdb
from pyspark.sql import functions as F
from pyspark.sql.connect.session import SparkSession as ConnectSession

from sparkless.server import SparklessConnect


def main() -> None:
    assert importlib.metadata.version("pyspark-client") == "4.0.5"
    parser = argparse.ArgumentParser()
    parser.add_argument("--server-bin", type=Path, required=True)
    args = parser.parse_args()

    with tempfile.TemporaryDirectory(prefix="sparkless-duckdb-source-") as temp_dir:
        database_path = Path(temp_dir) / "warehouse.duckdb"
        connection = duckdb.connect(str(database_path))
        connection.execute(
            """
            CREATE TABLE events (
                id BIGINT NOT NULL,
                bucket INTEGER,
                label VARCHAR,
                amount DECIMAL(12, 2),
                active BOOLEAN
            );
            INSERT INTO events VALUES
                (1, 10, 'alpha', 12.50, true),
                (2, 10, 'beta', NULL, false),
                (3, 20, 'gamma', 7.25, true);
            CREATE TABLE typed_events (
                small SMALLINT,
                score DOUBLE,
                day DATE,
                seen_at TIMESTAMPTZ
            );
            INSERT INTO typed_events VALUES
                (-32768, 3.5, DATE '2026-10-30', TIMESTAMPTZ '2026-10-30 12:34:56+00');
            CREATE TABLE non_finite_events (score DOUBLE);
            INSERT INTO non_finite_events VALUES (CAST('NaN' AS DOUBLE));
            CREATE TABLE nested_events (items INTEGER[]);
            INSERT INTO nested_events VALUES ([1, 2]);
            CREATE TABLE right_events (id BIGINT, note VARCHAR);
            INSERT INTO right_events VALUES (1, 'one'), (2, 'two'), (4, 'four');
            CREATE TABLE empty_events (id BIGINT NOT NULL, label VARCHAR);
            CREATE TABLE all_null_events (id INTEGER, label VARCHAR);
            INSERT INTO all_null_events VALUES (1, NULL), (2, NULL);
            """
        )
        connection.close()

        previous_sources = os.environ.get("SPARKLESS_DUCKDB_SOURCES")
        os.environ["SPARKLESS_DUCKDB_SOURCES"] = json.dumps(
            [{"alias": "warehouse", "database_path": str(database_path)}]
        )
        try:
            with SparklessConnect(
                server_options={
                    "executable_path": str(args.server_bin.absolute()),
                }
            ) as spark:
                assert isinstance(spark, ConnectSession)

                events = spark.table("warehouse.main.events")
                assert events.schema.fieldNames() == [
                    "id",
                    "bucket",
                    "label",
                    "amount",
                    "active",
                ]
                rows = sorted(
                    (
                        row.id,
                        row.bucket,
                        row.label,
                        str(row.amount) if row.amount is not None else None,
                        row.active,
                    )
                    for row in events.collect()
                )
                assert rows == [
                    (1, 10, "alpha", "12.50", True),
                    (2, 10, "beta", None, False),
                    (3, 20, "gamma", "7.25", True),
                ], rows

                typed = spark.table("warehouse.typed_events")
                assert typed.schema.fieldNames() == [
                    "small",
                    "score",
                    "day",
                    "seen_at",
                ]
                typed_row = typed.collect()[0]
                assert typed_row.small == -32768
                assert typed_row.score == 3.5
                assert str(typed_row.day) == "2026-10-30"
                expected_instant = datetime(
                    2026, 10, 30, 12, 34, 56, tzinfo=timezone.utc
                ).timestamp()
                assert typed_row.seen_at.timestamp() == expected_instant

                try:
                    spark.table("warehouse.non_finite_events").collect()
                except Exception as error:
                    assert "non-finite" in str(error).lower(), error
                else:
                    raise AssertionError("non-finite DuckDB DOUBLE values were accepted")
                try:
                    spark.table("warehouse.nested_events").schema
                except Exception as error:
                    message = str(error).lower()
                    assert (
                        "duckdb type" in message
                        and "not in the supported source type set" in message
                    ), error
                else:
                    raise AssertionError("nested DuckDB values were accepted")

                filtered = sorted(
                    (row.id, row.label)
                    for row in events.filter("bucket > 9")
                    .select("id", "label")
                    .collect()
                )
                assert filtered == [(1, "alpha"), (2, "beta"), (3, "gamma")]
                assert events.count() == 3
                assert (
                    events.agg(F.count("amount").alias("n")).collect()[0]["n"] == 2
                )
                assert events.limit(0).collect() == []

                empty = spark.table("warehouse.empty_events")
                assert empty.schema.fieldNames() == ["id", "label"]
                assert empty.collect() == []
                assert empty.count() == 0
                assert empty.agg(F.count("id").alias("n")).collect()[0]["n"] == 0
                all_null = spark.table("warehouse.all_null_events")
                assert all_null.agg(F.count("label").alias("n")).collect()[0]["n"] == 0

                grouped = sorted(
                    tuple(row)
                    for row in events.groupBy("bucket")
                    .agg(
                        F.count(F.lit(1)).alias("n"),
                        F.sum("bucket").alias("total"),
                    )
                    .collect()
                )
                assert grouped == [(10, 2, 20), (20, 1, 20)], grouped

                right = spark.table("warehouse.right_events")
                left_alias, right_alias = events.alias("l"), right.alias("r")
                joined = sorted(
                    tuple(row)
                    for row in left_alias.join(
                        right_alias,
                        left_alias["id"] == right_alias["id"],
                        "inner",
                    )
                    .select(
                        left_alias["id"].alias("id"),
                        right_alias["note"].alias("note"),
                    )
                    .collect()
                )
                assert joined == [(1, "one"), (2, "two")], joined

            print(
                "4.19 public Connect DuckDB source smoke passed "
                f"(Python DuckDB fixture writer {duckdb.__version__})"
            )
        finally:
            if previous_sources is None:
                os.environ.pop("SPARKLESS_DUCKDB_SOURCES", None)
            else:
                os.environ["SPARKLESS_DUCKDB_SOURCES"] = previous_sources


if __name__ == "__main__":
    main()
