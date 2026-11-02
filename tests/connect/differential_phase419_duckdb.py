"""Compare the declared DuckDB Connect-source subset with Spark 4.0.5 local mode."""

from __future__ import annotations

import argparse
from datetime import UTC, date, datetime
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import tempfile
from typing import Any

import duckdb
import sparkless
from pyspark.sql import DataFrame as LocalDataFrame
from pyspark.sql import SparkSession as LocalSparkSession
from pyspark.sql import functions as F
from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame
from pyspark.sql.connect.session import SparkSession as ConnectSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from sparkless.server import SparklessConnect


def normalize(value: Any) -> Any:
    if isinstance(value, Decimal):
        return {"decimal": str(value)}
    if isinstance(value, datetime):
        instant = (
            value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
        )
        return {"timestamp_utc": instant.isoformat()}
    if isinstance(value, date):
        return {"date": value.isoformat()}
    if isinstance(value, (tuple, list)):
        return [normalize(item) for item in value]
    return value


def dataframe_result(frame: LocalDataFrame | ConnectDataFrame) -> dict[str, Any]:
    schema = json.loads(frame.schema.json())
    rows = [normalize(tuple(row)) for row in frame.collect()]
    rows.sort(key=lambda row: json.dumps(row, sort_keys=True, separators=(",", ":")))
    return {"schema": schema, "rows": rows}


def compare_dataframe(
    name: str,
    candidate: ConnectDataFrame,
    reference: LocalDataFrame,
    results: dict[str, Any],
) -> None:
    candidate_result = dataframe_result(candidate)
    reference_result = dataframe_result(reference)
    if candidate_result != reference_result:
        raise AssertionError(
            f"Spark differential failed for {name}:\n"
            f"Sparkless={json.dumps(candidate_result, sort_keys=True, default=str)}\n"
            f"Spark={json.dumps(reference_result, sort_keys=True, default=str)}"
        )
    results[name] = candidate_result


def compare_scalar(
    name: str, candidate: Any, reference: Any, results: dict[str, Any]
) -> None:
    candidate_value = normalize(candidate)
    reference_value = normalize(reference)
    if candidate_value != reference_value:
        raise AssertionError(
            f"Spark differential failed for {name}: "
            f"Sparkless={candidate_value!r}, Spark={reference_value!r}"
        )
    results[name] = {"value": candidate_value}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server-bin", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    server_bin = args.server_bin.absolute()
    if not server_bin.is_file():
        parser.error(f"server binary not found: {server_bin}")
    if duckdb.__version__ != "1.5.1":
        parser.error(f"expected duckdb==1.5.1, found {duckdb.__version__}")

    previous_sources = os.environ.get("SPARKLESS_DUCKDB_SOURCES")
    previous_spark_home = os.environ.pop("SPARK_HOME", None)
    report: dict[str, Any] = {
        "schema_version": 1,
        "timestamp_utc": datetime.now(UTC).isoformat(),
        "oracle": "Apache Spark 4.0.5 local[4]",
        "duckdb_fixture_writer": duckdb.__version__,
        "pyspark_oracle": __import__("pyspark").__version__,
        "candidate_api_version": sparkless.__version__,
        "platform": platform.platform(),
        "server_binary": str(server_bin),
        "cases": {},
    }
    with server_bin.open("rb") as binary:
        report["server_binary_sha256"] = hashlib.file_digest(
            binary, "sha256"
        ).hexdigest()
    server_version = subprocess.run(
        [str(server_bin), "--version"],
        capture_output=True,
        check=True,
        text=True,
        timeout=10,
    )
    report["candidate_server_binary_version"] = server_version.stdout.strip()
    with tempfile.TemporaryDirectory(prefix="sparkless-phase419-differential-") as work:
        database_path = Path(work) / "warehouse.duckdb"
        fixture = duckdb.connect(str(database_path))
        fixture.execute(
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
            CREATE TABLE right_events (id BIGINT, note VARCHAR);
            INSERT INTO right_events VALUES (1, 'one'), (2, 'two'), (4, 'four');
            CREATE TABLE empty_events (id BIGINT NOT NULL, label VARCHAR);
            CREATE TABLE all_null_events (id INTEGER, label VARCHAR);
            INSERT INTO all_null_events VALUES (1, NULL), (2, NULL);
            CREATE TABLE null_measurement_events (group_key INTEGER, amount BIGINT);
            INSERT INTO null_measurement_events VALUES (1, NULL), (1, NULL), (NULL, NULL);
            CREATE TABLE schema_change_events (id INTEGER, label VARCHAR);
            INSERT INTO schema_change_events VALUES (1, 'before');
            CREATE TABLE nullable_filter_events (id BIGINT, bucket INTEGER);
            INSERT INTO nullable_filter_events VALUES (1, 10), (2, NULL), (3, 20);
            CREATE TABLE typed_events (
                small SMALLINT,
                score DOUBLE,
                day DATE,
                seen_at TIMESTAMPTZ
            );
            INSERT INTO typed_events VALUES
                (-32768, 3.5, DATE '2026-10-30', TIMESTAMPTZ '2026-10-30 12:34:56.123456+00');
            CREATE TABLE integer_boundary_events (
                small_min SMALLINT NOT NULL,
                small_max SMALLINT NOT NULL,
                int_min INTEGER NOT NULL,
                int_max INTEGER NOT NULL,
                bigint_min BIGINT NOT NULL,
                bigint_max BIGINT NOT NULL
            );
            INSERT INTO integer_boundary_events VALUES
                (-32768, 32767, -2147483648, 2147483647,
                 CAST('-9223372036854775808' AS BIGINT),
                 CAST('9223372036854775807' AS BIGINT));
            CREATE TABLE decimal_boundary_events (
                d1 DECIMAL(1, 0),
                d9 DECIMAL(9, 0),
                d18 DECIMAL(18, 6),
                d38 DECIMAL(38, 0),
                d38s38 DECIMAL(38, 38)
            );
            INSERT INTO decimal_boundary_events VALUES
                (9, 999999999, CAST('999999999999.999999' AS DECIMAL(18, 6)),
                 CAST('99999999999999999999999999999999999999' AS DECIMAL(38, 0)),
                 CAST('0.' || repeat('9', 38) AS DECIMAL(38, 38))),
                (-9, -999999999, CAST('-999999999999.999999' AS DECIMAL(18, 6)),
                 CAST('-99999999999999999999999999999999999999' AS DECIMAL(38, 0)),
                 CAST('-0.' || repeat('9', 38) AS DECIMAL(38, 38)));
            CREATE TABLE join_left (id BIGINT, label VARCHAR);
            INSERT INTO join_left VALUES
                (1, 'a'), (1, 'b'), (2, 'c'), (3, 'd'), (NULL, 'null');
            CREATE TABLE join_right (id BIGINT, note VARCHAR);
            INSERT INTO join_right VALUES
                (1, 'one'), (1, 'uno'), (2, NULL), (3, 'three'), (NULL, 'null');
            """
        )
        fixture.close()
        os.environ["SPARKLESS_DUCKDB_SOURCES"] = json.dumps(
            [{"alias": "warehouse", "database_path": str(database_path)}]
        )

        reference = (
            LocalSparkSession.builder.master("local[4]")
            .appName("sparkless-phase419-duckdb-differential")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.ansi.enabled", "true")
            .config("spark.sql.caseSensitive", "false")
            .getOrCreate()
        )
        try:
            event_schema = StructType(
                [
                    StructField("id", LongType(), False),
                    StructField("bucket", IntegerType(), True),
                    StructField("label", StringType(), True),
                    StructField("amount", DecimalType(12, 2), True),
                    StructField("active", BooleanType(), True),
                ]
            )
            reference_events = reference.createDataFrame(
                [
                    (1, 10, "alpha", Decimal("12.50"), True),
                    (2, 10, "beta", None, False),
                    (3, 20, "gamma", Decimal("7.25"), True),
                ],
                event_schema,
            )
            reference_right = reference.createDataFrame(
                [(1, "one"), (2, "two"), (4, "four")],
                StructType(
                    [
                        StructField("id", LongType(), True),
                        StructField("note", StringType(), True),
                    ]
                ),
            )
            reference_empty = reference.createDataFrame(
                [],
                StructType(
                    [
                        StructField("id", LongType(), False),
                        StructField("label", StringType(), True),
                    ]
                ),
            )
            reference_all_null = reference.createDataFrame(
                [(1, None), (2, None)],
                StructType(
                    [
                        StructField("id", IntegerType(), True),
                        StructField("label", StringType(), True),
                    ]
                ),
            )
            reference_null_measurements = reference.createDataFrame(
                [(1, None), (1, None), (None, None)],
                StructType(
                    [
                        StructField("group_key", IntegerType(), True),
                        StructField("amount", LongType(), True),
                    ]
                ),
            )
            reference_nullable_filter = reference.createDataFrame(
                [(1, 10), (2, None), (3, 20)],
                StructType(
                    [
                        StructField("id", LongType(), True),
                        StructField("bucket", IntegerType(), True),
                    ]
                ),
            )
            reference_typed = reference.createDataFrame(
                [
                    (
                        -32768,
                        3.5,
                        date(2026, 10, 30),
                        datetime(2026, 10, 30, 12, 34, 56, 123456, tzinfo=UTC),
                    )
                ],
                StructType(
                    [
                        StructField("small", ShortType(), True),
                        StructField("score", DoubleType(), True),
                        StructField("day", DateType(), True),
                        StructField("seen_at", TimestampType(), True),
                    ]
                ),
            )
            join_schema = StructType(
                [
                    StructField("id", LongType(), True),
                    StructField("label", StringType(), True),
                ]
            )
            reference_left = reference.createDataFrame(
                [(1, "a"), (1, "b"), (2, "c"), (3, "d"), (None, "null")],
                join_schema,
            )
            reference_join_right = reference.createDataFrame(
                [(1, "one"), (1, "uno"), (2, None), (3, "three"), (None, "null")],
                StructType(
                    [
                        StructField("id", LongType(), True),
                        StructField("note", StringType(), True),
                    ]
                ),
            )
            reference_events.createOrReplaceTempView("events_ref")
            reference_right.createOrReplaceTempView("right_events_ref")
            reference_empty.createOrReplaceTempView("empty_events_ref")
            reference_all_null.createOrReplaceTempView("all_null_events_ref")
            reference_null_measurements.createOrReplaceTempView(
                "null_measurement_events_ref"
            )
            reference_nullable_filter.createOrReplaceTempView(
                "nullable_filter_events_ref"
            )
            reference_typed.createOrReplaceTempView("typed_events_ref")
            reference_integer_extrema = reference.createDataFrame(
                [(-32768, 32767, -(2**31), 2**31 - 1, -(2**63), 2**63 - 1)],
                StructType(
                    [
                        StructField("small_min", ShortType(), False),
                        StructField("small_max", ShortType(), False),
                        StructField("int_min", IntegerType(), False),
                        StructField("int_max", IntegerType(), False),
                        StructField("bigint_min", LongType(), False),
                        StructField("bigint_max", LongType(), False),
                    ]
                ),
            )
            reference_integer_extrema.createOrReplaceTempView("integer_boundary_events_ref")
            reference_decimals = reference.createDataFrame(
                [
                    (
                        Decimal("9"),
                        Decimal("999999999"),
                        Decimal("999999999999.999999"),
                        Decimal("99999999999999999999999999999999999999"),
                        Decimal("0." + "9" * 38),
                    ),
                    (
                        Decimal("-9"),
                        Decimal("-999999999"),
                        Decimal("-999999999999.999999"),
                        Decimal("-99999999999999999999999999999999999999"),
                        Decimal("-0." + "9" * 38),
                    ),
                ],
                StructType(
                    [
                        StructField("d1", DecimalType(1, 0), True),
                        StructField("d9", DecimalType(9, 0), True),
                        StructField("d18", DecimalType(18, 6), True),
                        StructField("d38", DecimalType(38, 0), True),
                        StructField("d38s38", DecimalType(38, 38), True),
                    ]
                ),
            )
            reference_decimals.createOrReplaceTempView("decimal_boundary_events_ref")
            reference_left.createOrReplaceTempView("join_left_ref")
            reference_join_right.createOrReplaceTempView("join_right_ref")

            server_env = {
                "SPARKLESS_MAX_ACTIVE_OPERATIONS": "2",
                "SPARKLESS_MAX_PENDING_OPERATIONS": "16",
                "SPARKLESS_QUEUE_TIMEOUT_MS": "5000",
                "SPARKLESS_RESPONSE_TIMEOUT_MS": "60000",
                "SPARKLESS_CONNECT_TRACE": "0",
                "SPARKLESS_ARROW_TRACE": "0",
                "POLARS_MAX_THREADS": "4",
                "RAYON_NUM_THREADS": "4",
                "OMP_NUM_THREADS": "4",
                "TZ": "UTC",
            }
            with SparklessConnect(
                server_options={"executable_path": str(server_bin), "env": server_env}
            ) as candidate:
                if not isinstance(candidate, ConnectSession):
                    raise TypeError(
                        f"expected a Connect session, got {type(candidate)!r}"
                    )
                events = candidate.table("warehouse.main.events")
                compare_dataframe("events", events, reference_events, report["cases"])
                compare_dataframe(
                    "right_events",
                    candidate.table("warehouse.right_events"),
                    reference_right,
                    report["cases"],
                )
                compare_dataframe(
                    "filter_project",
                    events.filter("bucket > 9").select("id", "label"),
                    reference.sql("SELECT id, label FROM events_ref WHERE bucket > 9"),
                    report["cases"],
                )
                compare_dataframe(
                    "nullable_filter_drops_unknown",
                    candidate.table("warehouse.nullable_filter_events")
                    .filter(F.col("bucket") >= 10)
                    .select("id"),
                    reference.sql(
                        "SELECT id FROM nullable_filter_events_ref WHERE bucket >= 10"
                    ),
                    report["cases"],
                )
                compare_dataframe(
                    "limit_zero",
                    events.limit(0),
                    reference.sql("SELECT * FROM events_ref LIMIT 0"),
                    report["cases"],
                )
                compare_scalar(
                    "count_rows",
                    events.count(),
                    reference_events.count(),
                    report["cases"],
                )
                compare_dataframe(
                    "count_nullable_column",
                    events.agg(F.count("amount").alias("n")),
                    reference.sql("SELECT count(amount) AS n FROM events_ref"),
                    report["cases"],
                )
                compare_dataframe(
                    "empty_table",
                    candidate.table("warehouse.empty_events"),
                    reference.sql("SELECT * FROM empty_events_ref"),
                    report["cases"],
                )
                compare_dataframe(
                    "all_null_count",
                    candidate.table("warehouse.all_null_events").agg(
                        F.count("label").alias("n")
                    ),
                    reference.sql("SELECT count(label) AS n FROM all_null_events_ref"),
                    report["cases"],
                )
                compare_dataframe(
                    "empty_count_rows",
                    candidate.table("warehouse.empty_events").agg(
                        F.count(F.lit(1)).alias("n")
                    ),
                    reference.sql("SELECT count(*) AS n FROM empty_events_ref"),
                    report["cases"],
                )
                compare_dataframe(
                    "grouped_count_sum",
                    events.groupBy("bucket").agg(
                        F.count(F.lit(1)).alias("n"),
                        F.count("amount").alias("amount_count"),
                        F.sum("bucket").alias("bucket_sum"),
                    ),
                    reference.sql(
                        "SELECT bucket, count(1) AS n, count(amount) AS amount_count, "
                        "sum(bucket) AS bucket_sum FROM events_ref GROUP BY bucket"
                    ),
                    report["cases"],
                )
                candidate_left = candidate.table("warehouse.join_left").alias("l")
                candidate_right = candidate.table("warehouse.join_right").alias("r")
                compare_dataframe(
                    "inner_join_null_and_duplicate_keys",
                    candidate_left.join(
                        candidate_right,
                        candidate_left["id"] == candidate_right["id"],
                        "inner",
                    ).select(
                        candidate_left["id"].alias("id"),
                        candidate_left["label"].alias("label"),
                        candidate_right["note"].alias("note"),
                    ),
                    reference.sql(
                        "SELECT l.id AS id, l.label AS label, r.note AS note "
                        "FROM join_left_ref l INNER JOIN join_right_ref r ON l.id = r.id"
                    ),
                    report["cases"],
                )
                candidate_null_group = candidate.table("warehouse.join_left")
                compare_dataframe(
                    "grouped_null_keys",
                    candidate_null_group.groupBy("id").agg(
                        F.count(F.lit(1)).alias("n"), F.count("label").alias("labels")
                    ),
                    reference.sql(
                        "SELECT id, count(1) AS n, count(label) AS labels "
                        "FROM join_left_ref GROUP BY id"
                    ),
                    report["cases"],
                )
                compare_dataframe(
                    "grouped_null_measurements",
                    candidate.table("warehouse.null_measurement_events")
                    .groupBy("group_key")
                    .agg(
                        F.count(F.lit(1)).alias("n"),
                        F.count("amount").alias("amount_count"),
                        F.sum("amount").alias("amount_sum"),
                    ),
                    reference.sql(
                        "SELECT group_key, count(1) AS n, count(amount) AS amount_count, "
                        "sum(amount) AS amount_sum FROM null_measurement_events_ref "
                        "GROUP BY group_key"
                    ),
                    report["cases"],
                )
                compare_dataframe(
                    "grouped_empty_input",
                    candidate.table("warehouse.empty_events")
                    .groupBy("id")
                    .agg(
                        F.count(F.lit(1)).alias("n"),
                        F.count("label").alias("label_count"),
                        F.sum("id").alias("id_sum"),
                    ),
                    reference.sql(
                        "SELECT id, count(1) AS n, count(label) AS label_count, "
                        "sum(id) AS id_sum FROM empty_events_ref GROUP BY id"
                    ),
                    report["cases"],
                )
                compare_dataframe(
                    "typed_scalars_and_utc_timestamp",
                    candidate.table("warehouse.typed_events"),
                    reference.sql("SELECT * FROM typed_events_ref"),
                    report["cases"],
                )
                compare_dataframe(
                    "integer_extrema_and_non_null_schema",
                    candidate.table("warehouse.integer_boundary_events"),
                    reference.sql("SELECT * FROM integer_boundary_events_ref"),
                    report["cases"],
                )
                compare_dataframe(
                    "decimal_precision_scale_boundaries",
                    candidate.table("warehouse.decimal_boundary_events"),
                    reference.sql("SELECT * FROM decimal_boundary_events_ref"),
                    report["cases"],
                )
                empty_join_left = candidate.table("warehouse.join_left")
                empty_join_right = candidate.table("warehouse.empty_events")
                compare_dataframe(
                    "inner_join_empty_side",
                    empty_join_left.join(
                        empty_join_right,
                        empty_join_left["id"] == empty_join_right["id"],
                        "inner",
                    ).select(
                        empty_join_left["id"].alias("id"),
                        empty_join_left["label"].alias("label"),
                    ),
                    reference.sql(
                        "SELECT l.id AS id, l.label AS label FROM join_left_ref l "
                        "INNER JOIN empty_events_ref r ON l.id = r.id"
                    ),
                    report["cases"],
                )

                stale_schema_frame = candidate.table("warehouse.schema_change_events")
                schema_before_change = stale_schema_frame.schema
                if not isinstance(schema_before_change["id"].dataType, IntegerType):
                    raise AssertionError(
                        "schema-change fixture must start with an INTEGER id"
                    )
                writer = duckdb.connect(str(database_path))
                try:
                    writer.execute(
                        "ALTER TABLE schema_change_events "
                        "ALTER COLUMN id SET DATA TYPE BIGINT"
                    )
                    writer.execute(
                        "UPDATE schema_change_events SET id = 2147483648"
                    )
                finally:
                    writer.close()
                try:
                    changed_rows = stale_schema_frame.collect()
                except Exception as error:
                    message = str(error)
                    lowered = message.lower()
                    if not any(
                        marker in lowered
                        for marker in ("schema", "analysis", "changed")
                    ):
                        raise AssertionError(
                            "schema drift must fail with a clear analysis/schema error; "
                            f"received {message!r}"
                        ) from error
                    report["cases"]["schema_change_between_analysis_and_execution"] = {
                        "outcome": "clear_error",
                        "schema_before": schema_before_change.jsonValue(),
                        "message": message,
                    }
                else:
                    current_schema = candidate.table(
                        "warehouse.schema_change_events"
                    ).schema
                    if not isinstance(current_schema["id"].dataType, LongType):
                        raise AssertionError(
                            "post-change analysis did not report BIGINT id"
                        )
                    if [row.id for row in changed_rows] != [2147483648]:
                        raise AssertionError(
                            "pre-change plan did not return the reanalyzed BIGINT row: "
                            f"{changed_rows!r}"
                        )
                    report["cases"]["schema_change_between_analysis_and_execution"] = {
                        "outcome": "reanalyzed",
                        "schema_before": schema_before_change.jsonValue(),
                        "schema_after": current_schema.jsonValue(),
                        "rows": [[row.id, row.label] for row in changed_rows],
                    }
                report["candidate_reported_spark_version"] = candidate.version
            report["candidate_passed"] = True
        finally:
            reference.stop()
            if previous_sources is None:
                os.environ.pop("SPARKLESS_DUCKDB_SOURCES", None)
            else:
                os.environ["SPARKLESS_DUCKDB_SOURCES"] = previous_sources
            if previous_spark_home is not None:
                os.environ["SPARK_HOME"] = previous_spark_home

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8"
        )
    print(
        json.dumps(
            {
                "candidate_passed": report["candidate_passed"],
                "cases": list(report["cases"]),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
