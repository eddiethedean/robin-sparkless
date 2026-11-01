"""Exercise configured DuckDB sources through an unmodified public Connect client.

Standalone mode uses Python DuckDB to create a file fixture. Quack mode starts the bundled native
DuckDB 2.0.0 owner fixture passed with ``--quack-owner-bin``.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import importlib.metadata
import json
import os
import selectors
import subprocess
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
    parser.add_argument("--quack-owner-bin", type=Path)
    parser.add_argument("--quack-extension-path", type=Path)
    args = parser.parse_args()
    if bool(args.quack_owner_bin) != bool(args.quack_extension_path):
        parser.error(
            "--quack-owner-bin and --quack-extension-path must be supplied together"
        )

    previous_sources = os.environ.get("SPARKLESS_DUCKDB_SOURCES")
    owner: subprocess.Popen[str] | None = None
    standalone_database: Path | None = None
    with tempfile.TemporaryDirectory(prefix="sparkless-duckdb-source-") as temp_dir:
        if args.quack_owner_bin:
            assert args.quack_extension_path is not None
            owner = subprocess.Popen(
                [
                    str(args.quack_owner_bin.absolute()),
                    str(args.quack_extension_path.absolute()),
                ],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=None,
                text=True,
                bufsize=1,
            )
            with selectors.DefaultSelector() as selector:
                assert owner.stdout is not None
                selector.register(owner.stdout, selectors.EVENT_READ)
                if not selector.select(timeout=30):
                    owner.terminate()
                    owner.wait(timeout=10)
                    raise TimeoutError(
                        "DuckDB Quack fixture owner did not become ready"
                    )
                owner_line = owner.stdout.readline()
            if not owner_line:
                raise RuntimeError(
                    f"DuckDB Quack owner exited with status {owner.poll()}"
                )
            try:
                owner_info = json.loads(owner_line)
                source_config = [
                    {
                        "alias": "warehouse",
                        "quack_uri": owner_info["uri"],
                        "token": owner_info["token"],
                        "extension_path": str(args.quack_extension_path.absolute()),
                    }
                ]
            except Exception:
                owner.terminate()
                owner.wait(timeout=10)
                raise
        else:
            database_path = Path(temp_dir) / "warehouse.duckdb"
            standalone_database = database_path
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
            source_config = [
                {"alias": "warehouse", "database_path": str(database_path)}
            ]

        os.environ["SPARKLESS_DUCKDB_SOURCES"] = json.dumps(source_config)
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

                for statement in (
                    "INSERT INTO warehouse.main.events VALUES "
                    "(4, 30, 'delta', 99.00, true)",
                    "UPDATE warehouse.main.events SET label = 'changed' WHERE id = 1",
                    "DELETE FROM warehouse.main.events WHERE id = 1",
                    "CREATE TABLE warehouse.main.forbidden (id BIGINT)",
                    "CREATE TABLE IF NOT EXISTS warehouse.main.events (id BIGINT)",
                    "CREATE OR REPLACE TABLE warehouse.main.events "
                    "USING parquet AS SELECT 99 AS id",
                    "DROP TABLE warehouse.main.events",
                    "DROP TABLE IF EXISTS warehouse.main.events",
                    "ALTER TABLE warehouse.main.events ADD COLUMN forbidden STRING",
                    "TRUNCATE TABLE warehouse.main.events",
                ):
                    try:
                        spark.sql(statement).collect()
                    except Exception as error:
                        message = str(error).lower()
                        assert any(
                            marker in message
                            for marker in (
                                "unsupported",
                                "not supported",
                                "read-only",
                                "not implemented",
                                "parse error",
                                "only select",
                            )
                        ), error
                    else:
                        raise AssertionError(
                            f"DuckDB source write was accepted: {statement}"
                        )
                assert events.schema.fieldNames() == [
                    "id",
                    "bucket",
                    "label",
                    "amount",
                    "active",
                ], "a rejected DuckDB DDL statement changed the table schema"
                assert events.count() == 3, "a rejected source write changed the table"
                assert sorted(row.id for row in events.collect()) == [1, 2, 3]
                for mode in ("append", "overwrite"):
                    try:
                        events.write.mode(mode).saveAsTable("warehouse.main.events")
                    except Exception as error:
                        message = str(error).lower()
                        assert any(
                            marker in message
                            for marker in (
                                "unsupported",
                                "not supported",
                                "read-only",
                                "not implemented",
                            )
                        ), error
                    else:
                        raise AssertionError(f"DuckDB {mode} write was accepted")
                    assert events.count() == 3, (
                        f"a rejected {mode} write changed the table"
                    )

                try:
                    spark.range(1).write.mode("overwrite").saveAsTable(
                        "warehouse.main.created_by_sparkless"
                    )
                except Exception as error:
                    message = str(error).lower()
                    assert any(
                        marker in message
                        for marker in (
                            "unsupported",
                            "not supported",
                            "read-only",
                            "not implemented",
                        )
                    ), error
                else:
                    raise AssertionError(
                        "saveAsTable created a local table inside the DuckDB source namespace"
                    )

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
                    raise AssertionError(
                        "non-finite DuckDB DOUBLE values were accepted"
                    )
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
                assert events.agg(F.count("amount").alias("n")).collect()[0]["n"] == 2
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

                if standalone_database is not None:
                    # Analysis can precede execution. If the configured source disappears in
                    # between, fail the operation instead of silently resolving a local table
                    # or replaying against another source. Restoring it must leave the source
                    # connection lease usable for the next operation.
                    pending_read = spark.table("warehouse.main.events")
                    assert pending_read.schema.fieldNames() == [
                        "id",
                        "bucket",
                        "label",
                        "amount",
                        "active",
                    ]
                    unavailable_path = standalone_database.with_suffix(
                        ".duckdb.unavailable"
                    )
                    standalone_database.rename(unavailable_path)
                    try:
                        try:
                            pending_read.collect()
                        except Exception as error:
                            message = str(error).lower()
                            assert any(
                                marker in message
                                for marker in (
                                    "duckdb",
                                    "source",
                                    "database",
                                    "not found",
                                )
                            ), error
                        else:
                            raise AssertionError(
                                "a read succeeded after its configured DuckDB source disappeared"
                            )
                    finally:
                        unavailable_path.rename(standalone_database)
                    assert pending_read.count() == 3, (
                        "a source failure leaked its read lease or corrupted a later statement"
                    )

            source_mode = (
                "Quack shared owner"
                if owner
                else f"Python DuckDB fixture writer {duckdb.__version__}"
            )
            print(f"4.19 public Connect DuckDB source smoke passed ({source_mode})")
        finally:
            if previous_sources is None:
                os.environ.pop("SPARKLESS_DUCKDB_SOURCES", None)
            else:
                os.environ["SPARKLESS_DUCKDB_SOURCES"] = previous_sources
            if owner is not None:
                if owner.stdin is not None:
                    owner.stdin.write("stop\n")
                    owner.stdin.flush()
                    owner.stdin.close()
                owner.wait(timeout=15)


if __name__ == "__main__":
    main()
