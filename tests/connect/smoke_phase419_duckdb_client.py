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
import re
import selectors
import socket
import subprocess
import sys
import tempfile
from pathlib import Path
from urllib.parse import urlsplit

import duckdb
from pyspark.sql import functions as F
from pyspark.sql.connect.session import SparkSession as ConnectSession

from sparkless_server_duckdb import SparklessConnect, SparklessServer


def send_quack_owner_command(owner: subprocess.Popen[str], command: str) -> None:
    if owner.stdin is None or owner.stdout is None:
        raise RuntimeError("DuckDB Quack fixture owner control pipes are unavailable")
    owner.stdin.write(f"{command}\n")
    owner.stdin.flush()
    with selectors.DefaultSelector() as selector:
        selector.register(owner.stdout, selectors.EVENT_READ)
        if not selector.select(timeout=15):
            raise TimeoutError(f"DuckDB Quack fixture owner did not answer {command!r}")
        response = owner.stdout.readline().strip()
    expected_response = {"stop": "stopped", "start": "started"}.get(command)
    if response != expected_response:
        raise RuntimeError(
            f"DuckDB Quack fixture owner returned {response!r} for {command!r}"
        )


def run_source_replay_smoke(server_bin: Path) -> None:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        proxy_port = listener.getsockname()[1]
    server = SparklessServer(
        executable_path=str(server_bin),
        env={"SPARKLESS_CONNECT_TRACE": "1"},
    )
    proxy: subprocess.Popen[str] | None = None
    spark: ConnectSession | None = None
    try:
        server.start()
        upstream = urlsplit(server.spark_connect_url)
        if upstream.hostname is None or upstream.port is None:
            raise RuntimeError(f"invalid Spark Connect endpoint: {server.spark_connect_url}")
        proxy = subprocess.Popen(
            [
                sys.executable,
                str(Path(__file__).with_name("fault_proxy.py").resolve()),
                "--listen",
                f"127.0.0.1:{proxy_port}",
                "--upstream",
                f"{upstream.hostname}:{upstream.port}",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        if proxy.stdout is None:
            raise RuntimeError("fault proxy stdout was not captured")
        with selectors.DefaultSelector() as selector:
            selector.register(proxy.stdout, selectors.EVENT_READ)
            if not selector.select(timeout=30):
                raise TimeoutError("source replay fault proxy did not become ready")
            proxy_ready = proxy.stdout.readline().strip()
        if proxy_ready != f"fault_proxy ready address=127.0.0.1:{proxy_port}":
            raise RuntimeError(f"source replay fault proxy exited early: {proxy_ready!r}")

        producer_pattern = r"event=producer_started"
        reattach_pattern = r"event=rpc method=ReattachExecute"
        before = server.diagnostics
        producers_before = len(re.findall(producer_pattern, before))
        reattaches_before = len(re.findall(reattach_pattern, before))
        spark = ConnectSession.builder.remote(f"sc://127.0.0.1:{proxy_port}").create()
        values = [row.id for row in spark.table("warehouse.main.replay_events").collect()]
        if (
            len(values) != 50_000
            or values[0] != 0
            or values[-1] != 49_999
            or sum(values) != 1_249_975_000
        ):
            raise AssertionError("source replay returned incorrect rows")
        spark.stop()
        spark = None

        trace = server.diagnostics
        producer_delta = len(re.findall(producer_pattern, trace)) - producers_before
        reattach_delta = len(re.findall(reattach_pattern, trace)) - reattaches_before
        proxy.terminate()
        proxy_text, _ = proxy.communicate(timeout=10)
        proxy = None
        if (
            producer_delta != 1
            or reattach_delta < 1
            or "injected UNAVAILABLE" not in proxy_text
        ):
            raise AssertionError(
                "source query did not resume the original producer after disconnect: "
                f"producer_starts={producer_delta}, reattach_rpcs={reattach_delta}, "
                f"proxy_log={proxy_text!r}, server_trace={trace!r}"
            )
    finally:
        if spark is not None:
            spark.stop()
        if proxy is not None:
            proxy.terminate()
            proxy.wait(timeout=10)
        server.stop()


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
    owner_info: dict[str, str] | None = None
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
                    }
                ]
                packaged_extension = (
                    args.server_bin.resolve().parent / "quack.duckdb_extension"
                )
                if args.quack_extension_path.resolve() != packaged_extension.resolve():
                    source_config[0]["extension_path"] = str(
                        args.quack_extension_path.absolute()
                    )
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
                CREATE TABLE non_finite_events (score DOUBLE);
                INSERT INTO non_finite_events VALUES (CAST('NaN' AS DOUBLE));
                CREATE TABLE nested_events (items INTEGER[]);
                INSERT INTO nested_events VALUES ([1, 2]);
                CREATE TABLE oversized_events (payload VARCHAR);
                INSERT INTO oversized_events VALUES (repeat('x', 524289));
                CREATE TABLE replay_events AS
                    SELECT i::BIGINT AS id FROM range(50000) AS source_rows(i);
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
                    "INSERT OVERWRITE TABLE warehouse.main.events "
                    "SELECT id, bucket, label, amount, active "
                    "FROM warehouse.main.events",
                    "UPDATE warehouse.main.events SET label = 'changed' WHERE id = 1",
                    "DELETE FROM warehouse.main.events WHERE id = 1",
                    "CREATE TABLE warehouse.main.forbidden (id BIGINT)",
                    "CREATE TABLE IF NOT EXISTS warehouse.main.events (id BIGINT)",
                    "CREATE OR REPLACE TABLE warehouse.main.events "
                    "USING parquet AS SELECT 99 AS id",
                    "CREATE TABLE warehouse.main.copy LIKE warehouse.main.events",
                    "CREATE VIEW warehouse.main.forbidden_view "
                    "AS SELECT id FROM warehouse.main.events",
                    "CREATE OR REPLACE VIEW warehouse.main.events "
                    "AS SELECT 99 AS id",
                    "DROP TABLE warehouse.main.events",
                    "DROP TABLE IF EXISTS warehouse.main.events",
                    "DROP VIEW warehouse.main.events",
                    "CREATE SCHEMA warehouse.forbidden_schema",
                    "CREATE DATABASE warehouse.forbidden_database",
                    "DROP SCHEMA warehouse.main CASCADE",
                    "DROP DATABASE warehouse.main CASCADE",
                    "ALTER TABLE warehouse.main.events ADD COLUMN forbidden STRING",
                    "ALTER TABLE warehouse.main.events RENAME TO "
                    "warehouse.main.renamed_events",
                    "ALTER TABLE warehouse.main.events DROP COLUMN label",
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
                                "syntax",
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
                    2026, 10, 30, 12, 34, 56, 123456, tzinfo=timezone.utc
                ).timestamp()
                assert typed_row.seen_at.timestamp() == expected_instant
                assert typed_row.seen_at.microsecond == 123456

                integer_extrema = spark.table("warehouse.integer_boundary_events")
                assert [
                    (field.name, type(field.dataType).__name__, field.nullable)
                    for field in integer_extrema.schema.fields
                ] == [
                    ("small_min", "ShortType", False),
                    ("small_max", "ShortType", False),
                    ("int_min", "IntegerType", False),
                    ("int_max", "IntegerType", False),
                    ("bigint_min", "LongType", False),
                    ("bigint_max", "LongType", False),
                ]
                integer_row = integer_extrema.collect()[0]
                assert tuple(integer_row) == (
                    -32768,
                    32767,
                    -2147483648,
                    2147483647,
                    -(2**63),
                    2**63 - 1,
                )

                decimals = spark.table("warehouse.decimal_boundary_events")
                assert [
                    (
                        field.name,
                        field.dataType.precision,
                        field.dataType.scale,
                    )
                    for field in decimals.schema.fields
                ] == [
                    ("d1", 1, 0),
                    ("d9", 9, 0),
                    ("d18", 18, 6),
                    ("d38", 38, 0),
                    ("d38s38", 38, 38),
                ]
                decimal_rows = sorted(decimals.collect(), key=lambda row: row.d1)
                nines_38 = "9" * 38
                fractional_38 = "0." + nines_38
                assert [tuple(str(value) for value in row) for row in decimal_rows] == [
                    (
                        "-9",
                        "-999999999",
                        "-999999999999.999999",
                        f"-{nines_38}",
                        f"-{fractional_38}",
                    ),
                    (
                        "9",
                        "999999999",
                        "999999999999.999999",
                        nines_38,
                        fractional_38,
                    ),
                ]

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
                try:
                    spark.table("warehouse.oversized_events").collect()
                except Exception as error:
                    assert "512 kib" in str(error).lower(), error
                else:
                    raise AssertionError("oversized DuckDB VARCHAR values were accepted")

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
                else:
                    assert args.quack_owner_bin is not None
                    assert args.quack_extension_path is not None
                    assert owner_info is not None
                    subprocess.run(
                        [
                            str(args.quack_owner_bin.absolute()),
                            str(args.quack_extension_path.absolute()),
                            "--writer",
                            owner_info["uri"],
                            owner_info["token"],
                        ],
                        check=True,
                        capture_output=True,
                        text=True,
                        timeout=30,
                    )
                    assert events.count() == 4, (
                        "a commit from an independent Quack writer process was not visible "
                        "to the next Connect statement"
                    )

                    # Stop the shared Quack owner after analysis but before execution. The
                    # operation must fail without retrying against a different snapshot; a
                    # fresh operation must succeed after the same owner returns.
                    pending_read = spark.table("warehouse.main.events")
                    assert pending_read.schema.fieldNames() == [
                        "id",
                        "bucket",
                        "label",
                        "amount",
                        "active",
                    ]
                    send_quack_owner_command(owner, "stop")
                    try:
                        try:
                            pending_read.collect()
                        except Exception as error:
                            message = str(error).lower()
                            assert any(
                                marker in message
                                for marker in (
                                    "quack",
                                    "duckdb",
                                    "source",
                                    "connection",
                                    "refused",
                                )
                            ), error
                        else:
                            raise AssertionError(
                                "a read succeeded while its configured Quack owner was stopped"
                            )
                    finally:
                        send_quack_owner_command(owner, "start")
                    assert pending_read.count() == 4, (
                        "a Quack source failure leaked its read lease or corrupted a later statement"
                    )

            run_source_replay_smoke(args.server_bin.absolute())
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
                    owner.stdin.write("quit\n")
                    owner.stdin.flush()
                    owner.stdin.close()
                owner.wait(timeout=15)


if __name__ == "__main__":
    main()
