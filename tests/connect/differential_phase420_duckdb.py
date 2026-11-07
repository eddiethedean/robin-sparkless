"""Compare the DuckDB Connect execution subset with pinned Spark 4.0.5.

The test uses the unmodified pyspark-client API against both servers. It records only
schemas and result values from deterministic fixtures; server logs stay in a temporary
directory unless a failure is raised.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import importlib.metadata
import json
import math
import os
import platform
from pathlib import Path
import re
import signal
import socket
import subprocess
import sys
import tempfile
import time
from datetime import date, datetime, timezone
from typing import Any

import duckdb
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit, sum as spark_sum
from sparkless_server_duckdb import SparklessConnect


ORACLE_ARCHIVE_SHA512 = (
    "4cc2e43959b5e0a67b7d895e1aa1fb3ab05fa6ad4a5912de76236bb9b70f9557"
    "b20837b05ee63474798e927130e72ed6d1fadcd273a71e65c70eac18771c0d6b"
)


def free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def wait_for_port(
    address: tuple[str, int], process: subprocess.Popen[str], timeout: float
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(
                f"Spark reference launcher exited with {process.returncode}"
            )
        try:
            with socket.create_connection(address, timeout=0.25):
                return
        except OSError:
            time.sleep(0.1)
    raise TimeoutError(f"Spark reference listener did not become ready at {address}")


def stop_process_group(process: subprocess.Popen[str]) -> None:
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        process.wait(timeout=10)
        return
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        process.poll()
        members = subprocess.run(
            ["ps", "-axo", "pgid=,stat="], check=True, text=True, capture_output=True
        ).stdout.splitlines()
        if not any(
            len(fields := line.split()) == 2
            and int(fields[0]) == process.pid
            and not fields[1].startswith("Z")
            for line in members
        ):
            process.wait(timeout=10)
            return
        time.sleep(0.1)
    os.killpg(process.pid, signal.SIGKILL)
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        raise RuntimeError("Spark reference process group did not stop after SIGKILL")


def schema_snapshot(dataframe: Any) -> dict[str, Any]:
    return json.loads(dataframe.schema.json())


def row_snapshot(dataframe: Any) -> list[str]:
    return sorted(json.dumps(list(row), default=str) for row in dataframe.collect())


def prepare_table(spark: Any, *, owner_table_exists: bool) -> None:
    spark.sql("CREATE TABLE phase420_events (id BIGINT, label STRING)").collect()
    spark.sql(
        "INSERT INTO phase420_events VALUES (1, 'one'), (2, NULL), (3, 'three')"
    ).collect()
    spark.sql(
        "CREATE TABLE phase420_types ("
        "byte_value TINYINT, small_value SMALLINT, integer_value INTEGER, bigint_value BIGINT, "
        "decimal_value DECIMAL(18, 6), timestamp_value TIMESTAMP, note STRING)"
    ).collect()
    spark.sql(
        "INSERT INTO phase420_types VALUES "
        "(-128, -32768, -2147483648, -9223372036854775808, -999999999999.999999, "
        "CAST('1969-12-31 23:59:59.999999' AS TIMESTAMP), NULL), "
        "(127, 32767, 2147483647, 9223372036854775807, 999999999999.999999, "
        "CAST('2026-10-30 12:34:56.123456' AS TIMESTAMP), 'upper')"
    ).collect()
    spark.sql(
        "CREATE TABLE phase420_decimal_edges ("
        "decimal_one DECIMAL(1, 0), decimal_whole DECIMAL(38, 0), "
        "decimal_fraction DECIMAL(38, 38))"
    ).collect()
    spark.sql(
        "INSERT INTO phase420_decimal_edges VALUES "
        "(CAST('1' AS DECIMAL(1, 0)), "
        "CAST('99999999999999999999999999999999999999' AS DECIMAL(38, 0)), "
        "CAST('0.99999999999999999999999999999999999999' AS DECIMAL(38, 38))), "
        "(CAST('-1' AS DECIMAL(1, 0)), "
        "CAST('-99999999999999999999999999999999999999' AS DECIMAL(38, 0)), "
        "CAST('-0.99999999999999999999999999999999999999' AS DECIMAL(38, 38)))"
    ).collect()
    if not owner_table_exists:
        spark.sql("CREATE TABLE phase420_non_finite (score DOUBLE)").collect()
        spark.sql(
            "INSERT INTO phase420_non_finite VALUES (CAST('NaN' AS DOUBLE))"
        ).collect()
        spark.sql("CREATE DATABASE phase420_namespace").collect()
        spark.sql(
            "CREATE TABLE phase420_namespace.namespaced_events "
            "(id BIGINT, label STRING)"
        ).collect()
        spark.sql(
            "INSERT INTO phase420_namespace.namespaced_events "
            "VALUES (7, 'seven'), (8, 'eight')"
        ).collect()
    if not owner_table_exists:
        spark.sql("CREATE TABLE owner_witness (ready BOOLEAN)").collect()
        spark.sql("INSERT INTO owner_witness VALUES (true)").collect()


def workload(spark: Any, *, owner_table_exists: bool) -> dict[str, dict[str, Any]]:
    prepare_table(spark, owner_table_exists=owner_table_exists)
    local = spark.createDataFrame(
        [(1, "one", 2), (2, None, 4), (None, "three", 8)],
        "id BIGINT, label STRING, amount BIGINT",
    )
    typed_local = spark.createDataFrame(
        [
            (
                1.25,
                3.5,
                date(1970, 1, 1),
                datetime(2026, 10, 30, 12, 34, 56, 123456),
                b"\x00\xff",
                True,
            )
        ],
        "float_value FLOAT, double_value DOUBLE, date_value DATE, "
        "timestamp_ntz_value TIMESTAMP_NTZ, binary_value BINARY, "
        "boolean_value BOOLEAN",
    )
    left = spark.range(4).alias("left_rows")
    right = spark.range(3).alias("right_rows")
    frames = {
        "range_filter_project_limit": spark.range(8)
        .filter("id >= 2")
        .selectExpr("id", "id + 1 AS next_id")
        .limit(3),
        "range_partitions": spark.range(0, 6, 1, 3),
        "empty_range_projection": spark.range(0).selectExpr("id AS value"),
        "negative_step_range": spark.range(9, -1, -2),
        "empty_negative_step_range": spark.range(0, 4, -1),
        "duplicate_projection_labels": spark.range(3).selectExpr(
            "id AS value", "id + 1 AS value"
        ),
        "scalar_literal_matrix": spark.range(1).select(
            lit(7).alias("integer_value"),
            lit(8_000_000_000).alias("long_value"),
            lit(True).alias("boolean_value"),
            lit(1.25).alias("double_value"),
            lit("sparkless").alias("string_value"),
        ),
        "scalar_operator_matrix": spark.range(1, 6).selectExpr(
            "+id AS positive",
            "-id AS negative",
            "id + 2 AS added",
            "id - 1 AS subtracted",
            "id * 2 AS multiplied",
            "id / 2 AS divided",
            "id % 2 AS remainder",
            "abs(id - 3) AS distance",
        ),
        "boolean_operator_matrix": spark.range(8)
        .filter(((col("id") == 0) | (col("id") >= 6)) & (col("id") <= 7))
        .select("id"),
        "alias_metadata": spark.range(3).select(
            col("id").alias("metadata_value", metadata={"origin": "phase420"})
        ),
        "local_nullable_projection": local.selectExpr("id + 1 AS next_id", "label"),
        "local_nullable_filter": local.filter("id IS NOT NULL").select("id", "label"),
        "null_literal_filter": local.filter(col("id") == lit(None)).select("id"),
        "local_null_predicates": local.select(
            col("label").isNull().alias("label_is_null"),
            col("label").isNotNull().alias("label_is_not_null"),
            (~col("id").isNull()).alias("id_is_not_null"),
        ),
        "grouped_count_sum": local.groupBy("id").count(),
        "grouped_sum": local.groupBy("id").sum("amount"),
        "global_count_sum": local.agg(
            count("id").alias("non_null_ids"),
            count(lit(1)).alias("all_rows"),
            spark_sum("amount").alias("total_amount"),
        ),
        "global_count_star": local.agg(count("*").alias("all_rows")),
        "empty_global_count_sum": local.filter("id > 99").agg(
            count("id").alias("non_null_ids"),
            count(lit(1)).alias("all_rows"),
            spark_sum("amount").alias("total_amount"),
        ),
        "empty_grouped_count": local.filter("id > 99").groupBy("id").count(),
        "grouped_decimal_sum": spark.table("phase420_types")
        .groupBy("note")
        .sum("decimal_value"),
        "inner_join_raw": left.join(right, left.id == right.id),
        "inner_join": left.join(right, left.id == right.id).select(left.id.alias("id")),
        "filtered_join_projection": (
            left.join(right, left.id == right.id)
            .filter(left.id >= 1)
            .select(left.id.alias("id"))
        ),
        "inner_using_join": spark.range(4).join(spark.range(3), "id"),
        "case_insensitive_table_and_column": spark.sql(
            "SELECT ID, LABEL FROM PHASE420_EVENTS WHERE ID = 1"
        ),
        "sql_cross_join": spark.sql(
            "SELECT events.id, owner.ready FROM phase420_events AS events "
            "CROSS JOIN owner_witness AS owner"
        ),
        "sql_inner_join_on": spark.sql(
            "SELECT left_rows.id AS left_id, right_rows.label AS right_label "
            "FROM phase420_events AS left_rows INNER JOIN phase420_events AS right_rows "
            "ON left_rows.id = right_rows.id"
        ),
        "sql_inner_join_using": spark.sql(
            "SELECT id FROM phase420_events AS left_rows "
            "INNER JOIN phase420_events AS right_rows USING (id)"
        ),
        "sql_star_projection": spark.sql("SELECT * FROM phase420_events"),
        "sql_qualified_star_projection": spark.sql(
            "SELECT events.* FROM phase420_events AS events"
        ),
        "sql_expression": spark.sql(
            "SELECT id + 1 AS next_id FROM phase420_events WHERE id >= 2"
        ),
        "sql_operator_function_matrix": spark.sql(
            "SELECT +id AS positive, -id AS negative, id + 2 AS added, "
            "id - 1 AS subtracted, id * 2 AS multiplied, "
            "id / 2 AS divided, id % 2 AS remainder, ABS(id - 2) AS distance, "
            "id = 1 AS equal_one, id != 1 AS not_equal_one, id <> 1 AS alternate_not_equal_one, "
            "id > 1 AS greater_than_one, id >= 1 AS at_least_one, "
            "id < 3 AS less_than_three, id <= 3 AS at_most_three, "
            "1.2 = '1.24' AS decimal_string_comparison, "
            "'2147483648' > id AS wide_integer_string_comparison, "
            "NOT id = 2 AS not_two, label IS NULL AS label_is_null, "
            "COUNT(*) AS rows, SUM(id) AS id_sum "
            "FROM phase420_events WHERE id = 1 OR id >= 3 AND id <= 3 "
            "GROUP BY id, label"
        ),
        "sql_group_by_expression": spark.sql(
            "SELECT id + 1 AS shifted, COUNT(*) AS rows FROM phase420_events "
            "GROUP BY id + 1"
        ),
        "sql_derived_relation": spark.sql(
            "SELECT nested.id FROM (SELECT id FROM phase420_events WHERE id <= 2) AS nested "
            "WHERE nested.id >= 1 LIMIT 2"
        ),
        "sql_table_read": spark.sql(
            "SELECT id, label FROM phase420_events WHERE id >= 2"
        ),
        "typed_boundary_table": spark.table("phase420_types"),
        "decimal_boundary_table": spark.table("phase420_decimal_edges"),
        "empty_typed_table": spark.table("phase420_types").filter(
            "small_value > 32767"
        ),
        "multipart_named_table": spark.table("phase420_namespace.namespaced_events"),
        "multipart_sql_table": spark.sql(
            "SELECT id, label FROM phase420_namespace.namespaced_events WHERE id >= 7"
        ),
        "multi_batch_range": spark.range(40_000).select("id"),
        "shared_duckdb_owner_table": spark.table("owner_witness"),
        "typed_local_result": typed_local,
    }
    results = {}
    for name, frame in frames.items():
        print(f"checking result/schema: {name}", flush=True)
        results[name] = {"schema": schema_snapshot(frame), "rows": row_snapshot(frame)}
    return results


def assert_configuration_profile(candidate: Any) -> dict[str, str]:
    expected = {
        "spark.sql.ansi.enabled": "true",
        "spark.sql.caseSensitive": "false",
        "spark.sql.execution.arrow.useLargeVarTypes": "false",
        "spark.sql.execution.pandas.convertToArrowArraySafely": "false",
        "spark.sql.execution.pandas.inferPandasDictAsMap": "false",
        "spark.sql.pyspark.inferNestedDictAsStruct.enabled": "false",
        "spark.sql.pyspark.legacy.inferArrayTypeFromFirstElement.enabled": "false",
        "spark.sql.pyspark.legacy.inferMapTypeFromFirstPair.enabled": "false",
        "spark.sql.session.localRelationCacheThreshold": "67108864",
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.timestampType": "TIMESTAMP_LTZ",
    }
    actual = {key: candidate.conf.get(key) for key in expected}
    if actual != expected:
        raise AssertionError(
            "DuckDB backend configuration does not match its frozen Spark profile: "
            f"{actual!r}"
        )
    if candidate.conf.isModifiable("spark.sql.ansi.enabled"):
        raise AssertionError("the frozen Spark SQL configuration must be immutable")
    if candidate.conf.get("sparkless.phase420.missing", "fallback") != "fallback":
        raise AssertionError("GetWithDefault did not return its fallback value")
    all_values = candidate.conf.getAll
    if any(all_values.get(key) != value for key, value in expected.items()):
        raise AssertionError("GetAll did not expose the frozen Spark SQL profile")
    candidate.conf.set("spark.sql.ansi.enabled", "true")
    candidate.conf.unset("spark.sql.ansi.enabled")
    if candidate.conf.get("spark.sql.ansi.enabled") != "true":
        raise AssertionError("Unset changed a fixed effective Spark configuration")
    try:
        candidate.conf.set("spark.sql.ansi.enabled", "false")
    except Exception as error:
        if "not modifiable" not in str(error).lower():
            raise AssertionError(
                "a changed frozen configuration value failed for an unrelated reason"
            ) from error
    else:
        raise AssertionError("the server accepted a change to its frozen SQL profile")
    return actual


def assert_unsupported_forms_rejected(spark: Any) -> list[str]:
    cases = {
        "sql_order_by": (
            lambda: spark.sql("SELECT id FROM phase420_events ORDER BY id").collect(),
            "CTEs, ordering, OFFSET",
        ),
        "sql_offset": (
            lambda: spark.sql(
                "SELECT id FROM phase420_events LIMIT 1 OFFSET 1"
            ).collect(),
            "OFFSET",
        ),
        "sql_multiple_statements": (
            lambda: spark.sql("SELECT 1; SELECT 2").collect(),
            "expected exactly one statement",
        ),
        "sql_table_function": (
            lambda: spark.sql("SELECT * FROM range(5)").collect(),
            "table functions, table hints",
        ),
        "non_finite_double": (
            lambda: spark.table("phase420_non_finite").collect(),
            "non-finite",
        ),
        "unsupported_update_command": (
            lambda: spark.sql("UPDATE phase420_events SET id = 99").collect(),
            "only create table and insert into values are enabled",
        ),
        "left_outer_join": (
            lambda: spark.range(2).join(spark.range(2), "id", "left_outer").collect(),
            "inner equijoins and USING joins only",
        ),
        "distinct_function": (
            lambda: spark.table("phase420_events").distinct().collect(),
            "not enabled",
        ),
        "sql_distinct": (
            lambda: spark.sql("SELECT DISTINCT id FROM phase420_events").collect(),
            "distinct",
        ),
        "sql_having": (
            lambda: spark.sql(
                "SELECT id, COUNT(*) AS n FROM phase420_events GROUP BY id HAVING COUNT(*) > 0"
            ).collect(),
            "having",
        ),
        "sql_cte": (
            lambda: spark.sql(
                "WITH source AS (SELECT id FROM phase420_events) SELECT id FROM source"
            ).collect(),
            "CTEs",
        ),
        "sql_parameterized_relation": (
            lambda: spark.sql(
                "SELECT * FROM phase420_events WHERE id = :id", args={"id": 1}
            ).collect(),
            "parameter",
        ),
        "sql_cast_expression": (
            lambda: spark.sql(
                "SELECT CAST(id AS BIGINT) FROM phase420_events"
            ).collect(),
            "composite SQL expressions are outside the enabled subset",
        ),
        "sql_count_distinct": (
            lambda: spark.sql(
                "SELECT COUNT(DISTINCT id) FROM phase420_events"
            ).collect(),
            "SQL function is not enabled",
        ),
        "sql_sum_expression": (
            lambda: spark.sql("SELECT SUM(id + 1) FROM phase420_events").collect(),
            "SQL SUM requires a supported numeric input column",
        ),
        "sql_union": (
            lambda: spark.sql(
                "SELECT id FROM phase420_events UNION SELECT id FROM phase420_events"
            ).collect(),
            "single SELECT body",
        ),
        "sql_insert_select": (
            lambda: spark.sql(
                "INSERT INTO phase420_events SELECT id, label FROM phase420_events"
            ).collect(),
            "INSERT accepts literal VALUES only",
        ),
        "sql_create_table_as_select": (
            lambda: spark.sql("CREATE TABLE phase420_ctas AS SELECT 1 AS id").collect(),
            "CREATE TABLE accepts only typed columns",
        ),
        "null_type_projection": (
            lambda: spark.range(1).select(lit(None).alias("null_value")).collect(),
            "outside the initial Connect type set",
        ),
        "nested_list_table": (
            lambda: spark.table("phase420_nested").collect(),
            "outside the initial Connect type set",
        ),
    }
    for name, (operation, expected_message) in cases.items():
        try:
            operation()
        except Exception as error:
            if expected_message.lower() not in str(error).lower():
                raise AssertionError(
                    f"{name} failed with an unexpected error instead of {expected_message!r}: {error}"
                ) from error
        else:
            raise AssertionError(f"{name} unexpectedly succeeded")
    if any(
        name in cases
        for name in (
            "sql_insert_select",
            "sql_create_table_as_select",
            "unsupported_update_command",
        )
    ):
        ids = sorted(
            row.id for row in spark.table("phase420_events").select("id").collect()
        )
        if ids != [1, 2, 3]:
            raise AssertionError(f"a rejected write changed the source table: {ids!r}")
    if "sql_create_table_as_select" in cases:
        try:
            spark.table("phase420_ctas").schema
        except Exception:
            pass
        else:
            raise AssertionError("rejected CTAS unexpectedly created its target table")
    return sorted(cases)


def assert_reference_non_finite_double(reference: Any) -> str:
    rows = reference.table("phase420_non_finite").collect()
    if len(rows) != 1 or not math.isnan(rows[0].score):
        raise AssertionError(f"Spark reference should return the NaN fixture: {rows!r}")
    return (
        "Spark 4.0.5 returns NaN; Sparkless keeps the inherited finite-DOUBLE boundary"
    )


def assert_ansi_overflow_errors(candidate: Any, reference: Any) -> dict[str, Any]:
    expression = "id + 1 AS value"
    errors = {}
    for name, spark in (("sparkless_duckdb", candidate), ("spark_4_0_5", reference)):
        try:
            spark.createDataFrame([(9223372036854775807,)], "id BIGINT").selectExpr(
                expression
            ).collect()
        except Exception as error:
            message = str(error).lower()
            if not any(term in message for term in ("overflow", "out of range")):
                raise AssertionError(
                    f"{name} failed the ANSI overflow case for an unrelated reason: {error}"
                ) from error
            errors[name] = type(error).__name__
        else:
            raise AssertionError(f"{name} did not reject signed BIGINT overflow")
    return {
        "case": "BIGINT_MAX DataFrame projection: " + expression,
        "both_rejected_as_overflow": True,
        "error_types": errors,
    }


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--spark-home", type=Path, required=True)
    parser.add_argument("--spark-archive", type=Path, required=True)
    parser.add_argument("--server-bin", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    if importlib.metadata.version("pyspark-client") != "4.0.5":
        parser.error("the unmodified Connect client must be pyspark-client==4.0.5")
    if importlib.metadata.version("duckdb") != "1.5.1":
        parser.error("the standalone DuckDB client must be duckdb==1.5.1")
    start_script = args.spark_home / "sbin" / "start-connect-server.sh"
    release = args.spark_home / "RELEASE"
    if not start_script.is_file() or not release.is_file():
        parser.error(
            "--spark-home must be an extracted Spark 4.0.5 Connect distribution"
        )
    if not args.server_bin.is_file():
        parser.error(f"Sparkless server binary does not exist: {args.server_bin}")
    archive_digest = hashlib.sha512()
    with args.spark_archive.open("rb") as archive:
        for block in iter(lambda: archive.read(1024 * 1024), b""):
            archive_digest.update(block)
    archive_hash = archive_digest.hexdigest()
    if archive_hash != ORACLE_ARCHIVE_SHA512:
        parser.error("--spark-archive does not match the pinned Spark 4.0.5 archive")
    release_text = release.read_text(encoding="utf-8").strip()
    if not release_text.startswith("Spark 4.0.5 (git revision b472a3aa3c4"):
        parser.error(f"unexpected Apache Spark reference version: {release_text}")

    with tempfile.TemporaryDirectory(
        prefix="sparkless-phase420-differential-"
    ) as temporary:
        run_dir = Path(temporary)
        pid_dir = run_dir / "pids"
        log_dir = run_dir / "logs"
        warehouse = run_dir / "warehouse"
        pid_dir.mkdir()
        log_dir.mkdir()
        warehouse.mkdir()
        reference_port = free_port()
        reference_address = ("127.0.0.1", reference_port)
        reference_log_path = run_dir / "spark-reference.log"
        reference_log = reference_log_path.open("w", encoding="utf-8")
        reference_env = os.environ.copy()
        reference_env.update(
            {
                "SPARK_HOME": str(args.spark_home.resolve()),
                "SPARK_PID_DIR": str(pid_dir),
                "SPARK_LOG_DIR": str(log_dir),
                "SPARK_IDENT_STRING": f"sparkless-phase420-{os.getpid()}",
                "SPARK_LOCAL_IP": "127.0.0.1",
                "SPARK_LOCAL_HOSTNAME": "localhost",
                "TZ": "UTC",
            }
        )
        reference_process = subprocess.Popen(
            [
                str(start_script),
                "--wait",
                "--conf",
                f"spark.connect.grpc.binding.port={reference_port}",
                "--conf",
                "spark.sql.session.timeZone=UTC",
                "--conf",
                "spark.sql.ansi.enabled=true",
                "--conf",
                "spark.sql.caseSensitive=false",
                "--conf",
                "spark.sql.execution.arrow.useLargeVarTypes=false",
                "--conf",
                f"spark.sql.warehouse.dir={warehouse.as_uri()}",
            ],
            cwd=args.spark_home,
            env=reference_env,
            stdout=reference_log,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=True,
        )

        candidate_driver_path = run_dir / "duckdb-execution.duckdb"
        with duckdb.connect(str(candidate_driver_path)) as connection:
            connection.execute("CREATE TABLE owner_witness (ready BOOLEAN)")
            connection.execute("INSERT INTO owner_witness VALUES (true)")
            connection.execute("CREATE TABLE phase420_non_finite (score DOUBLE)")
            connection.execute(
                "INSERT INTO phase420_non_finite VALUES (CAST('NaN' AS DOUBLE))"
            )
            connection.execute("CREATE TABLE phase420_nested (items BIGINT[])")
            connection.execute("INSERT INTO phase420_nested VALUES ([1, 2])")
            connection.execute("CREATE SCHEMA phase420_namespace")
            connection.execute(
                "CREATE TABLE phase420_namespace.namespaced_events "
                "(id BIGINT, label VARCHAR)"
            )
            connection.execute(
                "INSERT INTO phase420_namespace.namespaced_events "
                "VALUES (7, 'seven'), (8, 'eight')"
            )
        options = {
            "executable_path": str(args.server_bin.resolve()),
            "startup_timeout": 90.0,
            "port": 0,
            "connect_port": 0,
            "env": {
                "SPARKLESS_EXECUTION_BACKEND": "duckdb",
                "SPARKLESS_CONNECT_TRACE": "1",
                "SPARKLESS_DUCKDB_EXECUTION_OWNER": json.dumps(
                    {"mode": "local", "database_path": str(candidate_driver_path)}
                ),
            },
        }
        candidate = SparklessConnect(server_options=options)
        reference_session = None
        try:
            wait_for_port(reference_address, reference_process, timeout=240)
            with candidate as candidate_session:
                reference_url = f"sc://127.0.0.1:{reference_port}"
                reference_session = SparkSession.builder.remote(reference_url).create()
                candidate_results = workload(candidate_session, owner_table_exists=True)
                configuration_profile = assert_configuration_profile(candidate_session)
                rejected_forms = assert_unsupported_forms_rejected(candidate_session)
                reference_results = workload(
                    reference_session, owner_table_exists=False
                )
                reference_configuration = {
                    key: reference_session.conf.get(key)
                    for key in configuration_profile
                }
                if reference_configuration != configuration_profile:
                    raise AssertionError(
                        "Spark reference and DuckDB candidate use different configuration profiles: "
                        f"candidate={configuration_profile!r}, "
                        f"reference={reference_configuration!r}"
                    )
                non_finite_reference = assert_reference_non_finite_double(
                    reference_session
                )
                ansi_overflow = assert_ansi_overflow_errors(
                    candidate_session, reference_session
                )
                server = candidate._server
                trace = "" if server is None else server.diagnostics
                witness_events = re.findall(
                    r"connect_trace event=duckdb_execution_witness "
                    r"engine=duckdb operation=query outcome=completed rows=\d+ batches=\d+",
                    trace,
                )
                write_witness_events = re.findall(
                    r"connect_trace event=duckdb_execution_witness "
                    r"engine=duckdb operation=write outcome=committed rows=0 batches=0",
                    trace,
                )
                witness_deadline = time.monotonic() + 5
                while (
                    len(witness_events) < len(candidate_results)
                    or len(write_witness_events) < 4
                ) and time.monotonic() < witness_deadline:
                    time.sleep(0.05)
                    trace = "" if server is None else server.diagnostics
                    witness_events = re.findall(
                        r"connect_trace event=duckdb_execution_witness "
                        r"engine=duckdb operation=query outcome=completed rows=\d+ batches=\d+",
                        trace,
                    )
                    write_witness_events = re.findall(
                        r"connect_trace event=duckdb_execution_witness "
                        r"engine=duckdb operation=write outcome=committed rows=0 batches=0",
                        trace,
                    )
                if len(witness_events) < len(candidate_results):
                    raise AssertionError(
                        "DuckDB engine trace is missing one or more successful query witnesses"
                    )
                if len(write_witness_events) < 4:
                    raise AssertionError(
                        "DuckDB engine trace is missing a committed CREATE/INSERT witness"
                    )
                differences = {}
                for name in sorted(candidate_results):
                    if candidate_results[name] != reference_results[name]:
                        differences[name] = {
                            "sparkless": candidate_results[name],
                            "spark": reference_results[name],
                        }
                if differences:
                    raise AssertionError(
                        "Phase 4.20 Spark result/schema mismatch: "
                        + json.dumps(differences, sort_keys=True)
                    )
                report = {
                    "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                    "client": "pyspark-client==4.0.5",
                    "duckdb_client": "duckdb==1.5.1",
                    "pyarrow_client": f"pyarrow=={importlib.metadata.version('pyarrow')}",
                    "python": sys.version.split()[0],
                    "platform": platform.platform(),
                    "reference_release": release_text,
                    "reference_archive_sha512": archive_hash,
                    "sparkless_server_version": subprocess.run(
                        [str(args.server_bin.resolve()), "--version"],
                        check=True,
                        capture_output=True,
                        text=True,
                    ).stdout.strip(),
                    "sparkless_server_binary": str(args.server_bin.resolve()),
                    "sparkless_server_sha256": file_sha256(args.server_bin),
                    "runner_sha256": file_sha256(Path(__file__).resolve()),
                    "settings": {
                        "spark.sql.session.timeZone": "UTC",
                        "spark.sql.ansi.enabled": "true",
                        "spark.sql.caseSensitive": "false",
                    },
                    "configuration_profile": configuration_profile,
                    "reference_configuration_profile": reference_configuration,
                    "cases": sorted(candidate_results),
                    "unsupported_forms_rejected": rejected_forms,
                    "non_finite_double_boundary": non_finite_reference,
                    "ansi_overflow_boundary": ansi_overflow,
                    "duckdb_query_witness_count": len(witness_events),
                    "duckdb_committed_write_witness_count": len(write_witness_events),
                    "matched_results_and_schemas": candidate_results,
                }
                if args.output:
                    args.output.parent.mkdir(parents=True, exist_ok=True)
                    serialized_report = (
                        json.dumps(report, indent=2, sort_keys=True) + "\n"
                    ).encode("utf-8")
                    if args.output.suffix == ".gz":
                        args.output.write_bytes(
                            gzip.compress(serialized_report, mtime=0)
                        )
                    else:
                        args.output.write_bytes(serialized_report)
                print(json.dumps(report, indent=2, sort_keys=True))
        finally:
            try:
                if reference_session is not None:
                    reference_session.stop()
            finally:
                try:
                    stop_process_group(reference_process)
                finally:
                    reference_log.close()


if __name__ == "__main__":
    main()
