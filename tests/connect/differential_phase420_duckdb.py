"""Compare the DuckDB Connect execution subset with pinned Spark 4.0.5.

The test uses the unmodified pyspark-client API against both servers. It records only
schemas and result values from deterministic fixtures; server logs stay in a temporary
directory unless a failure is raised.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
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
from pyspark.sql.functions import col
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
        "small_value SMALLINT, integer_value INTEGER, bigint_value BIGINT, "
        "decimal_value DECIMAL(18, 6), timestamp_value TIMESTAMP, note STRING)"
    ).collect()
    spark.sql(
        "INSERT INTO phase420_types VALUES "
        "(-32768, -2147483648, -9223372036854775808, -999999999999.999999, "
        "CAST('1969-12-31 23:59:59.999999' AS TIMESTAMP), NULL), "
        "(32767, 2147483647, 9223372036854775807, 999999999999.999999, "
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
        "empty_range_projection": spark.range(0).selectExpr("id AS value"),
        "duplicate_projection_labels": spark.range(3).selectExpr(
            "id AS value", "id + 1 AS value"
        ),
        "alias_metadata": spark.range(3).select(
            col("id").alias("metadata_value", metadata={"origin": "phase420"})
        ),
        "local_nullable_projection": local.selectExpr("id + 1 AS next_id", "label"),
        "local_nullable_filter": local.filter("id IS NOT NULL").select("id", "label"),
        "grouped_count_sum": local.groupBy("id").count(),
        "grouped_sum": local.groupBy("id").sum("amount"),
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
        "sql_expression": spark.sql(
            "SELECT id + 1 AS next_id FROM phase420_events WHERE id >= 2"
        ),
        "sql_table_read": spark.sql(
            "SELECT id, label FROM phase420_events WHERE id >= 2"
        ),
        "typed_boundary_table": spark.table("phase420_types"),
        "decimal_boundary_table": spark.table("phase420_decimal_edges"),
        "empty_typed_table": spark.table("phase420_types").filter(
            "small_value > 32767"
        ),
        "shared_duckdb_owner_table": spark.table("owner_witness"),
        "typed_local_result": typed_local,
    }
    results = {}
    for name, frame in frames.items():
        print(f"checking result/schema: {name}", flush=True)
        results[name] = {"schema": schema_snapshot(frame), "rows": row_snapshot(frame)}
    return results


def assert_unsupported_forms_rejected(spark: Any) -> list[str]:
    cases = {
        "sql_order_by": (
            lambda: spark.sql("SELECT id FROM phase420_events ORDER BY id").collect(),
            "CTEs, ordering, OFFSET",
        ),
        "sql_table_function": (
            lambda: spark.sql("SELECT * FROM range(5)").collect(),
            "table functions, table hints",
        ),
        "left_outer_join": (
            lambda: spark.range(2).join(spark.range(2), "id", "left_outer").collect(),
            "inner equijoins and USING joins only",
        ),
    }
    for name, (operation, expected_message) in cases.items():
        try:
            operation()
        except Exception as error:
            if expected_message not in str(error):
                raise AssertionError(
                    f"{name} failed with an unexpected error instead of {expected_message!r}: {error}"
                ) from error
        else:
            raise AssertionError(f"{name} unexpectedly succeeded")
    return sorted(cases)


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
                rejected_forms = assert_unsupported_forms_rejected(candidate_session)
                reference_results = workload(
                    reference_session, owner_table_exists=False
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
                    "cases": sorted(candidate_results),
                    "unsupported_forms_rejected": rejected_forms,
                    "duckdb_query_witness_count": len(witness_events),
                    "duckdb_committed_write_witness_count": len(write_witness_events),
                    "matched_results_and_schemas": candidate_results,
                }
                if args.output:
                    args.output.parent.mkdir(parents=True, exist_ok=True)
                    args.output.write_text(
                        json.dumps(report, indent=2, sort_keys=True) + "\n",
                        encoding="utf-8",
                    )
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
