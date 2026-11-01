"""Measure a bounded standalone DuckDB source scan through Spark Connect.

Creates a temporary source with up to the configured statement row cap, runs a full-table count
through the packaged Connect server, and records server-process RSS while batches are decoded.
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
import hashlib
import importlib.metadata
import json
import math
import os
from pathlib import Path
import platform
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

try:
    import resource
except ImportError:  # Windows has no getrusage module.
    resource = None

import duckdb
from pyspark.sql.connect.session import SparkSession as ConnectSession

from sparkless.server import SparklessConnect


SOURCE_LIMITS = {
    "rows_per_statement": 1_000_000,
    "ipc_bytes_per_statement": 32 * 1024 * 1024,
    "decoded_bytes_per_statement": 64 * 1024 * 1024,
    "ipc_bytes_per_batch": 1024 * 1024,
    "rows_per_batch": 16_384,
    "connections_per_catalog": 4,
    "queued_operations_per_catalog": 16,
}
CONNECT_RESULT_LIMIT_BYTES = 8 * 1024 * 1024

SERVER_ENVIRONMENT = {
    "SPARKLESS_MAX_ACTIVE_OPERATIONS": "2",
    "SPARKLESS_MAX_PENDING_OPERATIONS": "16",
    "SPARKLESS_MAX_RANGE_ROWS": "10000000",
    "SPARKLESS_QUEUE_TIMEOUT_MS": "5000",
    "SPARKLESS_RESPONSE_TIMEOUT_MS": "60000",
    "SPARKLESS_CONNECT_TRACE": "0",
    "SPARKLESS_ARROW_TRACE": "0",
    "POLARS_MAX_THREADS": "4",
    "RAYON_NUM_THREADS": "4",
    "OMP_NUM_THREADS": "4",
    "TZ": "UTC",
}


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(fraction * len(ordered)) - 1)]


def clean_server_environment(active_operations: int) -> None:
    for key in tuple(os.environ):
        if key.startswith("SPARKLESS_"):
            os.environ.pop(key, None)
    os.environ.update(SERVER_ENVIRONMENT)
    os.environ["SPARKLESS_MAX_ACTIVE_OPERATIONS"] = str(active_operations)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server-bin", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--payload-bytes", type=int, default=16)
    parser.add_argument("--iterations", type=int, default=3)
    parser.add_argument(
        "--concurrent-readers",
        type=int,
        default=1,
        help="number of independent Spark Connect clients reading the same source",
    )
    parser.add_argument(
        "--slow-reader-delay-ms",
        type=int,
        default=0,
        help="pause after each Arrow-sized row group while consuming toLocalIterator()",
    )
    parser.add_argument(
        "--expect-rejection",
        choices=("ipc", "rows", "result"),
        help="record a successful check that the selected statement limit rejects this scan",
    )
    parser.add_argument(
        "--allow-result-rejection",
        action="store_true",
        help="record both completed scans and expected 8 MiB result-cap rejections",
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    max_rows = SOURCE_LIMITS["rows_per_statement"] + int(
        args.expect_rejection == "rows"
    )
    if not 1 <= args.rows <= max_rows:
        parser.error("--rows must be between 1 and the configured one-million-row cap")
    if args.expect_rejection == "rows" and args.rows != max_rows:
        parser.error("row-limit rejection requires one row above the configured cap")
    if not 0 <= args.payload_bytes <= 4096 or args.iterations < 1:
        parser.error("payload bytes must be 0..4096 and iterations must be positive")
    if not 1 <= args.concurrent_readers <= SOURCE_LIMITS["connections_per_catalog"]:
        parser.error(
            "concurrent readers must be between 1 and the configured source connection cap"
        )
    if not 0 <= args.slow_reader_delay_ms <= 1000:
        parser.error("slow-reader delay must be between 0 and 1000 milliseconds")
    if args.expect_rejection in ("ipc", "rows") and args.slow_reader_delay_ms:
        parser.error("source-limit rejection cannot be combined with a slow reader")
    if args.expect_rejection == "result" and not args.slow_reader_delay_ms:
        parser.error("result-limit rejection requires --slow-reader-delay-ms")
    if args.allow_result_rejection and not args.slow_reader_delay_ms:
        parser.error("result-limit recording requires --slow-reader-delay-ms")
    if args.allow_result_rejection and args.expect_rejection:
        parser.error(
            "--allow-result-rejection cannot be combined with --expect-rejection"
        )
    if args.expect_rejection in ("ipc", "rows") and args.concurrent_readers != 1:
        parser.error("IPC and row-limit rejection supports one reader only")
    server_bin = args.server_bin.absolute()
    if not server_bin.is_file():
        parser.error(f"server binary not found: {server_bin}")
    client_version = importlib.metadata.version("pyspark-client")
    if client_version != "4.0.5":
        parser.error(f"expected pyspark-client==4.0.5, found {client_version}")
    pyarrow_version = importlib.metadata.version("pyarrow")
    if pyarrow_version != "26.0.0":
        parser.error(f"expected pyarrow==26.0.0, found {pyarrow_version}")
    if duckdb.__version__ != "1.5.1":
        parser.error(f"expected duckdb==1.5.1, found {duckdb.__version__}")

    clean_server_environment(args.concurrent_readers)

    def run_action(spark: ConnectSession) -> int:
        dataframe = spark.table("warehouse.main.events")
        if not args.slow_reader_delay_ms:
            return dataframe.count()
        observed = 0
        id_sum = 0
        id_xor = 0
        for row in dataframe.toLocalIterator():
            if not 0 <= row.id < args.rows or len(row.payload) != args.payload_bytes:
                raise AssertionError(f"unexpected source row: {row!r}")
            observed += 1
            id_sum += row.id
            id_xor ^= row.id
            if observed % SOURCE_LIMITS["rows_per_batch"] == 0:
                time.sleep(args.slow_reader_delay_ms / 1000)
        upper = args.rows - 1
        expected_xor = (upper, 1, upper + 1, 0)[upper % 4]
        if (
            observed != args.rows
            or id_sum != args.rows * upper // 2
            or id_xor != expected_xor
        ):
            raise AssertionError(
                "source iterator returned incorrect ids: "
                f"rows={observed}, sum={id_sum}, xor={id_xor}"
            )
        return observed

    with tempfile.TemporaryDirectory(
        prefix="sparkless-phase419-duckdb-budget-"
    ) as work:
        database_path = Path(work) / "source.duckdb"
        fixture = duckdb.connect(str(database_path))
        fixture.execute(
            "CREATE TABLE events AS SELECT i::BIGINT AS id, "
            "substr(repeat(md5(i::VARCHAR), ?), 1, ?) AS payload "
            "FROM range(?) AS source_rows(i)",
            [max(1, (args.payload_bytes + 31) // 32), args.payload_bytes, args.rows],
        )
        fixture.close()
        source_config = [{"alias": "warehouse", "database_path": str(database_path)}]
        os.environ["SPARKLESS_DUCKDB_SOURCES"] = json.dumps(source_config)
        database_size_bytes = database_path.stat().st_size

        owner = SparklessConnect(server_options={"executable_path": str(server_bin)})
        samples_ms: list[float] = []
        batch_samples_ms: list[float] = []
        action_outcomes: list[dict[str, Any]] = []
        expected_rejection: str | None = None
        with owner as spark:
            if not isinstance(spark, ConnectSession):
                raise TypeError(
                    f"expected PySpark Connect session, got {type(spark)!r}"
                )
            server_process = getattr(getattr(owner, "_server", None), "_process", None)
            if server_process is None or server_process.poll() is not None:
                raise RuntimeError(
                    "SparklessConnect did not retain a running server process"
                )
            client_sessions = [spark]
            connect_url = getattr(owner._server, "spark_connect_url", None)
            if not connect_url:
                raise RuntimeError(
                    "SparklessConnect did not retain its Connect endpoint"
                )
            client_sessions.extend(
                ConnectSession.builder.remote(connect_url).create()
                for _ in range(args.concurrent_readers - 1)
            )

            def run_parallel() -> tuple[
                list[int | None], list[float], list[str | None], float
            ]:
                batch_started = time.perf_counter()

                def timed_action(
                    session: ConnectSession,
                ) -> tuple[int | None, float, str | None]:
                    started = time.perf_counter()
                    try:
                        count = run_action(session)
                        return count, (time.perf_counter() - started) * 1000, None
                    except Exception as error:
                        return (
                            None,
                            (time.perf_counter() - started) * 1000,
                            str(error),
                        )

                with ThreadPoolExecutor(max_workers=len(client_sessions)) as pool:
                    results = list(pool.map(timed_action, client_sessions))
                elapsed = (time.perf_counter() - batch_started) * 1000
                round_index = len(action_outcomes) // len(client_sessions)
                for reader_index, (count, duration_ms, error) in enumerate(results):
                    action_outcomes.append(
                        {
                            "phase": "warmup" if round_index == 0 else "measured",
                            "iteration": round_index,
                            "reader": reader_index,
                            "result": "rejected" if error else "completed",
                            "count": count,
                            "duration_ms": duration_ms,
                            "error": error,
                        }
                    )
                return (
                    [item[0] for item in results],
                    [item[1] for item in results],
                    [item[2] for item in results],
                    elapsed,
                )

            def validate_round(
                counts: list[int | None], errors: list[str | None]
            ) -> None:
                failures = [error for error in errors if error is not None]
                expected_messages = {
                    "ipc": (
                        "DuckDB source exceeds the "
                        f"{SOURCE_LIMITS['ipc_bytes_per_statement']}-byte operation limit"
                    ),
                    "rows": (
                        "DuckDB source exceeds the "
                        f"{SOURCE_LIMITS['rows_per_statement']}-row operation limit"
                    ),
                    "result": (
                        "Connect replay and sender buffers exceed "
                        f"{CONNECT_RESULT_LIMIT_BYTES} bytes for one operation"
                    ),
                }
                expected_message = expected_messages.get(args.expect_rejection)
                if failures:
                    result_limit_message = expected_messages["result"]
                    result_limit_failures = all(
                        result_limit_message in error for error in failures
                    )
                    successful_counts = [
                        count
                        for count, error in zip(counts, errors, strict=True)
                        if error is None
                    ]
                    if any(count != args.rows for count in successful_counts):
                        raise AssertionError(
                            f"source counts returned {successful_counts}, expected {args.rows} each"
                        )
                    if expected_message and all(
                        expected_message in error for error in failures
                    ):
                        raise RuntimeError(
                            f"{expected_message} "
                            f"(rejected {len(failures)} of {len(client_sessions)} readers)"
                        )
                    if args.allow_result_rejection and result_limit_failures:
                        return
                    raise RuntimeError(f"concurrent source action failed: {failures!r}")
                if args.expect_rejection:
                    raise AssertionError(
                        "scan succeeded, but an expected source limit was not enforced"
                    )
                if any(count != args.rows for count in counts):
                    raise AssertionError(
                        f"source counts returned {counts}, expected {args.rows} each"
                    )

            try:
                warm_counts, _, warm_errors, _ = run_parallel()
                validate_round(warm_counts, warm_errors)
                for _ in range(args.iterations):
                    counts, action_samples, errors, batch_elapsed = run_parallel()
                    samples_ms.extend(action_samples)
                    batch_samples_ms.append(batch_elapsed)
                    validate_round(counts, errors)
            except Exception as error:
                message = str(error)
                expected_messages = {
                    "ipc": (
                        "DuckDB source exceeds the "
                        f"{SOURCE_LIMITS['ipc_bytes_per_statement']}-byte operation limit"
                    ),
                    "rows": (
                        "DuckDB source exceeds the "
                        f"{SOURCE_LIMITS['rows_per_statement']}-row operation limit"
                    ),
                    "result": (
                        "Connect replay and sender buffers exceed "
                        f"{CONNECT_RESULT_LIMIT_BYTES} bytes for one operation"
                    ),
                }
                expected_message = expected_messages.get(args.expect_rejection)
                if expected_message is None or expected_message not in message:
                    raise
                expected_rejection = message
            finally:
                for extra_session in client_sessions[1:]:
                    extra_session.stop()

    server_rss_peak_bytes = None
    client_peak_rss_bytes = None
    if resource is not None:
        rss_raw = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
        server_rss_peak_bytes = int(
            rss_raw if sys.platform == "darwin" else rss_raw * 1024
        )
        client_raw = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        client_peak_rss_bytes = int(
            client_raw if sys.platform == "darwin" else client_raw * 1024
        )

    server_version = subprocess.run(
        [str(server_bin), "--version"], check=True, capture_output=True, text=True
    ).stdout.strip()
    with server_bin.open("rb") as binary:
        server_binary_sha256 = hashlib.file_digest(binary, "sha256").hexdigest()
    harness_path = Path(__file__).resolve()
    repository_root = harness_path.parents[2]
    harness_revision = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repository_root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    with harness_path.open("rb") as harness:
        harness_sha256 = hashlib.file_digest(harness, "sha256").hexdigest()
    result_limit_message = (
        "Connect replay and sender buffers exceed "
        f"{CONNECT_RESULT_LIMIT_BYTES} bytes for one operation"
    )
    result_limit_rejections = sum(
        result_limit_message in (outcome["error"] or "") for outcome in action_outcomes
    )
    report: dict[str, Any] = {
        "schema_version": 2,
        "timestamp_utc": datetime.now(UTC).isoformat(),
        "harness_source_revision": harness_revision,
        "harness_script_sha256": harness_sha256,
        "server_binary": str(server_bin),
        "server_version": server_version,
        "server_binary_sha256": server_binary_sha256,
        "server_build_profile": "release",
        "server_build_features": "http,connect,duckdb-source",
        "workload": {
            "source_mode": "standalone read-only DuckDB file",
            "rows": args.rows,
            "concurrent_readers": args.concurrent_readers,
            "payload_bytes_per_row": args.payload_bytes,
            "logical_uncompressed_column_bytes": args.rows * (8 + args.payload_bytes),
            "aggregate_logical_uncompressed_column_bytes": (
                args.rows * (8 + args.payload_bytes) * args.concurrent_readers
            ),
            "source_database_bytes": database_size_bytes,
            "iterations": args.iterations,
            "warmups": 1,
            "attempted_rounds": len(action_outcomes) // args.concurrent_readers,
            "timed_rounds_completed": len(batch_samples_ms),
            "action": "slow_to_local_iterator"
            if args.slow_reader_delay_ms
            else "count",
            "slow_reader_delay_ms": args.slow_reader_delay_ms,
            "action_samples_ms": samples_ms,
            "concurrent_batch_samples_ms": batch_samples_ms,
            "action_outcomes": action_outcomes,
            "result_limit_rejections": result_limit_rejections,
            "action_p50_ms": percentile(samples_ms, 0.50) if samples_ms else None,
            "action_p95_ms": percentile(samples_ms, 0.95) if samples_ms else None,
            "result": (
                f"expected_{args.expect_rejection}_limit_rejection"
                if expected_rejection
                else "completed_with_result_limit_rejections"
                if args.allow_result_rejection and result_limit_rejections
                else "completed"
            ),
            "expected_error": expected_rejection,
        },
        "source_limits": SOURCE_LIMITS,
        "connect_result_limit_bytes": CONNECT_RESULT_LIMIT_BYTES,
        "server_configuration": {
            "max_active_operations": args.concurrent_readers,
            "max_pending_operations": 16,
            "queue_timeout_ms": 5000,
            "response_timeout_ms": 60000,
            "polars_max_threads": 4,
            "rayon_threads": 4,
            "omp_threads": 4,
        },
        "server_rss": {
            "peak_bytes": server_rss_peak_bytes,
            "method": "resource.getrusage(RUSAGE_CHILDREN) after the server child exits",
        },
        "environment": {
            "platform": platform.platform(),
            "machine": platform.machine(),
            "python": platform.python_version(),
            "pyspark_client": client_version,
            "pyarrow": pyarrow_version,
            "duckdb_fixture_writer": duckdb.__version__,
            "client_peak_rss_bytes": client_peak_rss_bytes,
        },
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {"server_rss": report["server_rss"], "workload": report["workload"]},
            indent=2,
        )
    )
    print(f"report={args.output}")


if __name__ == "__main__":
    main()
