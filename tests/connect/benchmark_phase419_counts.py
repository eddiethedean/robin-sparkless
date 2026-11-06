"""Measure exact public Connect range-count latency at 1k, 1M, and 10M rows."""

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
import time
from typing import Any

from pyspark.sql import functions as F
from pyspark.sql.connect.session import SparkSession as ConnectSession

from benchmark_lock import benchmark_lock
from sparkless_connect import SparklessConnect


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SIZES = (1_000, 1_000_000, 10_000_000)
SERVER_ENVIRONMENT = {
    "SPARKLESS_MAX_ACTIVE_OPERATIONS": "2",
    "SPARKLESS_MAX_PENDING_OPERATIONS": "16",
    "SPARKLESS_MAX_RANGE_ROWS": "10000000",
    "SPARKLESS_QUEUE_TIMEOUT_MS": "5000",
    "SPARKLESS_RESPONSE_TIMEOUT_MS": "60000",
    "SPARKLESS_CONNECT_TRACE": "0",
    "SPARKLESS_ARROW_TRACE": "0",
}


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    index = max(0, math.ceil(fraction * len(ordered)) - 1)
    return ordered[index]


def git_source_state() -> tuple[str, list[str]]:
    revision = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    status = subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.splitlines()
    return revision, [line[3:] for line in status]


def prepare_server_environment() -> None:
    for key in tuple(os.environ):
        if key.startswith("SPARKLESS_"):
            os.environ.pop(key, None)
    os.environ.update(SERVER_ENVIRONMENT)
    os.environ["TZ"] = "UTC"


def run_counts(spark: ConnectSession, iterations: int) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for rows in DEFAULT_SIZES:
        range_frame = spark.range(rows)
        cases = (
            (
                "string_gt_5",
                lambda: range_frame.filter("id > 5").count(),
                max(rows - 6, 0),
            ),
            (
                "column_gte_5",
                lambda: range_frame.filter(F.col("id") >= 5).count(),
                max(rows - 5, 0),
            ),
        )
        for name, count_query, expected in cases:
            warmup = count_query()
            if warmup != expected:
                raise AssertionError(
                    f"{name} warmup for {rows} rows returned {warmup}, expected {expected}"
                )
            samples = []
            for iteration in range(iterations):
                started = time.perf_counter()
                actual = count_query()
                elapsed_ms = (time.perf_counter() - started) * 1000
                if actual != expected:
                    raise AssertionError(
                        f"{name} iteration {iteration} for {rows} rows returned "
                        f"{actual}, expected {expected}"
                    )
                samples.append(elapsed_ms)
            records.append(
                {
                    "rows": rows,
                    "predicate": name,
                    "expected_count": expected,
                    "warmup_count": warmup,
                    "samples_ms": samples,
                    "sample_count": len(samples),
                    "p50_ms": percentile(samples, 0.50),
                    "p95_ms": percentile(samples, 0.95),
                    "p99_ms": percentile(samples, 0.99),
                    "max_ms": max(samples),
                }
            )
    return records


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--server-bin", type=Path, required=True)
    parser.add_argument("--iterations", type=int, default=20)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.iterations < 1:
        parser.error("--iterations must be positive")
    server_bin = args.server_bin.absolute()
    if not server_bin.is_file():
        parser.error(f"server binary not found: {server_bin}")

    prepare_server_environment()
    source_revision, source_dirty_paths = git_source_state()
    version = subprocess.run(
        [str(server_bin), "--version"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    with server_bin.open("rb") as binary:
        binary_sha256 = hashlib.file_digest(binary, "sha256").hexdigest()

    with SparklessConnect(server_options={"executable_path": str(server_bin)}) as spark:
        if not isinstance(spark, ConnectSession):
            raise TypeError(f"expected PySpark Connect session, got {type(spark)!r}")
        measurements = run_counts(spark, args.iterations)

    report = {
        "schema_version": 1,
        "timestamp_utc": datetime.now(UTC).isoformat(),
        "source_revision": source_revision,
        "source_dirty_paths": source_dirty_paths,
        "server_binary": str(server_bin),
        "server_version": version,
        "server_binary_sha256": binary_sha256,
        "server_environment": SERVER_ENVIRONMENT,
        "iterations_per_case": args.iterations,
        "warmups_per_case": 1,
        "environment": {
            "platform": platform.platform(),
            "machine": platform.machine(),
            "processor": platform.processor(),
            "python": platform.python_version(),
            "pyspark_client": importlib.metadata.version("pyspark-client"),
        },
        "measurements": measurements,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    for result in measurements:
        print(
            f"rows={result['rows']} predicate={result['predicate']} "
            f"p50_ms={result['p50_ms']:.3f} p95_ms={result['p95_ms']:.3f} "
            f"p99_ms={result['p99_ms']:.3f} max_ms={result['max_ms']:.3f} "
            f"samples={result['sample_count']}"
        )
    print(f"report={args.output}")


if __name__ == "__main__":
    with benchmark_lock():
        main()
