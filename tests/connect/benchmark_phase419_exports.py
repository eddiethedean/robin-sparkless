"""Measure public Spark Connect export and bounded local-relation latency."""

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
import time
from typing import Any, Callable

try:
    import resource
except ImportError:  # Windows exposes no getrusage module.
    resource = None

from pyspark.sql.connect.session import SparkSession as ConnectSession

from sparkless.server import SparklessConnect


ROOT = Path(__file__).resolve().parents[2]
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
    return ordered[max(0, math.ceil(fraction * len(ordered)) - 1)]


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


def measure_case(
    name: str,
    action: Callable[[], list[Any]],
    validate: Callable[[list[Any]], None],
    iterations: int,
) -> dict[str, Any]:
    warmup_result = action()
    validate(warmup_result)
    samples: list[float] = []
    for iteration in range(iterations):
        started = time.perf_counter()
        result = action()
        elapsed_ms = (time.perf_counter() - started) * 1000
        validate(result)
        samples.append(elapsed_ms)
    return {
        "workload": name,
        "warmup_rows": len(warmup_result),
        "samples_ms": samples,
        "sample_count": len(samples),
        "p50_ms": percentile(samples, 0.50),
        "p95_ms": percentile(samples, 0.95),
        "p99_ms": percentile(samples, 0.99),
        "max_ms": max(samples),
    }


def exact_range_rows(rows: int) -> Callable[[list[Any]], None]:
    def validate(result: list[Any]) -> None:
        if len(result) != rows:
            raise AssertionError(f"range({rows}) returned {len(result)} rows")
        if rows and (result[0].id != 0 or result[-1].id != rows - 1):
            raise AssertionError(f"range({rows}) returned incorrect boundary values")
        if any(row.id != index for index, row in enumerate(result)):
            raise AssertionError(f"range({rows}) returned incorrect values")

    return validate


def exact_arrow_range(rows: int) -> Callable[[Any], None]:
    def validate(table: Any) -> None:
        if table.num_rows != rows or table.column_names != ["id"]:
            raise AssertionError(f"range({rows}) returned an incorrect Arrow shape")
        if table.column("id").to_pylist() != list(range(rows)):
            raise AssertionError(f"range({rows}) returned incorrect Arrow values")

    return validate


def validate_empty_rows(result: list[Any]) -> None:
    if result:
        raise AssertionError(f"empty filter returned {len(result)} rows")


def validate_empty_arrow(table: Any) -> None:
    if table.num_rows != 0 or table.column_names != ["id"]:
        raise AssertionError("empty filter returned an incorrect Arrow result")


def run_exports(spark: ConnectSession, iterations: int) -> list[dict[str, Any]]:
    measurements: list[dict[str, Any]] = []
    for rows in (1_000, 10_000, 100_000):
        frame = spark.range(rows)
        measurements.append(
            measure_case(
                f"range_collect_{rows}",
                frame.collect,
                exact_range_rows(rows),
                iterations,
            )
        )
        measurements.append(
            measure_case(
                f"range_to_arrow_{rows}",
                frame.toArrow,
                exact_arrow_range(rows),
                iterations,
            )
        )

    for rows in (1_000, 10_000):
        frame = spark.range(rows).filter(f"id > {rows}")
        measurements.append(
            measure_case(
                f"empty_filter_collect_{rows}",
                frame.collect,
                validate_empty_rows,
                iterations,
            )
        )
        measurements.append(
            measure_case(
                f"empty_filter_to_arrow_{rows}",
                frame.toArrow,
                validate_empty_arrow,
                iterations,
            )
        )

    local_rows = [(index, f"label-{index}") for index in range(10_000)]

    def collect_local_relation() -> list[Any]:
        return spark.createDataFrame(local_rows, "id BIGINT, label STRING").collect()

    def to_arrow_local_relation() -> Any:
        return spark.createDataFrame(local_rows, "id BIGINT, label STRING").toArrow()

    def validate_local_relation(result: list[Any]) -> None:
        if len(result) != 10_000:
            raise AssertionError("10,000-row local relation returned incorrect values")
        if any(
            row.id != index or row.label != f"label-{index}"
            for index, row in enumerate(result)
        ):
            raise AssertionError("10,000-row local relation returned incorrect values")

    def validate_local_relation_arrow(table: Any) -> None:
        if (
            table.num_rows != 10_000
            or table.column_names != ["id", "label"]
            or table.column("id").to_pylist() != list(range(10_000))
            or table.column("label").to_pylist()
            != [f"label-{index}" for index in range(10_000)]
        ):
            raise AssertionError(
                "10,000-row Arrow local relation returned incorrect values"
            )

    measurements.append(
        measure_case(
            "local_relation_collect_10000",
            collect_local_relation,
            validate_local_relation,
            iterations,
        )
    )
    measurements.append(
        measure_case(
            "local_relation_to_arrow_10000",
            to_arrow_local_relation,
            validate_local_relation_arrow,
            iterations,
        )
    )

    wide_value = "x" * 300_000
    wide_frame = spark.createDataFrame(
        [(1, ""), (2, "x"), (3, "界" * 4_096), (4, wide_value)],
        "id INT, payload STRING",
    )

    def validate_variable_width(result: list[Any]) -> None:
        if (
            len(result) != 4
            or result[0].payload != ""
            or result[1].payload != "x"
            or result[2].payload != "界" * 4_096
            or result[3].payload != wide_value
        ):
            raise AssertionError(
                "variable-width local relation returned incorrect values"
            )

    def validate_variable_width_arrow(table: Any) -> None:
        if (
            table.num_rows != 4
            or table.column_names != ["id", "payload"]
            or table.column("id").to_pylist() != [1, 2, 3, 4]
            or table.column("payload").to_pylist()
            != ["", "x", "界" * 4_096, wide_value]
        ):
            raise AssertionError(
                "variable-width Arrow relation returned incorrect values"
            )

    measurements.append(
        measure_case(
            "variable_width_collect_300k_cell",
            wide_frame.collect,
            validate_variable_width,
            iterations,
        )
    )
    measurements.append(
        measure_case(
            "variable_width_to_arrow_300k_cell",
            wide_frame.toArrow,
            validate_variable_width_arrow,
            iterations,
        )
    )
    return measurements


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--server-bin", type=Path, required=True)
    parser.add_argument("--build-features", default="connect")
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
        measurements = run_exports(spark, args.iterations)

    client_peak_rss_bytes = None
    if resource is not None:
        rss_raw = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        client_peak_rss_bytes = int(
            rss_raw if sys.platform == "darwin" else rss_raw * 1024
        )
    report = {
        "schema_version": 1,
        "timestamp_utc": datetime.now(UTC).isoformat(),
        "source_revision": source_revision,
        "source_dirty_paths": source_dirty_paths,
        "server_binary": str(server_bin),
        "server_build_features": args.build_features,
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
            "client_peak_rss_bytes": client_peak_rss_bytes,
        },
        "measurements": measurements,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    for result in measurements:
        print(
            f"workload={result['workload']} p50_ms={result['p50_ms']:.3f} "
            f"p95_ms={result['p95_ms']:.3f} p99_ms={result['p99_ms']:.3f} "
            f"max_ms={result['max_ms']:.3f} samples={result['sample_count']}"
        )
    print(f"client_peak_rss_bytes={client_peak_rss_bytes}")
    print(f"report={args.output}")


if __name__ == "__main__":
    main()
