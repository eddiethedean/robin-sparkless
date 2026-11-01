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
import re
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
    "POLARS_MAX_THREADS": "4",
    "RAYON_NUM_THREADS": "4",
    "OMP_NUM_THREADS": "4",
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


def arrow_trace_records(diagnostics: str) -> list[dict[str, str]]:
    records = []
    for line in diagnostics.splitlines():
        if "sparkless_arrow_trace event=complete" in line:
            records.append(dict(re.findall(r"([a-z_]+)=([^\s]+)", line)))
    return records


def measure_additional_startups(server_bin: Path, sample_count: int) -> list[float]:
    samples = []
    for _ in range(sample_count):
        helper = SparklessConnect(server_options={"executable_path": str(server_bin)})
        started = time.perf_counter()
        with helper as spark:
            elapsed_ms = (time.perf_counter() - started) * 1_000
            if not isinstance(spark, ConnectSession):
                raise TypeError(
                    f"expected PySpark Connect session, got {type(spark)!r}"
                )
        samples.append(elapsed_ms)
    return samples


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
        projected = frame.select("id")
        measurements.append(
            measure_case(
                f"range_select_id_collect_{rows}",
                projected.collect,
                exact_range_rows(rows),
                iterations,
            )
        )
        measurements.append(
            measure_case(
                f"range_select_id_to_arrow_{rows}",
                projected.toArrow,
                exact_arrow_range(rows),
                iterations,
            )
        )

    limited_range = spark.range(100_000).limit(1_000)
    measurements.append(
        measure_case(
            "range_100000_limit_1000_collect",
            limited_range.collect,
            exact_range_rows(1_000),
            iterations,
        )
    )
    measurements.append(
        measure_case(
            "range_100000_limit_1000_to_arrow",
            limited_range.toArrow,
            exact_arrow_range(1_000),
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


def run_small_jobs(spark: ConnectSession, iterations: int) -> list[dict[str, Any]]:
    """Measure the small public-client cases used by the 4.19 regression gate."""
    measurements: list[dict[str, Any]] = []
    for row_count in (10, 100, 1_000, 10_000):
        rows = [(index, f"item-{index}") for index in range(row_count)]
        threshold = row_count // 2
        expected = max(row_count - threshold - 1, 0)

        def ingest_filter_count(rows: list[tuple[int, str]] = rows) -> list[int]:
            return [
                spark.createDataFrame(rows, "id BIGINT, label STRING")
                .filter(f"id > {threshold}")
                .count()
            ]

        def validate_count(
            result: list[int], expected: int = expected, row_count: int = row_count
        ) -> None:
            if result != [expected]:
                raise AssertionError(
                    f"{row_count}-row tuple ingest/filter/count returned {result}, "
                    f"expected {[expected]}"
                )

        measurements.append(
            measure_case(
                f"tuple_ingest_filter_count_{row_count}",
                ingest_filter_count,
                validate_count,
                iterations,
            )
        )
        measurements[-1]["input_rows"] = row_count
        measurements[-1]["expected_count"] = expected

    preview_rows = [
        (index, None if index % 7 == 0 else f"item-{index}") for index in range(1_000)
    ]

    def preview_25() -> list[Any]:
        return (
            spark.createDataFrame(preview_rows, "id BIGINT, label STRING")
            .limit(25)
            .collect()
        )

    def validate_preview(result: list[Any]) -> None:
        if len(result) != 25 or [row.id for row in result] != list(range(25)):
            raise AssertionError("25-row preview returned the wrong row window")
        if any(
            row.label != (None if row.id % 7 == 0 else f"item-{row.id}")
            for row in result
        ):
            raise AssertionError("25-row preview returned incorrect nullable values")

    measurements.append(
        measure_case("preview_25_nullable", preview_25, validate_preview, iterations)
    )
    measurements[-1]["input_rows"] = len(preview_rows)

    mixed_rows = [
        (
            index,
            None if index % 3 == 0 else index * 10,
            None if index % 5 == 0 else f"value-{index}",
            index % 2 == 0,
        )
        for index in range(256)
    ]

    def collect_nullable_mixed() -> list[Any]:
        return spark.createDataFrame(
            mixed_rows,
            "id INT, quantity BIGINT, label STRING, enabled BOOLEAN",
        ).collect()

    def validate_nullable_mixed(result: list[Any]) -> None:
        if len(result) != len(mixed_rows):
            raise AssertionError(
                "nullable/mixed collection returned the wrong row count"
            )
        for index, row in enumerate(result):
            expected = mixed_rows[index]
            if (row.id, row.quantity, row.label, row.enabled) != expected:
                raise AssertionError(
                    f"nullable/mixed row {index} returned "
                    f"{(row.id, row.quantity, row.label, row.enabled)!r}, "
                    f"expected {expected!r}"
                )

    measurements.append(
        measure_case(
            "nullable_mixed_collect_256",
            collect_nullable_mixed,
            validate_nullable_mixed,
            iterations,
        )
    )
    measurements[-1]["input_rows"] = len(mixed_rows)

    def analyze_range_schema() -> list[Any]:
        schema = spark.range(1_000).schema
        return [(schema.simpleString(), tuple(schema.fieldNames()))]

    def validate_range_schema(result: list[Any]) -> None:
        if result != [("struct<id:bigint>", ("id",))]:
            raise AssertionError(f"range schema analysis returned {result!r}")

    measurements.append(
        measure_case(
            "schema_analysis_range_1000",
            analyze_range_schema,
            validate_range_schema,
            iterations,
        )
    )
    measurements[-1]["input_rows"] = 1_000
    return measurements


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--server-bin", type=Path, required=True)
    parser.add_argument("--build-features", default="connect")
    parser.add_argument(
        "--build-profile", choices=("release", "debug", "unknown"), default="unknown"
    )
    parser.add_argument("--server-source-revision")
    parser.add_argument("--iterations", type=int, default=20)
    parser.add_argument(
        "--startup-samples",
        type=int,
        default=1,
        help="number of fresh server/readiness/session starts to measure",
    )
    parser.add_argument(
        "--arrow-trace",
        action="store_true",
        help="include per-stream Arrow timing and buffer high-water records",
    )
    parser.add_argument(
        "--small-jobs-only",
        action="store_true",
        help="run only tuple-ingest, preview, nullable, and schema guardrails",
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.iterations < 1 or args.startup_samples < 1:
        parser.error("--iterations and --startup-samples must be positive")
    server_bin = args.server_bin.absolute()
    if not server_bin.is_file():
        parser.error(f"server binary not found: {server_bin}")

    prepare_server_environment()
    server_environment = dict(SERVER_ENVIRONMENT)
    if args.arrow_trace:
        server_environment["SPARKLESS_ARROW_TRACE"] = "1"
    os.environ.update(server_environment)
    source_revision, source_dirty_paths = git_source_state()
    version = subprocess.run(
        [str(server_bin), "--version"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    with server_bin.open("rb") as binary:
        binary_sha256 = hashlib.file_digest(binary, "sha256").hexdigest()

    startup_started = time.perf_counter()
    helper = SparklessConnect(server_options={"executable_path": str(server_bin)})
    with helper as spark:
        startup_ms = (time.perf_counter() - startup_started) * 1_000
        owned_server = helper._server
        if owned_server is None:
            raise RuntimeError("SparklessConnect did not retain its owned server")
        if not isinstance(spark, ConnectSession):
            raise TypeError(f"expected PySpark Connect session, got {type(spark)!r}")
        if args.small_jobs_only:
            measurements = run_small_jobs(spark, args.iterations)
        else:
            measurements = run_exports(spark, args.iterations)
    startup_samples_ms = [startup_ms]
    startup_samples_ms.extend(
        measure_additional_startups(server_bin, args.startup_samples - 1)
    )

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
        "server_build_profile": args.build_profile,
        "server_source_revision": args.server_source_revision,
        "server_version": version,
        "server_binary_sha256": binary_sha256,
        "server_environment": server_environment,
        "startup_readiness_and_session_ms": startup_ms,
        "startup_samples_ms": startup_samples_ms,
        "startup_p50_ms": percentile(startup_samples_ms, 0.50),
        "startup_p95_ms": percentile(startup_samples_ms, 0.95),
        "iterations_per_case": args.iterations,
        "warmups_per_case": 1,
        "environment": {
            "platform": platform.platform(),
            "machine": platform.machine(),
            "processor": platform.processor(),
            "python": platform.python_version(),
            "pyspark_client": importlib.metadata.version("pyspark-client"),
            "pyarrow": importlib.metadata.version("pyarrow"),
            "client_peak_rss_bytes": client_peak_rss_bytes,
        },
        "measurements": measurements,
        "arrow_trace_records": arrow_trace_records(owned_server.diagnostics)
        if args.arrow_trace
        else [],
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
    print(f"startup_readiness_and_session_ms={startup_ms:.3f}")
    print(
        f"startup_samples={len(startup_samples_ms)} "
        f"startup_p50_ms={percentile(startup_samples_ms, 0.50):.3f} "
        f"startup_p95_ms={percentile(startup_samples_ms, 0.95):.3f}"
    )
    print(f"arrow_trace_records={len(report['arrow_trace_records'])}")
    print(f"report={args.output}")


if __name__ == "__main__":
    main()
