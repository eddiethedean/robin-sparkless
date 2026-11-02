#!/usr/bin/env python3
"""Compare a four-worker public Connect pytest suite on Sparkless and Spark 4.0.5.

Each engine uses one shared Connect server. Every xdist worker creates and closes its
own Spark Connect session, and all 38 test executions validate their results.
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import signal
import socket
import subprocess
import sys
import tempfile
import textwrap
import time
import xml.etree.ElementTree as ET
import zipfile
from typing import Any


CLIENT_ENVIRONMENT_KEYS = (
    "SPARK_HOME",
    "SPARK_LOCAL_IP",
    "PYSPARK_PYTHON",
    "PYSPARK_DRIVER_PYTHON",
    "PYSPARK_SUBMIT_ARGS",
    "SPARK_REMOTE",
    "SPARK_CONNECT_URL",
    "SPARKLESS_CONNECT_URL",
)

SERVER_ENVIRONMENT = {
    "SPARKLESS_MAX_ACTIVE_OPERATIONS": "4",
    "SPARKLESS_MAX_PENDING_OPERATIONS": "16",
    "SPARKLESS_MAX_RANGE_ROWS": "10000000",
    "SPARKLESS_QUEUE_TIMEOUT_MS": "30000",
    "SPARKLESS_RESPONSE_TIMEOUT_MS": "60000",
    "SPARKLESS_CONNECT_TRACE": "0",
    "SPARKLESS_ARROW_TRACE": "0",
    "POLARS_MAX_THREADS": "4",
    "RAYON_NUM_THREADS": "4",
    "OMP_NUM_THREADS": "4",
    "TZ": "UTC",
}

SUITE_SOURCE = textwrap.dedent(
    """
    import os

    import pytest
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col


    @pytest.fixture(scope="session")
    def spark():
        session = SparkSession.builder.remote(os.environ["PHASE419_CONNECT_URL"]).create()
        try:
            yield session
        finally:
            session.stop()


    @pytest.mark.parametrize("case", range(10))
    def test_ingest_filter_count_1k(spark, case):
        rows = [(row_id, row_id % 3) for row_id in range(1000)]
        frame = spark.createDataFrame(rows, "order_id long, status long")
        assert frame.filter(col("status") >= 1).count() == 666


    @pytest.mark.parametrize("case", range(10))
    def test_nullable_order_collection(spark, case):
        rows = [
            (row_id, None if row_id % 3 == 0 else float(row_id))
            for row_id in range(100)
        ]
        frame = spark.createDataFrame(rows, "order_id long, value double")
        result = frame.collect()
        assert len(result) == 100
        assert [row.order_id for row in result] == list(range(100))
        assert sum(row.value is None for row in result) == 34


    @pytest.mark.parametrize("case", range(10))
    def test_preview_25(spark, case):
        result = spark.range(1000).limit(25).collect()
        assert [row.id for row in result] == list(range(25))


    @pytest.mark.parametrize("case", range(8))
    def test_collect_10k(spark, case):
        result = spark.range(10000).select("id").collect()
        assert len(result) == 10000
        assert result[0].id == 0
        assert result[-1].id == 9999
    """
).lstrip()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def clean_client_environment() -> dict[str, str]:
    environment = os.environ.copy()
    for key in CLIENT_ENVIRONMENT_KEYS:
        environment.pop(key, None)
    environment["TZ"] = "UTC"
    environment["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"
    return environment


def verify_client_stack() -> dict[str, str]:
    expected = {
        "pyspark-client": "4.0.5",
        "pytest-xdist": "3.8.0",
        "pyarrow": "26.0.0",
    }
    versions: dict[str, str] = {}
    for name, wanted in expected.items():
        try:
            actual = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError as error:
            raise SystemExit(
                f"required distribution {name} is not installed"
            ) from error
        versions[name] = actual
        if actual != wanted:
            raise SystemExit(f"expected {name}=={wanted}, found {actual}")
    versions["pytest"] = importlib.metadata.version("pytest")
    return versions


def add_candidate_wheel_metadata(wheel: Path, destination: Path) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(wheel) as archive:
        metadata_paths = [
            name
            for name in archive.namelist()
            if name.endswith(".dist-info/METADATA") and ".dist-info/" in name
        ]
        if len(metadata_paths) != 1:
            raise SystemExit(
                "candidate server wheel must contain one dist-info/METADATA"
            )
        metadata_path = Path(metadata_paths[0])
        target = destination / metadata_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(archive.read(metadata_paths[0]))
    sys.path.insert(0, str(destination))
    try:
        actual = importlib.metadata.version("sparkless-server-bin")
    except importlib.metadata.PackageNotFoundError as error:
        raise SystemExit("could not read candidate server wheel metadata") from error
    if actual != "4.19.0":
        raise SystemExit(f"expected sparkless-server-bin==4.19.0, found {actual}")


def nearest_rank(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int((fraction * len(ordered) + 0.999999)) - 1))
    return ordered[index]


def run_pytest_suite(
    suite_dir: Path, run_name: str, connect_url: str
) -> dict[str, Any]:
    junit_path = suite_dir / f"{run_name}.xml"
    command = [
        sys.executable,
        "-m",
        "pytest",
        "-p",
        "xdist.plugin",
        "-q",
        "-n",
        "4",
        "--dist=load",
        "--junitxml",
        str(junit_path),
        "test_phase419_xdist_suite.py",
    ]
    environment = clean_client_environment()
    environment["PHASE419_CONNECT_URL"] = connect_url
    started = time.perf_counter()
    process = subprocess.run(
        command,
        cwd=suite_dir,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    elapsed_ms = (time.perf_counter() - started) * 1000
    stdout = process.stdout
    stderr = process.stderr
    if process.returncode != 0:
        raise RuntimeError(
            f"{run_name} pytest suite failed (exit {process.returncode})\n"
            f"stdout:\n{stdout[-6000:]}\nstderr:\n{stderr[-6000:]}"
        )
    if not junit_path.is_file():
        raise RuntimeError(f"{run_name} did not write its JUnit report")
    root = ET.parse(junit_path).getroot()
    cases = []
    for case in root.findall(".//testcase"):
        cases.append(
            {
                "name": case.attrib.get("name"),
                "classname": case.attrib.get("classname"),
                "duration_seconds": float(case.attrib.get("time", "0")),
                "status": "failed" if case.find("failure") is not None else "passed",
            }
        )
    failures = sum(case["status"] == "failed" for case in cases)
    if len(cases) != 38 or failures:
        raise RuntimeError(
            f"{run_name}: expected 38 passing tests, found {len(cases)} tests and {failures} failures"
        )
    return {
        "command": command,
        "elapsed_ms": elapsed_ms,
        "test_count": len(cases),
        "failure_count": failures,
        "testcase_samples": cases,
        "stdout_sha256": hashlib.sha256(stdout.encode()).hexdigest(),
        "stderr_sha256": hashlib.sha256(stderr.encode()).hexdigest(),
        "stdout_tail": stdout[-1000:],
    }


def public_readiness_query(connect_url: str) -> None:
    from pyspark.sql.connect.session import SparkSession

    session = SparkSession.builder.remote(connect_url).create()
    try:
        if session.range(1).count() != 1:
            raise RuntimeError("Connect readiness action returned the wrong result")
    finally:
        session.stop()


def run_sparkless(
    binary: Path,
    suite_dir: Path,
    run_name: str,
    pair: int,
    order: int,
) -> dict[str, Any]:
    from sparkless.server import SparklessServer

    for key in tuple(os.environ):
        if key.startswith("SPARKLESS_") or key in {
            "POLARS_MAX_THREADS",
            "RAYON_NUM_THREADS",
            "OMP_NUM_THREADS",
        }:
            os.environ.pop(key, None)
    os.environ.update(SERVER_ENVIRONMENT)
    server = SparklessServer(
        executable_path=str(binary),
        startup_timeout=60,
        shutdown_timeout=30,
        env=SERVER_ENVIRONMENT,
    )
    run_start = time.perf_counter()
    with server:
        connect_url = server.spark_connect_url
        readiness_start = time.perf_counter()
        public_readiness_query(connect_url)
        readiness_ms = (time.perf_counter() - readiness_start) * 1000
        first_suite = run_pytest_suite(suite_dir, f"{run_name}-first", connect_url)
        startup_first_suite_ms = (time.perf_counter() - run_start) * 1000
        timed_suite = run_pytest_suite(suite_dir, f"{run_name}-timed", connect_url)
    return {
        "engine": "sparkless-server",
        "pair": pair,
        "order": order,
        "server_version": "4.19.0",
        "connect_url": connect_url,
        "readiness_query_ms": readiness_ms,
        "startup_readiness_first_suite_ms": startup_first_suite_ms,
        "first_suite": first_suite,
        "timed_suite": timed_suite,
    }


def wait_for_jvm_server(
    process: subprocess.Popen[bytes], port: int, log_path: Path
) -> None:
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        if process.poll() is not None:
            tail = log_path.read_text(errors="replace")[-6000:]
            raise RuntimeError(
                f"Spark Connect server exited {process.returncode}:\n{tail}"
            )
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.1)
    tail = log_path.read_text(errors="replace")[-6000:]
    process.terminate()
    raise TimeoutError(f"Spark Connect server did not open port {port}:\n{tail}")


def run_jvm_spark(
    spark_home: Path,
    suite_dir: Path,
    run_name: str,
    pair: int,
    order: int,
    work_dir: Path,
) -> dict[str, Any]:
    port = free_port()
    connect_url = f"sc://127.0.0.1:{port}"
    log_path = work_dir / f"{run_name}-spark-server.log"
    command = [
        str(spark_home / "bin" / "spark-submit"),
        "--class",
        "org.apache.spark.sql.connect.service.SparkConnectServer",
        "--master",
        "local[4]",
        "--conf",
        "spark.scheduler.mode=FAIR",
        "--conf",
        "spark.sql.shuffle.partitions=4",
        "--conf",
        "spark.default.parallelism=4",
        "--conf",
        "spark.connect.grpc.binding.address=127.0.0.1",
        "--conf",
        f"spark.connect.grpc.binding.port={port}",
        "--driver-java-options",
        "-XX:ActiveProcessorCount=4",
        str(spark_home / "jars" / "spark-connect_2.13-4.0.5.jar"),
    ]
    environment = clean_client_environment()
    environment.update(
        {
            "SPARK_HOME": str(spark_home),
            "SPARK_LOCAL_IP": "127.0.0.1",
            "SPARK_LOCAL_HOSTNAME": "localhost",
        }
    )
    run_start = time.perf_counter()
    with log_path.open("w", encoding="utf-8") as log:
        process = subprocess.Popen(
            command,
            cwd=work_dir,
            env=environment,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        try:
            wait_for_jvm_server(process, port, log_path)
            readiness_start = time.perf_counter()
            public_readiness_query(connect_url)
            readiness_ms = (time.perf_counter() - readiness_start) * 1000
            first_suite = run_pytest_suite(suite_dir, f"{run_name}-first", connect_url)
            startup_first_suite_ms = (time.perf_counter() - run_start) * 1000
            timed_suite = run_pytest_suite(suite_dir, f"{run_name}-timed", connect_url)
        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGINT)
                try:
                    process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    process.terminate()
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
    return {
        "engine": "jvm-spark-connect",
        "pair": pair,
        "order": order,
        "server_version": "4.0.5",
        "server_command": command,
        "connect_url": connect_url,
        "readiness_query_ms": readiness_ms,
        "startup_readiness_first_suite_ms": startup_first_suite_ms,
        "first_suite": first_suite,
        "timed_suite": timed_suite,
        "server_log_sha256": sha256_file(log_path),
        "server_exit_code": process.returncode,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server-binary", type=Path, required=True)
    parser.add_argument("--server-wheel", type=Path, required=True)
    parser.add_argument("--spark-archive", type=Path, required=True)
    parser.add_argument("--spark-home", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--pairs", type=int, default=3)
    args = parser.parse_args()
    if args.pairs != 3:
        parser.error("the retained comparison uses exactly three rotating pairs")
    binary = args.server_binary.resolve()
    wheel = args.server_wheel.resolve()
    spark_archive = args.spark_archive.resolve()
    spark_home = args.spark_home.resolve()
    for path in (
        binary,
        wheel,
        spark_archive,
        spark_home / "bin" / "spark-submit",
        spark_home / "jars" / "spark-connect_2.13-4.0.5.jar",
    ):
        if not path.exists():
            parser.error(f"required input does not exist: {path}")
    versions = verify_client_stack()
    with tempfile.TemporaryDirectory(prefix="phase419-xdist-") as temporary:
        work_dir = Path(temporary)
        suite_dir = work_dir / "pytest-suite"
        suite_dir.mkdir()
        (suite_dir / "test_phase419_xdist_suite.py").write_text(
            SUITE_SOURCE, encoding="utf-8"
        )
        add_candidate_wheel_metadata(wheel, work_dir / "candidate-wheel-metadata")
        reports = []
        order = (
            "sparkless-server",
            "jvm-spark-connect",
            "jvm-spark-connect",
            "sparkless-server",
            "sparkless-server",
            "jvm-spark-connect",
        )
        for index, engine in enumerate(order):
            pair = index // 2 + 1
            run_name = f"pair-{pair}-{engine}-{index % 2 + 1}"
            if engine == "sparkless-server":
                result = run_sparkless(binary, suite_dir, run_name, pair, index % 2 + 1)
            else:
                result = run_jvm_spark(
                    spark_home,
                    suite_dir,
                    run_name,
                    pair,
                    index % 2 + 1,
                    work_dir,
                )
            reports.append(result)
            print(
                f"{run_name}: startup+readiness+first suite="
                f"{result['startup_readiness_first_suite_ms']:.1f} ms; "
                f"warm suite={result['timed_suite']['elapsed_ms']:.1f} ms"
            )

    by_engine: dict[str, list[dict[str, Any]]] = {}
    for report in reports:
        by_engine.setdefault(report["engine"], []).append(report)
    summary = {}
    for engine, items in by_engine.items():
        warm = [item["timed_suite"]["elapsed_ms"] for item in items]
        cold = [item["startup_readiness_first_suite_ms"] for item in items]
        summary[engine] = {
            "warm_suite_median_ms": nearest_rank(warm, 0.5),
            "warm_suite_p95_ms": nearest_rank(warm, 0.95),
            "startup_readiness_first_suite_median_ms": nearest_rank(cold, 0.5),
            "warm_suite_samples_ms": warm,
            "startup_readiness_first_suite_samples_ms": cold,
            "test_executions": sum(
                item["first_suite"]["test_count"] + item["timed_suite"]["test_count"]
                for item in items
            ),
        }
    output = {
        "schema_version": 1,
        "record_type": "phase-419-four-worker-pytest-connect-comparison",
        "timestamp_utc": datetime.now(UTC).isoformat(),
        "environment": {
            "platform": platform.platform(),
            "machine": platform.machine(),
            "python": sys.version,
            "cpu_count": os.cpu_count(),
            "client_distributions": versions,
        },
        "workload": {
            "test_count_per_suite": 38,
            "suite_mix": {
                "ingest_filter_count_1k": 10,
                "nullable_order_collection": 10,
                "preview_25": 10,
                "collect_10k": 8,
            },
            "xdist_workers": 4,
            "sessions": "one session per worker, all workers share one Connect server",
            "rotation": list(order),
            "server_configuration": {
                "sparkless": SERVER_ENVIRONMENT,
                "jvm_spark": {
                    "version": "4.0.5",
                    "master": "local[4]",
                    "scheduler": "FAIR",
                    "active_processor_count": 4,
                    "shuffle_partitions": 4,
                },
            },
        },
        "artifacts": {
            "sparkless_server_binary_sha256": sha256_file(binary),
            "sparkless_server_wheel_sha256": sha256_file(wheel),
            "spark_server_distribution_sha256": sha256_file(spark_archive),
            "spark_connect_jar_sha256": sha256_file(
                spark_home / "jars" / "spark-connect_2.13-4.0.5.jar"
            ),
            "suite_source_sha256": hashlib.sha256(SUITE_SOURCE.encode()).hexdigest(),
            "benchmark_script_sha256": sha256_file(Path(__file__).resolve()),
        },
        "source_revision": subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=Path(__file__).resolve().parents[2],
            text=True,
        ).strip(),
        "summary": summary,
        "runs": reports,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(output, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    print(f"report: {args.output}")


if __name__ == "__main__":
    main()
