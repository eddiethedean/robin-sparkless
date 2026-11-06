"""Reproducible standalone Spark Connect concurrency and resource witness."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import re
import selectors
import socket
import statistics
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from benchmark_lock import benchmark_lock


ROOT = Path(__file__).resolve().parents[2]
CLIENT_SCRIPT = ROOT / "tests/connect/slice8_benchmark_client.py"
SLOW_READER_SCRIPT = ROOT / "tests/connect/slice8_slow_reader.py"
RAW_SLOW_READER_SCRIPT = ROOT / "tests/connect/slice8_raw_slow_reader.py"
RETRY_CLIENT_SCRIPT = ROOT / "tests/connect/slice6_retry_client.py"
PROXY_CLIENT_SCRIPT = ROOT / "tests/connect/slice8_proxy_client.py"
FAULT_PROXY_SCRIPT = ROOT / "tests/connect/fault_proxy.py"
DEFAULT_BINARY = ROOT / "target/release/sparkless-server"
CLIENT_ENVIRONMENT_KEYS = (
    "SPARK_HOME",
    "SPARK_CONF_DIR",
    "PYSPARK_PYTHON",
    "PYSPARK_DRIVER_PYTHON",
    "PYSPARK_SUBMIT_ARGS",
    "PYTHONPATH",
    "SPARK_REMOTE",
)
SERVER_CONFIG_ENVIRONMENT = {
    "SPARKLESS_MAX_REQUEST_BYTES": "1048576",
    "SPARKLESS_MAX_SQL_BYTES": "262144",
    "SPARKLESS_MAX_RESULT_ROWS": "10000",
    "SPARKLESS_MAX_RESULT_BYTES": "8388608",
    "SPARKLESS_MAX_CELL_BYTES": "1048576",
    "SPARKLESS_MAX_ACTIVE_OPERATIONS": "2",
    "SPARKLESS_MAX_PENDING_OPERATIONS": "16",
    "SPARKLESS_MAX_SESSIONS": "64",
    "SPARKLESS_SESSION_IDLE_TTL_MS": "900000",
    "SPARKLESS_MAX_MUTATION_RECORDS": "1024",
    "SPARKLESS_MAX_MUTATION_BYTES": "4194304",
    "SPARKLESS_BODY_READ_TIMEOUT_MS": "10000",
    "SPARKLESS_QUEUE_TIMEOUT_MS": "5000",
}
THREAD_ENVIRONMENT_KEYS = (
    "RAYON_NUM_THREADS",
    "POLARS_MAX_THREADS",
    "OMP_NUM_THREADS",
)


def clean_client_environment() -> dict[str, str]:
    environment = os.environ.copy()
    for key in CLIENT_ENVIRONMENT_KEYS:
        environment.pop(key, None)
    environment["TZ"] = "UTC"
    return environment


def server_configuration(
    rows: int,
    active_operations: int,
    queue_timeout_ms: int,
    operation_history_records: int = 1024,
    operation_history_bytes: int = 4 * 1024 * 1024,
    response_timeout_ms: int = 60_000,
) -> dict[str, str]:
    return {
        **SERVER_CONFIG_ENVIRONMENT,
        "SPARKLESS_MAX_RANGE_ROWS": str(max(rows, 10_000_000, 200_000)),
        "SPARKLESS_MAX_ACTIVE_OPERATIONS": str(active_operations),
        "SPARKLESS_MAX_MUTATION_RECORDS": str(operation_history_records),
        "SPARKLESS_MAX_MUTATION_BYTES": str(operation_history_bytes),
        "SPARKLESS_QUEUE_TIMEOUT_MS": str(queue_timeout_ms),
        "SPARKLESS_RESPONSE_TIMEOUT_MS": str(response_timeout_ms),
        "SPARKLESS_CONNECT_TRACE": "1",
        "SPARKLESS_ARROW_TRACE": "1",
    }


def execution_backend_environment(
    duckdb_local_database: Path | None = None,
    duckdb_owner_config: dict[str, Any] | None = None,
) -> dict[str, str]:
    if duckdb_local_database is not None and duckdb_owner_config is not None:
        raise ValueError("choose one DuckDB execution owner mode")
    if duckdb_owner_config is not None:
        return {
            "SPARKLESS_EXECUTION_BACKEND": "duckdb",
            "SPARKLESS_DUCKDB_EXECUTION_OWNER": json.dumps(
                duckdb_owner_config, sort_keys=True
            ),
        }
    if duckdb_local_database is None:
        return {}
    return {
        "SPARKLESS_EXECUTION_BACKEND": "duckdb",
        "SPARKLESS_DUCKDB_EXECUTION_OWNER": json.dumps(
            {
                "mode": "local",
                "database_path": str(duckdb_local_database.resolve()),
            },
            sort_keys=True,
        ),
    }


def redacted_execution_backend_environment(
    environment: dict[str, str],
) -> dict[str, str]:
    result = dict(environment)
    serialized_owner = result.get("SPARKLESS_DUCKDB_EXECUTION_OWNER")
    if serialized_owner is not None:
        owner = json.loads(serialized_owner)
        if owner.get("mode") == "quack" and "token" in owner:
            owner["token"] = "[redacted]"
            result["SPARKLESS_DUCKDB_EXECUTION_OWNER"] = json.dumps(
                owner, sort_keys=True
            )
    return result


def free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(fraction * len(ordered)) - 1)]


def cpu_time_seconds(value: str) -> float:
    parts = value.strip().split(":")
    if len(parts) == 3:
        leading, minutes, seconds = parts
        if "-" in leading:
            days_text, hours_text = leading.split("-", maxsplit=1)
            days, hours = int(days_text), int(hours_text)
        else:
            days, hours = 0, int(leading)
    elif len(parts) == 2:
        days, hours = 0, 0
        minutes, seconds = parts
    elif len(parts) == 1:
        days, hours, minutes, seconds = 0, 0, 0, parts[0]
    else:
        raise ValueError(f"unexpected ps CPU time: {value!r}")
    return days * 86_400 + hours * 3_600 + int(minutes) * 60 + float(seconds)


def process_metrics(pid: int) -> tuple[float, int]:
    result = subprocess.run(
        ["ps", "-o", "cputime=,rss=", "-p", str(pid)],
        check=True,
        capture_output=True,
        text=True,
    )
    fields = result.stdout.split()
    if len(fields) != 2:
        raise RuntimeError(f"could not read server process metrics for pid {pid}")
    # `ps` reports RSS in KiB on the supported macOS and Linux release runners.
    return cpu_time_seconds(fields[0]), int(fields[1]) * 1024


class ProcessSampler:
    def __init__(
        self, pid: int, interval_seconds: float = 0.1, label: str = "server"
    ) -> None:
        self.pid = pid
        self.interval_seconds = interval_seconds
        self.label = label
        self.stop_event = threading.Event()
        self.samples: list[tuple[float, int]] = []
        self.errors: list[str] = []
        self.thread = threading.Thread(target=self._sample, daemon=True)

    def _sample(self) -> None:
        while not self.stop_event.is_set():
            try:
                self.samples.append(process_metrics(self.pid))
            except (
                OSError,
                subprocess.CalledProcessError,
                ValueError,
                RuntimeError,
            ) as error:
                self.errors.append(str(error))
                break
            self.stop_event.wait(self.interval_seconds)

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> dict[str, Any]:
        self.stop_event.set()
        self.thread.join(timeout=2)
        if self.thread.is_alive():
            self.errors.append("resource sampler did not stop")
        return {
            f"{self.label}_peak_rss_bytes": max(
                (rss for _, rss in self.samples), default=None
            ),
            f"{self.label}_cpu_seconds_start": self.samples[0][0]
            if self.samples
            else None,
            f"{self.label}_cpu_seconds_end": self.samples[-1][0]
            if self.samples
            else None,
            f"{self.label}_cpu_seconds": (
                self.samples[-1][0] - self.samples[0][0]
                if len(self.samples) > 1
                else None
            ),
            f"{self.label}_samples": len(self.samples),
            f"{self.label}_sampling_errors": self.errors,
        }


def read_control_record(
    selector: selectors.BaseSelector,
    process: subprocess.Popen[str],
    timeout: float,
) -> dict[str, Any]:
    assert process.stdout is not None
    if not selector.select(timeout):
        raise TimeoutError("native server did not emit a control record")
    line = process.stdout.readline()
    if not line:
        raise RuntimeError(
            f"native server exited before its control record: {process.poll()}"
        )
    record = json.loads(line)
    if not isinstance(record, dict):
        raise RuntimeError("native server control record was not a JSON object")
    return record


def start_quack_owner(
    owner_binary: Path,
    extension_path: Path,
    database_path: Path,
    port: int,
    log_path: Path,
) -> tuple[subprocess.Popen[str], dict[str, Any]]:
    log = log_path.open("w", encoding="utf-8")
    owner = subprocess.Popen(
        [
            str(owner_binary),
            str(extension_path),
            "--persistent",
            str(database_path),
            str(port),
        ],
        cwd=ROOT,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=log,
        text=True,
        bufsize=1,
    )
    selector = selectors.DefaultSelector()
    assert owner.stdout is not None
    selector.register(owner.stdout, selectors.EVENT_READ)
    try:
        record = read_control_record(selector, owner, 30)
        if not isinstance(record.get("uri"), str) or not isinstance(
            record.get("token"), str
        ):
            raise RuntimeError("Quack fixture owner returned an invalid startup record")
        return owner, record
    except BaseException:
        if owner.poll() is None:
            owner.kill()
            owner.wait(timeout=5)
        raise
    finally:
        selector.close()
        log.close()


def stop_quack_owner(owner: subprocess.Popen[str], timeout: float = 15) -> None:
    if owner.poll() is not None:
        if owner.returncode != 0:
            raise RuntimeError(f"Quack fixture owner exited with {owner.returncode}")
        return
    if owner.stdin is None:
        raise RuntimeError("Quack fixture owner has no command pipe")
    owner.stdin.write("quit\n")
    owner.stdin.flush()
    owner.wait(timeout=timeout)
    if owner.returncode != 0:
        raise RuntimeError(f"Quack fixture owner exited with {owner.returncode}")


def start_server(
    binary: Path,
    log_path: Path,
    rows: int,
    active_operations: int,
    queue_timeout_ms: int,
    operation_history_records: int,
    operation_history_bytes: int,
    duckdb_local_database: Path | None = None,
    duckdb_owner_config: dict[str, Any] | None = None,
    response_timeout_ms: int = 60_000,
) -> tuple[subprocess.Popen[str], dict[str, Any], float]:
    environment = os.environ.copy()
    for key in (
        "SPARK_HOME",
        "SPARK_CONF_DIR",
        "PYSPARK_PYTHON",
        "PYSPARK_DRIVER_PYTHON",
        "PYSPARK_SUBMIT_ARGS",
        "PYTHONPATH",
        "SPARK_REMOTE",
        *THREAD_ENVIRONMENT_KEYS,
    ):
        environment.pop(key, None)
    for key in tuple(environment):
        if key.startswith("SPARKLESS_"):
            environment.pop(key, None)
    environment.update(
        {
            **server_configuration(
                rows,
                active_operations,
                queue_timeout_ms,
                operation_history_records,
                operation_history_bytes,
                response_timeout_ms,
            ),
            "TZ": "UTC",
        }
    )
    environment.update(
        execution_backend_environment(duckdb_local_database, duckdb_owner_config)
    )
    command = [
        str(binary),
        "control",
        "--host",
        "127.0.0.1",
        "--port",
        "0",
        "--connect-host",
        "127.0.0.1",
        "--connect-port",
        "0",
    ]
    started = time.perf_counter()
    with log_path.open("w", encoding="utf-8") as log:
        process = subprocess.Popen(
            command,
            cwd=ROOT,
            env=environment,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=log,
            text=True,
            bufsize=1,
        )
    selector = selectors.DefaultSelector()
    assert process.stdout is not None
    selector.register(process.stdout, selectors.EVENT_READ)
    try:
        record = read_control_record(selector, process, 60)
        if record.get("type") != "ready":
            raise RuntimeError(f"native server readiness failed: {record}")
        capabilities = record.get("capabilities")
        if not isinstance(capabilities, dict) or capabilities.get("transports") != [
            "http",
            "spark_connect",
        ]:
            raise RuntimeError(
                "benchmark server did not enable both HTTP and Spark Connect"
            )
        expected_backend = (
            "duckdb"
            if duckdb_local_database is not None or duckdb_owner_config is not None
            else "polars"
        )
        if record.get("execution_backend") != expected_backend:
            raise RuntimeError(
                "benchmark server selected an unexpected execution backend: "
                f"expected {expected_backend!r}, got {record.get('execution_backend')!r}"
            )
        record["_startup_seconds"] = time.perf_counter() - started
        record["_command"] = command
        return process, record, time.perf_counter() - started
    except BaseException:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        raise
    finally:
        selector.close()


def stop_server(process: subprocess.Popen[str], timeout: float = 15) -> dict[str, Any]:
    started = time.perf_counter()
    if process.poll() is not None:
        if process.returncode != 0:
            raise RuntimeError(f"native server exited with status {process.returncode}")
        return {
            "graceful": True,
            "seconds": time.perf_counter() - started,
            "exit_code": process.returncode,
            "acknowledged": False,
        }
    assert process.stdin is not None and process.stdout is not None
    process.stdin.write('{"control_protocol_version":2,"command":"shutdown"}\n')
    process.stdin.flush()
    selector = selectors.DefaultSelector()
    selector.register(process.stdout, selectors.EVENT_READ)
    try:
        stopped = read_control_record(selector, process, timeout)
    finally:
        selector.close()
    if stopped.get("type") != "stopped" or stopped.get("control_protocol_version") != 2:
        raise RuntimeError(f"native server did not acknowledge shutdown: {stopped}")
    process.wait(timeout=timeout)
    if process.returncode != 0:
        raise RuntimeError(f"native server exited with status {process.returncode}")
    return {
        "graceful": True,
        "seconds": time.perf_counter() - started,
        "exit_code": process.returncode,
        "acknowledged": True,
        "control_protocol_version": stopped["control_protocol_version"],
        "rust_global_allocator": stopped.get("rust_global_allocator"),
    }


def stop_fault_proxy(
    process: subprocess.Popen[str], timeout: float = 10
) -> dict[str, Any]:
    started = time.perf_counter()
    forced_kill = False
    if process.poll() is None:
        process.terminate()
    try:
        process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        forced_kill = True
        process.kill()
        process.wait(timeout=5)
    return {
        "reaped": True,
        "graceful": not forced_kill and process.returncode == 0,
        "forced_kill": forced_kill,
        "exit_code": process.returncode,
        "seconds": time.perf_counter() - started,
    }


def health_probe(http_url: str) -> tuple[float, bool]:
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(http_url + "/health", timeout=2) as response:
            payload = json.load(response)
        return (
            time.perf_counter() - started
        ) * 1000, response.status == 200 and payload.get("status") == "ready"
    except (OSError, urllib.error.URLError, TimeoutError, json.JSONDecodeError):
        return (time.perf_counter() - started) * 1000, False


def http_probe(http_url: str, timeout_seconds: float = 5) -> tuple[float, bool]:
    body = json.dumps(
        {
            # Reuse one read-only probe session so a 60-second cohort does not
            # fill the server's bounded session registry with synthetic clients.
            "session": "slice8-benchmark-http-progress",
            "code": "SELECT 1 AS result",
            "kind": "sql",
        }
    ).encode()
    request = urllib.request.Request(
        http_url + "/statements",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            payload = json.load(response)
        result = payload["data"]["application/json"]["data"]
        return (
            time.perf_counter() - started
        ) * 1000, response.status == 200 and result == [[1]]
    except (
        OSError,
        urllib.error.URLError,
        TimeoutError,
        json.JSONDecodeError,
        KeyError,
        TypeError,
    ):
        return (time.perf_counter() - started) * 1000, False


class ProgressMonitor:
    def __init__(
        self,
        http_url: str,
        http_timeout_seconds: float = 5,
        http_sql_enabled: bool = True,
    ) -> None:
        if not math.isfinite(http_timeout_seconds) or http_timeout_seconds <= 0:
            raise ValueError("HTTP probe timeout must be a finite positive number")
        self.http_url = http_url
        self.http_timeout_seconds = http_timeout_seconds
        self.http_sql_enabled = http_sql_enabled
        self.stop_event = threading.Event()
        self.health_ms: list[float] = []
        self.http_ms: list[float] = []
        self.errors: list[str] = []
        self.threads = [threading.Thread(target=self._health_loop, daemon=True)]
        if http_sql_enabled:
            self.threads.append(threading.Thread(target=self._http_loop, daemon=True))

    def _health_loop(self) -> None:
        while not self.stop_event.is_set():
            elapsed, ok = health_probe(self.http_url)
            self.health_ms.append(elapsed)
            if not ok:
                self.errors.append("HTTP /health failed")
            self.stop_event.wait(0.1)

    def _http_loop(self) -> None:
        while not self.stop_event.is_set():
            elapsed, ok = http_probe(self.http_url, self.http_timeout_seconds)
            self.http_ms.append(elapsed)
            if not ok:
                self.errors.append("HTTP SELECT 1 failed")
            self.stop_event.wait(0.25)

    def start(self) -> None:
        for thread in self.threads:
            thread.start()

    def stop(self) -> dict[str, Any]:
        self.stop_event.set()
        for thread in self.threads:
            thread.join(timeout=max(7, self.http_timeout_seconds + 1))
        if any(thread.is_alive() for thread in self.threads):
            raise TimeoutError("health/HTTP progress monitor did not stop")
        return {
            "health_requests": len(self.health_ms),
            "health_latency_p50_ms": percentile(self.health_ms, 0.50),
            "health_latency_p95_ms": percentile(self.health_ms, 0.95),
            "http_statements": len(self.http_ms),
            "http_statement_latency_p50_ms": percentile(self.http_ms, 0.50),
            "http_statement_latency_p95_ms": percentile(self.http_ms, 0.95),
            "probe_errors": self.errors,
        }


def wait_for_ready_files(
    paths: list[Path], processes: list[subprocess.Popen[str]], timeout: float = 120
) -> None:
    deadline = time.monotonic() + timeout
    while not all(path.exists() for path in paths):
        for process in processes:
            if process.poll() is not None:
                raise RuntimeError(
                    f"benchmark client exited during warmup with {process.returncode}"
                )
        if time.monotonic() >= deadline:
            raise TimeoutError("benchmark clients did not reach the start barrier")
        time.sleep(0.02)


def connect_capacity_rejection(
    stderr: str, source: str, index: int | None = None
) -> dict[str, Any] | None:
    if not any(
        marker in stderr.lower()
        for marker in (
            "timed out waiting for connect compute admission",
            "connect_plan_queue_timeout",
            "connect response stream stalled without progress for",
        )
    ):
        return None
    response_stalled = (
        "connect response stream stalled without progress for" in stderr.lower()
    )
    rejection: dict[str, Any] = {
        "source": source,
        "status": "DEADLINE_EXCEEDED"
        if "DEADLINE_EXCEEDED" in stderr
        else "SERVER_QUEUE_TIMEOUT",
        "diagnostic": "Connect response stream stalled without progress"
        if response_stalled
        else "timed out waiting for Connect compute admission",
    }
    if index is not None:
        rejection["client_index"] = index
    return rejection


def require_candidate_without_capacity_rejections(
    report: dict[str, Any], pair: int
) -> None:
    """Keep expected baseline saturation visible without accepting it in the candidate."""
    count = report.get("capacity_rejection_count")
    if not isinstance(count, int) or count < 0:
        raise RuntimeError(
            f"paired candidate {pair} has an invalid capacity-rejection count: {count!r}"
        )
    if count:
        rejections = [
            rejection
            for cohort in report.get("cohorts", [])
            for rejection in cohort.get("capacity_rejections", [])
        ]
        final_rejection = report.get("slow_reader", {}).get("capacity_rejection")
        if final_rejection is not None:
            rejections.append(final_rejection)
        raise RuntimeError(
            f"paired candidate {pair} had {count} capacity rejection(s): {rejections!r}"
        )


def phase_trace_summary(
    lines: str, *, slow_reader_operation_id: str | None = None
) -> dict[str, Any]:
    producer_phases: list[dict[str, str]] = []
    arrow_streams: list[dict[str, str]] = []
    for line in lines.splitlines():
        if "connect_trace event=producer_phases" in line:
            producer_phases.append(dict(re.findall(r"([a-z_]+)=([^\s]+)", line)))
        elif "sparkless_arrow_trace event=complete" in line:
            arrow_streams.append(dict(re.findall(r"([a-z_]+)=([^\s]+)", line)))

    summary: dict[str, Any] = {
        "producer_phase_samples": len(producer_phases),
        "arrow_streams": len(arrow_streams),
        "source_batches_total": sum(
            int(stream.get("source_batches", "0")) for stream in arrow_streams
        ),
        "source_rows_total": sum(
            int(stream.get("source_rows", "0")) for stream in arrow_streams
        ),
        "output_batches_total": sum(
            int(stream.get("output_batches", "0")) for stream in arrow_streams
        ),
        "serialization_ms_total": sum(
            float(stream.get("serialization_ms", "0")) for stream in arrow_streams
        ),
        "rechunk_ms_total": sum(
            float(stream.get("rechunk_ms", "0")) for stream in arrow_streams
        ),
        "ipc_write_ms_total": sum(
            float(stream.get("ipc_write_ms", "0")) for stream in arrow_streams
        ),
        "schema_rewrite_ms_total": sum(
            float(stream.get("schema_rewrite_ms", "0")) for stream in arrow_streams
        ),
        "combine_ms_total": sum(
            float(stream.get("combine_ms", "0")) for stream in arrow_streams
        ),
        "max_source_batch_bytes_estimated": max(
            (
                int(stream.get("max_source_batch_bytes_estimated", "0"))
                for stream in arrow_streams
            ),
            default=0,
        ),
        "peak_buffered_frame_bytes_estimated": max(
            (
                int(stream.get("peak_buffered_frame_bytes_estimated", "0"))
                for stream in arrow_streams
            ),
            default=0,
        ),
        "peak_combined_frame_bytes_estimated": max(
            (
                int(stream.get("peak_combined_frame_bytes_estimated", "0"))
                for stream in arrow_streams
            ),
            default=0,
        ),
        "peak_ipc_buffer_bytes": max(
            (int(stream.get("peak_ipc_buffer_bytes", "0")) for stream in arrow_streams),
            default=0,
        ),
        "peak_ipc_rewrite_bytes_estimated": max(
            (
                int(stream.get("peak_ipc_rewrite_bytes_estimated", "0"))
                for stream in arrow_streams
            ),
            default=0,
        ),
        "serialized_ipc_bytes_total": sum(
            int(stream.get("serialized_ipc_bytes", "0")) for stream in arrow_streams
        ),
        "emitted_ipc_bytes_total": sum(
            int(stream.get("emitted_ipc_bytes", "0")) for stream in arrow_streams
        ),
    }
    for field in (
        "admission_wait_ms",
        "admission_reacquire_ms",
        "planning_ms",
        "arrow_next_ms",
        "replay_accounting_ms",
        "blocked_send_ms",
    ):
        values = [float(sample[field]) for sample in producer_phases if field in sample]
        summary[f"{field}_p50"] = percentile(values, 0.50)
        summary[f"{field}_p95"] = percentile(values, 0.95)
        summary[f"{field}_p99"] = percentile(values, 0.99)
    if slow_reader_operation_id is not None:
        summary["slow_reader_producer_phases"] = next(
            (
                sample
                for sample in producer_phases
                if sample.get("operation_id") == slow_reader_operation_id
            ),
            None,
        )
    return summary


def run_cohort(
    client_python: Path,
    url: str,
    output_dir: Path,
    rows: int,
    iterations: int,
    concurrency: int,
    server_pid: int,
    http_url: str,
    profile: str,
    duration_seconds: int,
    batch_rows: int,
    slow_reader_rows: int,
    slow_reader_pause_ms: float,
    http_probe_timeout_seconds: float,
    non_reattachable_slow_reader: bool,
    http_sql_enabled: bool = True,
    allow_capacity_rejections: bool = False,
    owner_pid: int | None = None,
) -> dict[str, Any]:
    trace_log_path = output_dir / "server.log"
    trace_start = trace_log_path.stat().st_size if trace_log_path.exists() else 0
    cohort_dir = output_dir / f"clients-{concurrency}"
    cohort_dir.mkdir(parents=True, exist_ok=True)
    barrier = cohort_dir / "start.barrier"
    ready_files = [cohort_dir / f"client-{index}.ready" for index in range(concurrency)]
    commands = [
        [
            str(client_python),
            str(CLIENT_SCRIPT),
            "--url",
            url,
            "--rows",
            str(rows),
            "--iterations",
            str(iterations),
            "--profile",
            profile,
            "--duration-seconds",
            str(duration_seconds),
            "--batch-rows",
            str(batch_rows),
            "--ready-file",
            str(ready_files[index]),
            "--barrier-file",
            str(barrier),
        ]
        for index in range(concurrency)
    ]
    slow_reader_ready = cohort_dir / "slow-reader.ready"
    environment = clean_client_environment()
    processes = [
        subprocess.Popen(
            command,
            cwd=ROOT,
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for command in commands
    ]
    slow_reader_process: subprocess.Popen[str] | None = None
    if profile == "419":
        slow_reader_command = [
            str(client_python),
            str(RAW_SLOW_READER_SCRIPT),
            "--url",
            url,
            "--rows",
            str(slow_reader_rows),
            "--pause-ms",
            str(slow_reader_pause_ms),
            "--ready-file",
            str(slow_reader_ready),
            "--barrier-file",
            str(barrier),
        ]
        if non_reattachable_slow_reader:
            slow_reader_command.append("--non-reattachable")
        slow_reader_process = subprocess.Popen(
            slow_reader_command,
            cwd=ROOT,
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    progress: ProgressMonitor | None = None
    sampler: ProcessSampler | None = None
    owner_sampler: ProcessSampler | None = None
    try:
        readiness_files = [*ready_files]
        if profile == "419":
            readiness_files.append(slow_reader_ready)
        monitored_processes = [*processes]
        if slow_reader_process is not None:
            monitored_processes.append(slow_reader_process)
        wait_for_ready_files(readiness_files, monitored_processes)
        sampler = ProcessSampler(server_pid)
        sampler.start()
        if owner_pid is not None:
            owner_sampler = ProcessSampler(owner_pid, label="owner")
            owner_sampler.start()
        stage_started = time.perf_counter()
        barrier.write_text("start\n", encoding="utf-8")
        progress = ProgressMonitor(
            http_url,
            http_probe_timeout_seconds,
            http_sql_enabled=http_sql_enabled,
        )
        progress.start()
        results = []
        capacity_rejections: list[dict[str, Any]] = []
        for index, process in enumerate(processes):
            stdout, stderr = process.communicate(
                timeout=max(120, iterations * 60, duration_seconds * 2 + 60)
            )
            if process.returncode:
                rejection = connect_capacity_rejection(stderr, "client", index)
                if not allow_capacity_rejections or rejection is None:
                    raise RuntimeError(
                        f"benchmark client {index} failed ({process.returncode}): {stderr[-4000:]}"
                    )
                capacity_rejections.append(rejection)
                continue
            try:
                results.append(json.loads(stdout.strip().splitlines()[-1]))
            except (IndexError, json.JSONDecodeError) as error:
                raise RuntimeError(
                    f"benchmark client {index} returned invalid JSON: {stdout[-2000:]}"
                ) from error
        slow_reader_result = None
        if slow_reader_process is not None:
            slow_stdout, slow_stderr = slow_reader_process.communicate(
                timeout=max(120, duration_seconds * 2 + 60)
            )
            if slow_reader_process.returncode:
                rejection = connect_capacity_rejection(slow_stderr, "slow_reader")
                if not allow_capacity_rejections or rejection is None:
                    raise RuntimeError(
                        "mixed-cohort slow reader failed "
                        f"({slow_reader_process.returncode}): {slow_stderr[-4000:]}"
                    )
                capacity_rejections.append(rejection)
            else:
                try:
                    slow_reader_result = json.loads(
                        slow_stdout.strip().splitlines()[-1]
                    )
                except (IndexError, json.JSONDecodeError) as error:
                    raise RuntimeError(
                        "mixed-cohort slow reader returned invalid JSON: "
                        f"{slow_stdout[-2000:]}"
                    ) from error
        elapsed = time.perf_counter() - stage_started
        probe_result = progress.stop()
        process_result = sampler.stop()
        owner_process_result = owner_sampler.stop() if owner_sampler is not None else {}
    finally:
        barrier.touch(exist_ok=True)
        if progress is not None and not progress.stop_event.is_set():
            progress.stop()
        if sampler is not None and not sampler.stop_event.is_set():
            sampler.stop()
        if owner_sampler is not None and not owner_sampler.stop_event.is_set():
            owner_sampler.stop()
        for process in processes:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=5)
        if slow_reader_process is not None:
            if slow_reader_process.poll() is None:
                slow_reader_process.kill()
            slow_reader_process.wait(timeout=5)

    latencies = [
        float(measurement["elapsed_ms"])
        for client in results
        for measurement in client["measurements"]
    ]
    count_latencies = [
        float(measurement["elapsed_ms"])
        for client in results
        for measurement in client["measurements"]
        if measurement["kind"] == "filtered_count"
    ]
    collect_latencies = [
        float(measurement["elapsed_ms"])
        for client in results
        for measurement in client["measurements"]
        if measurement["kind"] == "multi_batch_collect"
    ]
    short_job_latencies = []
    short_job_by_kind = {}
    for kind in ("small_count", "preview_25"):
        samples = [
            float(measurement["elapsed_ms"])
            for client in results
            for measurement in client["measurements"]
            if measurement["kind"] == kind
        ]
        short_job_latencies.extend(samples)
        short_job_by_kind[kind] = {
            "sample_count": len(samples),
            **{
                label: percentile(samples, fraction)
                for label, fraction in (
                    ("p50_ms", 0.50),
                    ("p95_ms", 0.95),
                    ("p99_ms", 0.99),
                )
            },
        }
    operation_count = len(latencies)
    with trace_log_path.open("rb") as trace_log:
        trace_log.seek(trace_start)
        phase_trace = phase_trace_summary(
            trace_log.read().decode("utf-8", "replace"),
            slow_reader_operation_id=(slow_reader_result or {}).get("operation_id"),
        )
    workload_seconds = (
        max(client["finished_monotonic_seconds"] for client in results) - stage_started
        if results
        else elapsed
    )
    throughput = (
        operation_count / (elapsed if profile == "v26" else workload_seconds)
        if operation_count
        else 0.0
    )
    client_fingerprints = [client["client_session_fingerprint"] for client in results]
    client_cpu_seconds = [float(client["cpu_seconds"]) for client in results]
    if len(set(client_fingerprints)) != len(client_fingerprints):
        raise AssertionError(
            "concurrent benchmark processes did not use distinct client sessions"
        )
    if len(results) != concurrency and not allow_capacity_rejections:
        raise AssertionError("benchmark lost a client without capacity-rejection mode")
    if not probe_result["health_requests"] or (
        http_sql_enabled and not probe_result["http_statements"]
    ):
        raise RuntimeError(
            "required progress probes did not run during the Connect cohort"
        )
    if probe_result["probe_errors"]:
        raise RuntimeError(
            f"health or HTTP progress failed during the Connect cohort: {probe_result['probe_errors']}"
        )
    if process_result["server_sampling_errors"]:
        raise RuntimeError(
            "native server resource sampling failed: "
            f"{process_result['server_sampling_errors']}"
        )
    if process_result["server_samples"] < 2:
        raise RuntimeError(
            "native server resource sampling did not collect enough samples for CPU/RSS"
        )
    if owner_process_result.get("owner_sampling_errors"):
        raise RuntimeError(
            "DuckDB owner resource sampling failed: "
            f"{owner_process_result['owner_sampling_errors']}"
        )
    return {
        "concurrency": concurrency,
        "iterations_per_client": iterations if profile == "v26" else None,
        "profile": profile,
        "duration_seconds_per_client": duration_seconds if profile == "419" else None,
        "rows_per_count": rows,
        "rows_per_collect": batch_rows,
        "wall_seconds": elapsed,
        "workload_seconds": workload_seconds,
        "correct_operations": operation_count,
        "operations_per_second": throughput,
        "client_cpu_seconds": client_cpu_seconds,
        "client_cpu_seconds_total": sum(client_cpu_seconds),
        "latency_p50_ms": percentile(latencies, 0.50),
        "latency_p95_ms": percentile(latencies, 0.95),
        "latency_p99_ms": percentile(latencies, 0.99),
        "latency_sample_count": len(latencies),
        "short_job_latency_p50_ms": percentile(short_job_latencies, 0.50),
        "short_job_latency_p95_ms": percentile(short_job_latencies, 0.95),
        "short_job_latency_p99_ms": percentile(short_job_latencies, 0.99),
        "short_job_sample_count": len(short_job_latencies),
        "short_job_latency_by_kind": short_job_by_kind,
        "filtered_count_latency_p50_ms": percentile(count_latencies, 0.50),
        "filtered_count_latency_p95_ms": percentile(count_latencies, 0.95),
        "filtered_count_latency_p99_ms": percentile(count_latencies, 0.99),
        "multi_batch_collect_latency_p50_ms": percentile(collect_latencies, 0.50),
        "multi_batch_collect_latency_p95_ms": percentile(collect_latencies, 0.95),
        "client_peak_rss_bytes": [client["peak_rss_bytes"] for client in results],
        "client_session_fingerprints": client_fingerprints,
        "capacity_rejections": capacity_rejections,
        "per_client_measurements": [client["measurements"] for client in results],
        "concurrent_slow_reader": slow_reader_result,
        "phase_trace": phase_trace,
        **probe_result,
        **process_result,
        **owner_process_result,
    }


def read_python_versions(client_python: Path) -> dict[str, str]:
    command = [
        str(client_python),
        "-c",
        "import importlib.metadata as m,json,sys; "
        "names=('pyspark-client','grpcio','protobuf','pyarrow'); "
        "print(json.dumps({'python':sys.version.split()[0], "
        "**{name.replace('-','_'):m.version(name) for name in names}}))",
    ]
    result = subprocess.run(
        command, check=True, capture_output=True, text=True, cwd=ROOT
    )
    try:
        versions = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise RuntimeError(
            "could not determine pinned public client versions"
        ) from error
    if versions.get("pyspark_client") != "4.0.5":
        raise RuntimeError(
            "benchmark requires pyspark-client==4.0.5, got "
            f"{versions.get('pyspark_client')}"
        )
    return versions


def binary_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as binary:
        for chunk in iter(lambda: binary.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def repository_revision() -> str:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
        cwd=ROOT,
    )
    return result.stdout.strip()


def repository_dirty_paths() -> list[str]:
    result = subprocess.run(
        ["git", "status", "--porcelain=v1", "--untracked-files=all"],
        check=True,
        capture_output=True,
        text=True,
        cwd=ROOT,
    )
    return [line[3:] for line in result.stdout.splitlines() if len(line) >= 4]


def repository_dirty_digest(paths: list[str]) -> str:
    manifest = []
    for relative_path in sorted(paths):
        path = ROOT / relative_path
        if path.is_file():
            content_digest = hashlib.sha256(path.read_bytes()).hexdigest()
            manifest.append((relative_path, path.stat().st_size, content_digest))
        else:
            manifest.append((relative_path, None, "deleted-or-unreadable"))
    serialized = json.dumps(manifest, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def command_version(command: str) -> str | None:
    try:
        result = subprocess.run(
            [command, "--version"], check=True, capture_output=True, text=True
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return result.stdout.strip().splitlines()[0]


def host_details() -> dict[str, str | int | None]:
    cpu_model = platform.processor() or None
    physical_memory_bytes = None
    if sys.platform == "darwin":
        try:
            cpu_model = (
                subprocess.run(
                    ["sysctl", "-n", "machdep.cpu.brand_string"],
                    check=True,
                    capture_output=True,
                    text=True,
                ).stdout.strip()
                or cpu_model
            )
        except (OSError, subprocess.CalledProcessError):
            pass
        try:
            physical_memory_bytes = int(
                subprocess.run(
                    ["sysctl", "-n", "hw.memsize"],
                    check=True,
                    capture_output=True,
                    text=True,
                ).stdout.strip()
            )
        except (OSError, subprocess.CalledProcessError, ValueError):
            pass
    elif sys.platform.startswith("linux"):
        try:
            for line in Path("/proc/cpuinfo").read_text(encoding="utf-8").splitlines():
                if line.lower().startswith("model name"):
                    cpu_model = line.split(":", 1)[1].strip()
                    break
            physical_memory_bytes = os.sysconf("SC_PAGE_SIZE") * os.sysconf(
                "SC_PHYS_PAGES"
            )
        except (OSError, ValueError):
            pass
    return {
        "cpu_model": cpu_model,
        "physical_memory_bytes": physical_memory_bytes,
        "rustc": command_version("rustc"),
        "cargo": command_version("cargo"),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client-python", type=Path, required=True)
    parser.add_argument("--server-bin", type=Path, default=DEFAULT_BINARY)
    parser.add_argument(
        "--duckdb-local-owner",
        action="store_true",
        help="run the DuckDB product against a fresh local execution-owner database",
    )
    parser.add_argument(
        "--quack-owner-bin",
        type=Path,
        help="run a separate local Quack fixture owner for a shared-owner V38 measurement",
    )
    parser.add_argument("--quack-extension-path", type=Path)
    parser.add_argument(
        "--allow-capacity-rejections",
        action="store_true",
        help="record bounded saturation rejections; paired comparisons still require a clean candidate",
    )
    parser.add_argument(
        "--baseline-bin", type=Path, help="compare with this binary in rotating pairs"
    )
    parser.add_argument("--pair-count", type=int, default=3)
    parser.add_argument(
        "--min-throughput-ratio",
        type=float,
        help="minimum candidate/baseline correct throughput ratio for paired runs",
    )
    parser.add_argument(
        "--build", action="store_true", help="build the optimized combined server first"
    )
    parser.add_argument(
        "--profile",
        choices=("v26", "419"),
        default="v26",
        help="keep the historical V26 witness or run sustained 4.19 mixed workloads",
    )
    parser.add_argument(
        "--concurrency",
        default=None,
        help="comma-separated client process counts (default depends on profile)",
    )
    parser.add_argument("--iterations", type=int, default=5)
    parser.add_argument("--duration-seconds", type=int, default=60)
    parser.add_argument("--active-operations", type=int, default=2)
    parser.add_argument("--queue-timeout-ms", type=int, default=5_000)
    parser.add_argument(
        "--response-timeout-ms",
        type=int,
        default=60_000,
        help="per-operation deadline; raise it for deliberately paced long-result diagnostics",
    )
    parser.add_argument("--operation-history-records", type=int, default=1024)
    parser.add_argument("--operation-history-bytes", type=int, default=4 * 1024 * 1024)
    parser.add_argument("--http-probe-timeout-seconds", type=float, default=5.0)
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--collect-rows", type=int, default=None)
    parser.add_argument("--slow-reader-rows", type=int, default=None)
    parser.add_argument("--slow-reader-pause-ms", type=float, default=100.0)
    parser.add_argument(
        "--non-reattachable-slow-reader",
        action="store_true",
        help="run the 4.19 mixed-cohort slow reader without response reattachment",
    )
    parser.add_argument("--output", type=Path, default=ROOT / "target/connect-slice8")
    args = parser.parse_args()
    if args.duckdb_local_owner and args.quack_owner_bin is not None:
        parser.error("choose either --duckdb-local-owner or --quack-owner-bin")
    if (args.quack_owner_bin is None) != (args.quack_extension_path is None):
        parser.error(
            "--quack-owner-bin and --quack-extension-path must be used together"
        )
    if (
        args.duckdb_local_owner or args.quack_owner_bin
    ) and args.baseline_bin is not None:
        parser.error("DuckDB owner modes are candidate-only V38 measurements")
    if (args.duckdb_local_owner or args.quack_owner_bin) and args.build:
        parser.error("build the DuckDB server product before a V38 owner-mode run")
    if args.baseline_bin is not None:
        if args.profile != "419" or args.build:
            parser.error("paired runs require --profile 419 and prebuilt binaries")
        if args.pair_count < 3 or args.pair_count % 2 == 0:
            parser.error("--pair-count must be odd and at least three")
        if args.min_throughput_ratio is None or not 0 < args.min_throughput_ratio <= 1:
            parser.error("paired runs require --min-throughput-ratio in (0, 1]")
    elif args.min_throughput_ratio is not None:
        parser.error("--min-throughput-ratio requires --baseline-bin")

    with benchmark_lock():
        if args.baseline_bin is None:
            run(args)
            return 0
        return run_comparison(args)


def run_comparison(args: argparse.Namespace) -> int:
    binaries = {
        "baseline": args.baseline_bin.resolve(),
        "candidate": args.server_bin.resolve(),
    }
    hashes = {side: binary_sha256(path) for side, path in binaries.items()}
    if hashes["baseline"] == hashes["candidate"]:
        raise ValueError("baseline and candidate must be distinct binaries")

    comparison_dir = args.output / (
        datetime.now(UTC).strftime("paired-%Y%m%dT%H%M%S.%fZ") + f"-{os.getpid()}"
    )
    comparison_dir.mkdir(parents=True)
    summary_path = comparison_dir / "comparison.json"
    summary = {
        "schema_version": 1,
        "command": [sys.executable, *sys.argv],
        "binary_sha256": hashes,
        "pair_count": args.pair_count,
        "min_throughput_ratio": args.min_throughput_ratio,
        "short_job_regression_budget": {
            "p50": {"relative": 0.05, "absolute_ms": 1},
            "p95": {"relative": 0.10, "absolute_ms": 2},
        },
        "pairs": [],
        "cohorts": [],
    }
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    comparisons_by_concurrency = {}
    for pair in range(1, args.pair_count + 1):
        order = ["baseline", "candidate"] if pair % 2 else ["candidate", "baseline"]
        reports = {}
        for side in order:
            print(f"pair={pair} side={side}", flush=True)
            reports[side] = run(
                argparse.Namespace(
                    **{
                        **vars(args),
                        "server_bin": binaries[side],
                        "output": comparison_dir / f"pair-{pair}-{side}",
                    }
                )
            )
        baseline, candidate = (
            json.loads(reports[side].read_text(encoding="utf-8"))
            for side in ("baseline", "candidate")
        )
        require_candidate_without_capacity_rejections(candidate, pair)
        if (
            baseline["settings"] != candidate["settings"]
            or not baseline["server_shutdown"]["graceful"]
            or not candidate["server_shutdown"]["graceful"]
            or baseline["environment"]["server_binary_sha256"] != hashes["baseline"]
            or candidate["environment"]["server_binary_sha256"] != hashes["candidate"]
        ):
            raise RuntimeError(
                "paired runs need matching settings, expected binaries, and graceful shutdown"
            )
        pair_results = []
        for base, cand in zip(baseline["cohorts"], candidate["cohorts"], strict=True):
            if base["concurrency"] != cand["concurrency"]:
                raise RuntimeError("paired cohorts used different client counts")
            result = {
                "concurrency": base["concurrency"],
                "baseline_operations_per_second": base["operations_per_second"],
                "candidate_operations_per_second": cand["operations_per_second"],
                "throughput_ratio": cand["operations_per_second"]
                / base["operations_per_second"],
                "baseline_capacity_rejection_count": baseline[
                    "capacity_rejection_count"
                ],
                "candidate_capacity_rejection_count": candidate[
                    "capacity_rejection_count"
                ],
            }
            for quantile, budget in summary["short_job_regression_budget"].items():
                baseline_ms = base[f"short_job_latency_{quantile}_ms"]
                candidate_ms = cand[f"short_job_latency_{quantile}_ms"]
                limit_ms = max(baseline_ms * budget["relative"], budget["absolute_ms"])
                result[f"baseline_short_job_{quantile}_ms"] = baseline_ms
                result[f"candidate_short_job_{quantile}_ms"] = candidate_ms
                result[f"short_job_{quantile}_change_ms"] = candidate_ms - baseline_ms
                result[f"short_job_{quantile}_limit_ms"] = limit_ms
                result[f"short_job_{quantile}_over_budget"] = (
                    candidate_ms - baseline_ms > limit_ms
                )
            pair_results.append(result)
            comparisons_by_concurrency.setdefault(base["concurrency"], []).append(
                result
            )
        summary["pairs"].append(
            {
                "pair": pair,
                "order": order,
                "reports": {
                    side: str(path.relative_to(comparison_dir))
                    for side, path in reports.items()
                },
                "cohorts": pair_results,
            }
        )
        summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    for concurrency, comparisons in comparisons_by_concurrency.items():
        median_ratio = statistics.median(
            sample["throughput_ratio"] for sample in comparisons
        )
        cohort = {
            "concurrency": concurrency,
            "median_throughput_ratio": median_ratio,
            "meets_throughput_floor": median_ratio >= args.min_throughput_ratio,
        }
        for quantile in summary["short_job_regression_budget"]:
            cohort[f"median_short_job_{quantile}_change_ms"] = statistics.median(
                sample[f"short_job_{quantile}_change_ms"] for sample in comparisons
            )
            cohort[f"short_job_{quantile}_breached_pairs"] = sum(
                sample[f"short_job_{quantile}_over_budget"] for sample in comparisons
            )
        cohort["meets_short_job_budget"] = all(
            cohort[f"short_job_{quantile}_breached_pairs"] <= args.pair_count // 2
            for quantile in summary["short_job_regression_budget"]
        )
        summary["cohorts"].append(cohort)
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(f"comparison={summary_path}", flush=True)
    return int(
        not all(
            cohort["meets_throughput_floor"] and cohort["meets_short_job_budget"]
            for cohort in summary["cohorts"]
        )
    )


def run(args: argparse.Namespace) -> Path:
    client_python = args.client_python.absolute()
    server_bin = args.server_bin.absolute()
    if not client_python.is_file():
        raise FileNotFoundError(f"pinned client interpreter not found: {client_python}")
    if (
        args.iterations < 1
        or args.rows < 10
        or args.active_operations < 1
        or args.queue_timeout_ms < 1
        or args.response_timeout_ms < 1
        or args.operation_history_records < 1
        or args.operation_history_bytes < 1
        or not math.isfinite(args.slow_reader_pause_ms)
        or args.slow_reader_pause_ms < 0
        or not math.isfinite(args.http_probe_timeout_seconds)
        or args.http_probe_timeout_seconds <= 0
    ):
        raise ValueError(
            "iterations, workload rows, active operations, queue timeout, and operation-history limits must be positive; pause duration must be non-negative and timeouts must be positive"
        )
    slow_reader_rows = args.slow_reader_rows
    if slow_reader_rows is None:
        slow_reader_rows = 1_000_000 if args.profile == "419" else 200_000
    if slow_reader_rows < 1:
        raise ValueError("slow-reader rows must be positive")
    collect_rows = args.collect_rows
    if collect_rows is None:
        collect_rows = 100_000 if args.profile == "419" else 20_000
    if args.profile == "419" and args.duration_seconds < 60:
        raise ValueError("the 4.19 scheduling profile requires at least 60 seconds")
    concurrency_text = args.concurrency or (
        "1,4,8,16" if args.profile == "419" else "1,2,4,8"
    )
    concurrencies = [int(value) for value in concurrency_text.split(",")]
    if (
        not concurrencies
        or any(value < 1 for value in concurrencies)
        or sorted(set(concurrencies)) != concurrencies
    ):
        raise ValueError(
            "concurrency must be increasing, unique, positive client counts"
        )
    if max(concurrencies) < 8:
        raise ValueError("the benchmark requires measurements at eight or more clients")

    build_command = [
        "cargo",
        "build",
        "--locked",
        "--release",
        "-p",
        "sparkless-server",
        "--no-default-features",
        "--features",
        "http,connect",
    ]
    if args.build:
        subprocess.run(build_command, cwd=ROOT, check=True)
        server_bin = DEFAULT_BINARY.absolute()
    if not server_bin.is_file():
        raise FileNotFoundError(
            f"native server binary not found: {server_bin}; pass --build"
        )
    quack_owner_binary = (
        args.quack_owner_bin.resolve() if args.quack_owner_bin is not None else None
    )
    quack_extension_path = (
        args.quack_extension_path.resolve()
        if args.quack_extension_path is not None
        else None
    )
    if quack_owner_binary is not None and not quack_owner_binary.is_file():
        raise FileNotFoundError(f"Quack fixture owner not found: {quack_owner_binary}")
    if quack_extension_path is not None and not quack_extension_path.is_file():
        raise FileNotFoundError(f"Quack extension not found: {quack_extension_path}")

    args.output.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now(UTC).strftime("run-%Y%m%dT%H%M%SZ") + f"-{os.getpid()}"
    run_dir = args.output / run_id
    run_dir.mkdir()
    versions = read_python_versions(client_python)
    version_result = subprocess.run(
        [str(server_bin), "--version"], check=True, capture_output=True, text=True
    )
    server_version = version_result.stdout.strip().split()[-1]

    server: subprocess.Popen[str] | None = None
    quack_owner: subprocess.Popen[str] | None = None
    quack_owner_record: dict[str, Any] | None = None
    quack_owner_database = run_dir / "quack-owner.duckdb"
    quack_owner_log = run_dir / "quack-owner.log"
    quack_owner_config: dict[str, Any] | None = None
    owner_shutdown_result: dict[str, Any] | None = None
    local_owner_database = (
        run_dir / "execution-owner.duckdb" if args.duckdb_local_owner else None
    )
    duckdb_mode = args.duckdb_local_owner or quack_owner_binary is not None
    try:
        if quack_owner_binary is not None and quack_extension_path is not None:
            quack_owner, quack_owner_record = start_quack_owner(
                quack_owner_binary,
                quack_extension_path,
                quack_owner_database,
                free_port(),
                quack_owner_log,
            )
            quack_owner_config = {
                "mode": "quack",
                "quack_uri": quack_owner_record["uri"],
                "token": quack_owner_record["token"],
                "extension_path": str(quack_extension_path),
                "disable_ssl": True,
            }
        backend_environment = execution_backend_environment(
            local_owner_database, quack_owner_config
        )
        server, ready_record, startup_seconds = start_server(
            server_bin,
            run_dir / "server.log",
            args.rows,
            args.active_operations,
            args.queue_timeout_ms,
            args.operation_history_records,
            args.operation_history_bytes,
            local_owner_database,
            quack_owner_config,
            args.response_timeout_ms,
        )
        http_url = ready_record["http_url"]
        connect_url = ready_record["spark_connect_url"]
        stages = [
            run_cohort(
                client_python,
                connect_url,
                run_dir,
                args.rows,
                args.iterations,
                concurrency,
                server.pid,
                http_url,
                args.profile,
                args.duration_seconds,
                collect_rows,
                slow_reader_rows,
                args.slow_reader_pause_ms,
                args.http_probe_timeout_seconds,
                args.non_reattachable_slow_reader,
                http_sql_enabled=not duckdb_mode,
                allow_capacity_rejections=args.allow_capacity_rejections,
                owner_pid=quack_owner.pid if quack_owner is not None else None,
            )
            for concurrency in concurrencies
        ]
        final_capacity_rejection = None

        slow_progress = ProgressMonitor(
            http_url,
            args.http_probe_timeout_seconds,
            http_sql_enabled=not duckdb_mode,
        )
        slow_progress.start()
        slow_sampler = ProcessSampler(server.pid)
        slow_sampler.start()
        owner_sampler = (
            ProcessSampler(quack_owner.pid, label="owner")
            if quack_owner is not None
            else None
        )
        if owner_sampler is not None:
            owner_sampler.start()
        slow_started = time.perf_counter()
        try:
            slow_reader_command = [
                str(client_python),
                str(
                    RAW_SLOW_READER_SCRIPT
                    if args.profile == "419"
                    else SLOW_READER_SCRIPT
                ),
                "--url",
                connect_url,
                "--rows",
                str(slow_reader_rows),
            ]
            if args.profile == "419":
                slow_reader_command.extend(
                    ["--pause-ms", str(args.slow_reader_pause_ms)]
                )
                if args.non_reattachable_slow_reader:
                    slow_reader_command.append("--non-reattachable")
            slow = subprocess.run(
                slow_reader_command,
                cwd=ROOT,
                env=clean_client_environment(),
                check=False,
                capture_output=True,
                text=True,
                timeout=300,
            )
            slow_elapsed = time.perf_counter() - slow_started
        finally:
            slow_progress_result = slow_progress.stop()
            slow_resource_result = slow_sampler.stop()
            slow_owner_resource_result = (
                owner_sampler.stop() if owner_sampler is not None else {}
            )
        if slow.returncode:
            final_capacity_rejection = connect_capacity_rejection(
                slow.stderr, "final_slow_reader"
            )
            if not args.allow_capacity_rejections or final_capacity_rejection is None:
                raise RuntimeError(
                    "final slow-reader workload failed "
                    f"({slow.returncode}): {slow.stderr[-4000:]}"
                )
            slow_result = {"rows": None}
        else:
            try:
                slow_result = json.loads(slow.stdout.strip().splitlines()[-1])
            except (IndexError, json.JSONDecodeError) as error:
                raise RuntimeError(
                    f"final slow reader returned invalid JSON: {slow.stdout[-2000:]}"
                ) from error
        if final_capacity_rejection is None and slow_result["rows"] != slow_reader_rows:
            raise AssertionError("slow-reader witness returned an unexpected row count")
        if not slow_progress_result["health_requests"] or (
            not duckdb_mode and not slow_progress_result["http_statements"]
        ):
            raise RuntimeError(
                "required progress probes did not run during slow-reader workload"
            )
        if slow_progress_result["probe_errors"]:
            raise RuntimeError(
                f"health or HTTP progress failed during slow-reader workload: {slow_progress_result['probe_errors']}"
            )

        replay_port = free_port()
        replay_address = f"127.0.0.1:{replay_port}"
        replay_url = f"sc://localhost:{replay_port}/;token=slice8-local-route-token"
        proxy_log_path = run_dir / "fault-proxy.log"
        proxy_log = proxy_log_path.open("w", encoding="utf-8")
        proxy_environment = os.environ.copy()
        proxy_environment["TZ"] = "UTC"
        proxy = subprocess.Popen(
            [
                str(client_python),
                str(FAULT_PROXY_SCRIPT),
                "--listen",
                replay_address,
                "--upstream",
                connect_url.removeprefix("sc://"),
                "--local-auth",
            ],
            cwd=ROOT,
            env=proxy_environment,
            stdout=proxy_log,
            stderr=subprocess.STDOUT,
            text=True,
        )
        proxy_shutdown_result: dict[str, Any] | None = None
        try:
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                if proxy.poll() is not None:
                    raise RuntimeError(
                        f"fault proxy exited before ready with {proxy.returncode}"
                    )
                try:
                    with socket.create_connection(
                        ("127.0.0.1", replay_port), timeout=0.2
                    ):
                        break
                except OSError:
                    time.sleep(0.05)
            else:
                raise TimeoutError("fault proxy failed to bind its replay witness port")
            log_text = (run_dir / "server.log").read_text(encoding="utf-8")
            producers_before = len(re.findall(r"event=producer_started", log_text))
            reattaches_before = len(
                re.findall(r"event=rpc method=ReattachExecute", log_text)
            )
            replay = subprocess.run(
                [
                    str(client_python),
                    str(RETRY_CLIENT_SCRIPT),
                    replay_url,
                ],
                cwd=ROOT,
                env=clean_client_environment(),
                check=True,
                capture_output=True,
                text=True,
                timeout=240,
            )
            proxy_log.flush()
            proxy_text = proxy_log_path.read_text(encoding="utf-8")
            server_text = (run_dir / "server.log").read_text(encoding="utf-8")
            producer_delta = (
                len(re.findall(r"event=producer_started", server_text))
                - producers_before
            )
            reattach_delta = (
                len(re.findall(r"event=rpc method=ReattachExecute", server_text))
                - reattaches_before
            )
            if (
                producer_delta != 1
                or reattach_delta < 1
                or "injected UNAVAILABLE" not in proxy_text
            ):
                raise AssertionError(
                    "default-client retry did not reattach through the fault proxy exactly once"
                )
            replay_result = {
                "public_client_output": replay.stdout.strip(),
                "native_producer_starts": producer_delta,
                "reattach_rpcs": reattach_delta,
                "fault_injected": True,
            }
            route_environment = clean_client_environment()
            route_environment.update(
                {
                    "SPARKLESS_PROXY_URL": f"sc://localhost:{replay_port}",
                    "SPARKLESS_PROXY_TOKEN": "slice8-local-route-token",
                    "SPARKLESS_PROXY_CANCEL_RANGE_ROWS": str(
                        1_000_000 if duckdb_mode else 10_000_000
                    ),
                }
            )
            try:
                local_route = subprocess.run(
                    [str(client_python), str(PROXY_CLIENT_SCRIPT), "workload"],
                    cwd=ROOT,
                    env=route_environment,
                    check=True,
                    capture_output=True,
                    text=True,
                    timeout=360,
                )
            except subprocess.CalledProcessError as error:
                (run_dir / "local-proxy-route.log").write_text(
                    error.stdout + error.stderr, encoding="utf-8"
                )
                raise RuntimeError(
                    "local fault-proxy public-client route failed; see "
                    f"{run_dir / 'local-proxy-route.log'}"
                ) from error
            (run_dir / "local-proxy-route.log").write_text(
                local_route.stdout + local_route.stderr, encoding="utf-8"
            )
            local_route_result = json.loads(local_route.stdout.strip().splitlines()[-1])
            if (
                not local_route_result.get("same_session_affinity")
                or not local_route_result.get("release_isolation")
                or not local_route_result.get("stale_identity_rejected", {}).get(
                    "rejected"
                )
                or not local_route_result.get("cancellation", {}).get("cancelled")
                or local_route_result.get("multi_batch_arrow_stream", {}).get("rows")
                != 20_000
            ):
                raise AssertionError(
                    "local fault-proxy route did not pass its session/stream lifecycle checks"
                )
        finally:
            proxy_shutdown_result = stop_fault_proxy(proxy)
            proxy_log.close()
        if not proxy_shutdown_result["graceful"]:
            raise RuntimeError(
                "local fault proxy required a forced or unsuccessful shutdown"
            )

        shutdown_result = stop_server(server)
        server = None
        if quack_owner is not None:
            stop_quack_owner(quack_owner)
            owner_shutdown_result = {"graceful": True, "exit_code": 0}
            quack_owner = None
        dirty_paths = repository_dirty_paths()
        report = {
            "result": "capacity_rejections_observed"
            if final_capacity_rejection
            or any(stage["capacity_rejections"] for stage in stages)
            else "passed",
            "capacity_rejection_count": sum(
                len(stage["capacity_rejections"]) for stage in stages
            )
            + int(final_capacity_rejection is not None),
            "schema_version": 1 if args.profile == "v26" else 2,
            "slice": "420-04-v38-duckdb-load"
            if duckdb_mode
            else "418-08-local-v26"
            if args.profile == "v26"
            else "419-11-mixed-scheduling",
            "run_id": run_id,
            "source_revision": repository_revision(),
            "source_dirty_paths": dirty_paths,
            "source_dirty_sha256": repository_dirty_digest(dirty_paths),
            "command": [sys.executable, *sys.argv],
            "build_command": build_command if args.build else None,
            "server_command": ready_record["_command"],
            "environment": {
                "platform": platform.platform(),
                "machine": platform.machine(),
                "processor": platform.processor(),
                "logical_cpu_count": os.cpu_count(),
                **versions,
                **host_details(),
                "server_version": server_version,
                "server_binary_sha256": binary_sha256(server_bin),
                "execution_backend": ready_record.get("execution_backend"),
                "proxy_version": None,
                "reference_spark_version": None,
                "timezone": "UTC",
            },
            "settings": {
                "concurrency": concurrencies,
                "profile": args.profile,
                "iterations_per_client": args.iterations
                if args.profile == "v26"
                else None,
                "duration_seconds_per_client": args.duration_seconds
                if args.profile == "419"
                else None,
                "count_rows": args.rows,
                "collect_rows": collect_rows,
                "slow_reader_rows": slow_reader_rows,
                "slow_reader_pause_ms": args.slow_reader_pause_ms,
                "slow_reader_reattachable": not args.non_reattachable_slow_reader,
                "active_operations": args.active_operations,
                "duckdb_local_owner": args.duckdb_local_owner,
                "duckdb_owner_mode": "quack"
                if quack_owner_record is not None
                else "local"
                if args.duckdb_local_owner
                else None,
                "allow_capacity_rejections": args.allow_capacity_rejections,
                "queue_timeout_ms": args.queue_timeout_ms,
                "response_timeout_ms": args.response_timeout_ms,
                "operation_history_records": args.operation_history_records,
                "operation_history_bytes": args.operation_history_bytes,
                "http_probe_timeout_seconds": args.http_probe_timeout_seconds,
                "warmup": "one filtered count and 32-row collect per client, excluded from timed cohorts",
                "regression_budget": None,
                "interpretation": "measurements only; no numeric regression budget or performance claim is implied",
                "server_config_environment": {
                    **server_configuration(
                        args.rows,
                        args.active_operations,
                        args.queue_timeout_ms,
                        args.operation_history_records,
                        args.operation_history_bytes,
                        args.response_timeout_ms,
                    ),
                    **redacted_execution_backend_environment(backend_environment),
                },
                "server_thread_environment": {
                    key: None for key in THREAD_ENVIRONMENT_KEYS
                },
            },
            "server_startup_seconds": startup_seconds,
            "server_shutdown": shutdown_result,
            "quack_owner_shutdown": owner_shutdown_result,
            "local_fault_proxy_shutdown": proxy_shutdown_result,
            "duckdb_owner": {
                "mode": "quack",
                "uri": quack_owner_record["uri"],
                "database_path": str(quack_owner_database),
                "binary": str(quack_owner_binary),
                "binary_sha256": binary_sha256(quack_owner_binary),
                "extension": str(quack_extension_path),
                "extension_sha256": hashlib.sha256(
                    quack_extension_path.read_bytes()
                ).hexdigest(),
                "resource_sampling": "owner process RSS is sampled separately",
            }
            if quack_owner_record is not None
            else {
                "mode": "local" if args.duckdb_local_owner else None,
                "database_path": str(local_owner_database)
                if local_owner_database is not None
                else None,
                "resource_sampling": "owner RSS is included in server process RSS"
                if local_owner_database is not None
                else None,
            },
            "cohorts": stages,
            "slow_reader": {
                "wall_seconds": slow_elapsed,
                "capacity_rejection": final_capacity_rejection,
                **slow_result,
                **slow_progress_result,
                **slow_resource_result,
                **slow_owner_resource_result,
            },
            "disconnected_reader_replay": replay_result,
            "local_fault_proxy_route_workload": local_route_result,
            "artifacts": {
                "server_log": str(run_dir / "server.log"),
                "fault_proxy_log": str(proxy_log_path),
                "local_proxy_client_log": str(run_dir / "local-proxy-route.log"),
                "quack_owner_log": str(quack_owner_log)
                if quack_owner_binary is not None
                else None,
            },
        }
        report_path = run_dir / "benchmark.json"
        report_path.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        print(f"report={report_path}", flush=True)
        return report_path
    finally:
        if server is not None:
            try:
                stop_server(server)
            except BaseException:
                if server.poll() is None:
                    server.kill()
                    server.wait(timeout=5)
                raise
        if quack_owner is not None:
            try:
                stop_quack_owner(quack_owner)
            except BaseException:
                if quack_owner.poll() is None:
                    quack_owner.kill()
                    quack_owner.wait(timeout=5)
                raise


if __name__ == "__main__":
    raise SystemExit(main())
