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
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
CLIENT_SCRIPT = ROOT / "tests/connect/slice8_benchmark_client.py"
SLOW_READER_SCRIPT = ROOT / "tests/connect/slice8_slow_reader.py"
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
    def __init__(self, pid: int, interval_seconds: float = 0.1) -> None:
        self.pid = pid
        self.interval_seconds = interval_seconds
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
            "server_peak_rss_bytes": max(
                (rss for _, rss in self.samples), default=None
            ),
            "server_cpu_seconds_start": self.samples[0][0] if self.samples else None,
            "server_cpu_seconds_end": self.samples[-1][0] if self.samples else None,
            "server_cpu_seconds": (
                self.samples[-1][0] - self.samples[0][0]
                if len(self.samples) > 1
                else None
            ),
            "server_samples": len(self.samples),
            "server_sampling_errors": self.errors,
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


def start_server(
    binary: Path, log_path: Path, rows: int
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
            "SPARKLESS_CONNECT_TRACE": "1",
            "SPARKLESS_MAX_RANGE_ROWS": str(max(rows, 10_000_000, 200_000)),
            "SPARKLESS_RESPONSE_TIMEOUT_MS": "60000",
            "TZ": "UTC",
            **SERVER_CONFIG_ENVIRONMENT,
        }
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


def http_probe(http_url: str, sequence: int) -> tuple[float, bool]:
    body = json.dumps(
        {
            "session": f"slice8-benchmark-{sequence}",
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
        with urllib.request.urlopen(request, timeout=5) as response:
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
    def __init__(self, http_url: str) -> None:
        self.http_url = http_url
        self.stop_event = threading.Event()
        self.health_ms: list[float] = []
        self.http_ms: list[float] = []
        self.errors: list[str] = []
        self.threads = [
            threading.Thread(target=self._health_loop, daemon=True),
            threading.Thread(target=self._http_loop, daemon=True),
        ]

    def _health_loop(self) -> None:
        while not self.stop_event.is_set():
            elapsed, ok = health_probe(self.http_url)
            self.health_ms.append(elapsed)
            if not ok:
                self.errors.append("HTTP /health failed")
            self.stop_event.wait(0.1)

    def _http_loop(self) -> None:
        sequence = 0
        while not self.stop_event.is_set():
            elapsed, ok = http_probe(self.http_url, sequence)
            self.http_ms.append(elapsed)
            if not ok:
                self.errors.append("HTTP SELECT 1 failed")
            sequence += 1
            self.stop_event.wait(0.25)

    def start(self) -> None:
        for thread in self.threads:
            thread.start()

    def stop(self) -> dict[str, Any]:
        self.stop_event.set()
        for thread in self.threads:
            thread.join(timeout=7)
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


def run_cohort(
    client_python: Path,
    url: str,
    output_dir: Path,
    rows: int,
    iterations: int,
    concurrency: int,
    server_pid: int,
    http_url: str,
) -> dict[str, Any]:
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
            "--ready-file",
            str(ready_files[index]),
            "--barrier-file",
            str(barrier),
        ]
        for index in range(concurrency)
    ]
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
    progress: ProgressMonitor | None = None
    sampler: ProcessSampler | None = None
    try:
        wait_for_ready_files(ready_files, processes)
        sampler = ProcessSampler(server_pid)
        sampler.start()
        stage_started = time.perf_counter()
        barrier.write_text("start\n", encoding="utf-8")
        progress = ProgressMonitor(http_url)
        progress.start()
        results = []
        for index, process in enumerate(processes):
            stdout, stderr = process.communicate(timeout=max(120, iterations * 60))
            if process.returncode:
                raise RuntimeError(
                    f"benchmark client {index} failed ({process.returncode}): {stderr[-4000:]}"
                )
            try:
                results.append(json.loads(stdout.strip().splitlines()[-1]))
            except (IndexError, json.JSONDecodeError) as error:
                raise RuntimeError(
                    f"benchmark client {index} returned invalid JSON: {stdout[-2000:]}"
                ) from error
        elapsed = time.perf_counter() - stage_started
        probe_result = progress.stop()
        process_result = sampler.stop()
    finally:
        barrier.touch(exist_ok=True)
        if progress is not None and not progress.stop_event.is_set():
            progress.stop()
        if sampler is not None and not sampler.stop_event.is_set():
            sampler.stop()
        for process in processes:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=5)

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
    operation_count = len(latencies)
    throughput = operation_count / elapsed if elapsed else None
    client_fingerprints = [client["client_session_fingerprint"] for client in results]
    client_cpu_seconds = [float(client["cpu_seconds"]) for client in results]
    if len(set(client_fingerprints)) != concurrency:
        raise AssertionError(
            "concurrent benchmark processes did not use distinct client sessions"
        )
    if not probe_result["health_requests"] or not probe_result["http_statements"]:
        raise RuntimeError(
            "health and HTTP progress probes did not run during the Connect cohort"
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
    return {
        "concurrency": concurrency,
        "iterations_per_client": iterations,
        "rows_per_count": rows,
        "rows_per_collect": 20_000,
        "wall_seconds": elapsed,
        "correct_operations": operation_count,
        "operations_per_second": throughput,
        "client_cpu_seconds": client_cpu_seconds,
        "client_cpu_seconds_total": sum(client_cpu_seconds),
        "latency_p50_ms": percentile(latencies, 0.50),
        "latency_p95_ms": percentile(latencies, 0.95),
        "filtered_count_latency_p50_ms": percentile(count_latencies, 0.50),
        "filtered_count_latency_p95_ms": percentile(count_latencies, 0.95),
        "multi_batch_collect_latency_p50_ms": percentile(collect_latencies, 0.50),
        "multi_batch_collect_latency_p95_ms": percentile(collect_latencies, 0.95),
        "client_peak_rss_bytes": [client["peak_rss_bytes"] for client in results],
        "client_session_fingerprints": client_fingerprints,
        **probe_result,
        **process_result,
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
        "--build", action="store_true", help="build the optimized combined server first"
    )
    parser.add_argument(
        "--concurrency", default="1,2,4,8", help="comma-separated client process counts"
    )
    parser.add_argument("--iterations", type=int, default=5)
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--slow-reader-rows", type=int, default=200_000)
    parser.add_argument("--output", type=Path, default=ROOT / "target/connect-slice8")
    args = parser.parse_args()

    client_python = args.client_python.absolute()
    server_bin = args.server_bin.absolute()
    if not client_python.is_file():
        raise FileNotFoundError(f"pinned client interpreter not found: {client_python}")
    if args.iterations < 1 or args.rows < 10 or args.slow_reader_rows < 1:
        raise ValueError("iterations and workload rows must be positive")
    concurrencies = [int(value) for value in args.concurrency.split(",")]
    if (
        not concurrencies
        or any(value < 1 for value in concurrencies)
        or sorted(set(concurrencies)) != concurrencies
    ):
        raise ValueError(
            "concurrency must be increasing, unique, positive client counts"
        )
    if max(concurrencies) < 8:
        raise ValueError("Slice 8 requires measurements at eight or more clients")

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
    try:
        server, ready_record, startup_seconds = start_server(
            server_bin, run_dir / "server.log", args.rows
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
            )
            for concurrency in concurrencies
        ]

        slow_progress = ProgressMonitor(http_url)
        slow_progress.start()
        slow_sampler = ProcessSampler(server.pid)
        slow_sampler.start()
        slow_started = time.perf_counter()
        try:
            slow = subprocess.run(
                [
                    str(client_python),
                    str(SLOW_READER_SCRIPT),
                    "--url",
                    connect_url,
                    "--rows",
                    str(args.slow_reader_rows),
                ],
                cwd=ROOT,
                env=clean_client_environment(),
                check=True,
                capture_output=True,
                text=True,
                timeout=300,
            )
            slow_elapsed = time.perf_counter() - slow_started
        finally:
            slow_progress_result = slow_progress.stop()
            slow_resource_result = slow_sampler.stop()
        slow_result = json.loads(slow.stdout.strip().splitlines()[-1])
        if slow_result["rows"] != args.slow_reader_rows:
            raise AssertionError("slow-reader witness returned an unexpected row count")
        if (
            not slow_progress_result["health_requests"]
            or not slow_progress_result["http_statements"]
        ):
            raise RuntimeError(
                "health and HTTP progress probes did not run during slow-reader workload"
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
        dirty_paths = repository_dirty_paths()
        report = {
            "schema_version": 1,
            "slice": "418-08-local-v26",
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
                "proxy_version": None,
                "reference_spark_version": None,
                "timezone": "UTC",
            },
            "settings": {
                "concurrency": concurrencies,
                "iterations_per_client": args.iterations,
                "count_rows": args.rows,
                "collect_rows": 20_000,
                "slow_reader_rows": args.slow_reader_rows,
                "warmup": "one filtered count and 32-row collect per client, excluded from timed cohorts",
                "regression_budget": None,
                "interpretation": "measurements only; no numeric regression budget or performance claim is implied",
                "server_config_environment": {
                    **SERVER_CONFIG_ENVIRONMENT,
                    "SPARKLESS_MAX_RANGE_ROWS": str(
                        max(args.rows, 10_000_000, 200_000)
                    ),
                    "SPARKLESS_RESPONSE_TIMEOUT_MS": "60000",
                    "SPARKLESS_CONNECT_TRACE": "1",
                },
                "server_thread_environment": {
                    key: None for key in THREAD_ENVIRONMENT_KEYS
                },
            },
            "server_startup_seconds": startup_seconds,
            "server_shutdown": shutdown_result,
            "local_fault_proxy_shutdown": proxy_shutdown_result,
            "cohorts": stages,
            "slow_reader": {
                "wall_seconds": slow_elapsed,
                **slow_result,
                **slow_progress_result,
                **slow_resource_result,
            },
            "disconnected_reader_replay": replay_result,
            "local_fault_proxy_route_workload": local_route_result,
            "artifacts": {
                "server_log": str(run_dir / "server.log"),
                "fault_proxy_log": str(proxy_log_path),
                "local_proxy_client_log": str(run_dir / "local-proxy-route.log"),
            },
        }
        report_path = run_dir / "benchmark.json"
        report_path.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        print(
            json.dumps(
                {"report": str(report_path), "cohorts": stages},
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    finally:
        if server is not None:
            try:
                stop_server(server)
            except BaseException:
                if server.poll() is None:
                    server.kill()
                    server.wait(timeout=5)
                raise


if __name__ == "__main__":
    raise SystemExit(main())
