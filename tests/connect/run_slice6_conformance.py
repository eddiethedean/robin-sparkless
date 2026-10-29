"""Integrated Spark Connect Slice 6 conformance runner for a local native build."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CLIENT = Path("/tmp/sparkless-connect-client-py311/bin/python")
DEFAULT_ORACLE_CLIENT = Path("/tmp/sparkless-connect-oracle-py311/bin/python")
DEFAULT_ORACLE_ARCHIVE = Path("/tmp/spark-4.0.5-bin-hadoop3-connect.tgz")
ORACLE_ARCHIVE_SHA512 = "4cc2e43959b5e0a67b7d895e1aa1fb3ab05fa6ad4a5912de76236bb9b70f9557b20837b05ee63474798e927130e72ed6d1fadcd273a71e65c70eac18771c0d6b"


def free_port() -> int:
    with socket.socket() as server_socket:
        server_socket.bind(("127.0.0.1", 0))
        return server_socket.getsockname()[1]


def wait_for_port(
    address: str,
    process: subprocess.Popen,
    logfile: Path,
    timeout: float = 60,
    allow_launcher_exit: bool = False,
) -> None:
    host, port_text = address.rsplit(":", 1)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None and not allow_launcher_exit:
            raise RuntimeError(
                f"process exited {process.returncode} before {address} was ready; see {logfile}"
            )
        try:
            with socket.create_connection((host, int(port_text)), timeout=0.25):
                return
        except OSError:
            time.sleep(0.1)
    raise TimeoutError(f"timed out waiting for {address}; see {logfile}")


def wait_for_port_closed(address: str, timeout: float = 30) -> None:
    host, port_text = address.rsplit(":", 1)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, int(port_text)), timeout=0.25):
                time.sleep(0.1)
        except OSError:
            return
    raise TimeoutError(f"listener {address} remained open after shutdown")


def wait_for_clean_exit(process: subprocess.Popen, name: str, timeout: float) -> None:
    try:
        return_code = process.wait(timeout=timeout)
    except subprocess.TimeoutExpired as error:
        process.kill()
        process.wait(timeout=10)
        raise RuntimeError(
            f"{name} did not shut down cleanly and required a forced kill"
        ) from error
    if return_code != 0:
        raise RuntimeError(f"{name} exited with status {return_code}")


def stop_process(process: subprocess.Popen, name: str, timeout: float) -> None:
    """Request shutdown and reap a process, escalating only when it hangs."""
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=timeout)
    except subprocess.TimeoutExpired as error:
        process.kill()
        process.wait(timeout=10)
        raise RuntimeError(
            f"{name} did not stop after SIGTERM and required a forced kill"
        ) from error


def wait_for_process_group_exit(process: subprocess.Popen, timeout: float) -> None:
    """Reap the launcher and wait until its isolated group has no running members."""
    deadline = time.monotonic() + timeout
    while True:
        process.poll()
        completed = subprocess.run(
            ["ps", "-axo", "pgid=,stat="], check=True, text=True, capture_output=True
        )
        running = False
        for line in completed.stdout.splitlines():
            fields = line.split()
            if (
                len(fields) == 2
                and int(fields[0]) == process.pid
                and not fields[1].startswith("Z")
            ):
                running = True
                break
        if not running:
            process.wait(timeout=max(0.1, deadline - time.monotonic()))
            return
        if time.monotonic() >= deadline:
            raise subprocess.TimeoutExpired(process.args, timeout)
        time.sleep(0.1)


def stop_process_group(process: subprocess.Popen, name: str, timeout: float) -> None:
    """Stop an isolated launcher group, even if its launcher has already exited."""
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        process.wait(timeout=10)
        return
    try:
        wait_for_process_group_exit(process, timeout)
    except subprocess.TimeoutExpired as error:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        wait_for_process_group_exit(process, 10)
        raise RuntimeError(
            f"{name} process group did not stop after SIGTERM and required a forced kill"
        ) from error


def distribution_versions(
    python: Path, env: dict[str, str], *names: str
) -> dict[str, str]:
    script = (
        "import importlib.metadata as m, json, sys; "
        "print(json.dumps({name: m.version(name) for name in sys.argv[1:]}))"
    )
    try:
        return json.loads(
            subprocess.check_output(
                [str(python), "-c", script, *names], text=True, env=env
            )
        )
    except subprocess.CalledProcessError as error:
        raise RuntimeError(
            f"could not read required Python distributions {', '.join(names)} from {python}; "
            "verify the dedicated virtual environment is installed"
        ) from error


def health(http_url: str) -> bool:
    try:
        with urllib.request.urlopen(f"{http_url}/health", timeout=2) as response:
            payload = json.load(response)
            return response.status == 200 and payload.get("status") == "ready"
    except (OSError, urllib.error.URLError, TimeoutError, json.JSONDecodeError):
        return False


def http_witness(http_url: str, sequence: int) -> None:
    body = json.dumps(
        {
            "session": f"slice6-http-{sequence}",
            "code": "SELECT 1 AS result",
            "kind": "sql",
        }
    ).encode()
    request = urllib.request.Request(
        f"{http_url}/statements",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        payload = json.load(response)
    result = payload["data"]["application/json"]
    assert result["schema"]["fields"][0]["name"] == "result", payload
    assert result["data"] == [[1]], payload


def run_logged(
    command: list[str], log_path: Path, env: dict[str, str], timeout: float = 180
) -> None:
    with log_path.open("w", encoding="utf-8") as log:
        completed = subprocess.run(
            command,
            cwd=ROOT,
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
            timeout=timeout,
            check=False,
        )
    if completed.returncode:
        raise RuntimeError(
            f"command failed ({completed.returncode}): {' '.join(command)}; see {log_path}"
        )


def count_trace(log_path: Path, event: str, value: str | None = None) -> int:
    if not log_path.exists():
        return 0
    needle = f"event={event}"
    if value:
        needle += f" {value}"
    return sum(
        needle in line for line in log_path.read_text(encoding="utf-8").splitlines()
    )


def sha512(path: Path) -> str:
    digest = hashlib.sha512()
    with path.open("rb") as archive:
        while chunk := archive.read(4 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def native_descendants(server_pid: int) -> list[tuple[int, str]]:
    """Return descendant commands, allowing the runner to reject Python/JVM server workers."""
    completed = subprocess.run(
        ["ps", "-axo", "pid=,ppid=,comm="],
        check=True,
        text=True,
        capture_output=True,
    )
    processes = []
    for line in completed.stdout.splitlines():
        parts = line.strip().split(None, 2)
        if len(parts) == 3:
            processes.append((int(parts[0]), int(parts[1]), parts[2]))
    ancestors = {server_pid}
    descendants = []
    changed = True
    while changed:
        changed = False
        for pid, ppid, command in processes:
            if ppid in ancestors and pid not in ancestors:
                ancestors.add(pid)
                descendants.append((pid, command))
                changed = True
    return descendants


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client-python", type=Path, default=DEFAULT_CLIENT)
    parser.add_argument("--oracle-python", type=Path, default=DEFAULT_ORACLE_CLIENT)
    parser.add_argument("--oracle-archive", type=Path, default=DEFAULT_ORACLE_ARCHIVE)
    parser.add_argument("--server-bin", type=Path)
    parser.add_argument(
        "--server-cli-mode",
        choices=("integration-example", "serve-cli"),
        default="integration-example",
        help="launch the integration example arguments or production serve CLI",
    )
    parser.add_argument("--oracle-home", type=Path)
    parser.add_argument(
        "--reference-url", help="Compare against an already-running Spark 4.0.5 server"
    )
    parser.add_argument("--output", type=Path, default=ROOT / "target/connect-slice6")
    parser.add_argument("--skip-concurrent", action="store_true")
    args = parser.parse_args()

    # Keep the virtualenv entry path: resolving its symlink escapes the environment.
    client_python = args.client_python.absolute()
    if not client_python.is_file():
        raise FileNotFoundError(
            f"locked pyspark-client interpreter not found: {client_python}"
        )
    oracle_python = args.oracle_python.absolute()
    if args.reference_url and not oracle_python.is_file():
        oracle_python = client_python

    client_env = os.environ.copy()
    for key in (
        "SPARK_HOME",
        "SPARK_CONF_DIR",
        "PYSPARK_PYTHON",
        "PYSPARK_DRIVER_PYTHON",
        "PYSPARK_SUBMIT_ARGS",
        "PYTHONPATH",
    ):
        client_env.pop(key, None)
    client_env["TZ"] = "UTC"
    client_distributions = distribution_versions(
        client_python, client_env, "pyspark-client"
    )
    client_version = client_distributions["pyspark-client"]
    if client_version != "4.0.5":
        raise AssertionError(
            f"standalone client distribution must be pyspark-client 4.0.5, got {client_version}"
        )
    try:
        distribution_versions(client_python, client_env, "pyspark")
    except RuntimeError:
        pass
    else:
        raise AssertionError(
            "standalone client environment must not contain the full pyspark distribution"
        )

    reference_home = args.oracle_home.resolve() if args.oracle_home else None
    if reference_home:
        archive = args.oracle_archive.resolve()
        if not archive.is_file():
            raise FileNotFoundError(
                f"pinned Spark 4.0.5 oracle archive not found: {archive}"
            )
        if sha512(archive) != ORACLE_ARCHIVE_SHA512:
            raise AssertionError(
                f"Spark reference archive SHA-512 does not match the pinned distribution: {archive}"
            )
        release_file = reference_home / "RELEASE"
        if not release_file.is_file() or not release_file.read_text(
            encoding="utf-8"
        ).startswith("Spark 4.0.5 (git revision b472a3aa3c4"):
            raise AssertionError(
                f"reference home is not the pinned Apache Spark 4.0.5 build: {reference_home}"
            )
    if reference_home or args.reference_url:
        if not oracle_python.is_file():
            raise FileNotFoundError(
                f"separate pyspark[connect]==4.0.5 environment not found: {oracle_python}"
            )
        oracle_env = client_env.copy()
        oracle_distribution = (
            "pyspark-client"
            if args.reference_url and oracle_python == client_python
            else "pyspark"
        )
        oracle_distributions = distribution_versions(
            oracle_python, oracle_env, oracle_distribution
        )
        oracle_client_version = oracle_distributions[oracle_distribution]
        if oracle_client_version != "4.0.5":
            raise AssertionError(
                f"oracle client must be {oracle_distribution} 4.0.5, got {oracle_client_version}"
            )
    else:
        oracle_client_version = None
        oracle_distribution = None

    output_root = args.output.resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    run_dir = output_root / datetime.now(UTC).strftime(
        f"run-%Y%m%dT%H%M%SZ-{os.getpid()}"
    )
    run_dir.mkdir(parents=True, exist_ok=False)
    evidence_log = run_dir / "native-server.log"

    server_bin = (
        args.server_bin.resolve()
        if args.server_bin
        else ROOT / "target/release/examples/connect-integration-server"
    )
    build_command = [
        "cargo",
        "build",
        "--locked",
        "--release",
        "-p",
        "sparkless-server",
        "--example",
        "connect-integration-server",
        "--no-default-features",
        "--features",
        "http,connect",
    ]
    if not args.server_bin:
        run_logged(
            build_command, run_dir / "native-build.log", os.environ.copy(), timeout=900
        )
    if not server_bin.is_file():
        raise FileNotFoundError(
            f"native integration server binary not found: {server_bin}"
        )

    http_port, connect_port = free_port(), free_port()
    while connect_port == http_port:
        connect_port = free_port()
    http_address = f"127.0.0.1:{http_port}"
    connect_address = f"127.0.0.1:{connect_port}"
    http_url = f"http://{http_address}"
    connect_url = f"sc://{connect_address}"

    server_env = os.environ.copy()
    for key in (
        "JAVA_HOME",
        "SPARK_HOME",
        "SPARK_CONF_DIR",
        "PYSPARK_PYTHON",
        "PYSPARK_DRIVER_PYTHON",
        "PYSPARK_SUBMIT_ARGS",
        "PYTHONPATH",
    ):
        server_env.pop(key, None)
    server_env.update(
        {
            "SPARKLESS_CONNECT_TRACE": "1",
            "SPARKLESS_RESPONSE_TIMEOUT_MS": "300000",
            "TZ": "UTC",
        }
    )
    server_log = evidence_log.open("w", encoding="utf-8")
    if args.server_cli_mode == "serve-cli":
        http_host, http_port = http_address.rsplit(":", 1)
        connect_host, connect_port = connect_address.rsplit(":", 1)
        server_command = [
            str(server_bin),
            "serve",
            "--host",
            http_host,
            "--port",
            http_port,
            "--connect-host",
            connect_host,
            "--connect-port",
            connect_port,
        ]
    else:
        server_command = [str(server_bin), http_address, connect_address]
    native_server = subprocess.Popen(
        server_command,
        cwd=ROOT,
        env=server_env,
        stdout=server_log,
        stderr=subprocess.STDOUT,
        text=True,
    )
    reference_process: subprocess.Popen | None = None
    reference_log_file = None
    try:
        wait_for_port(http_address, native_server, evidence_log)
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline and not health(http_url):
            if native_server.poll() is not None:
                raise RuntimeError(
                    f"native server exited {native_server.returncode}; see {evidence_log}"
                )
            time.sleep(0.1)
        if not health(http_url):
            raise TimeoutError(
                f"native HTTP health did not become ready; see {evidence_log}"
            )
        wait_for_port(connect_address, native_server, evidence_log)
        descendants = native_descendants(native_server.pid)
        forbidden = [
            (pid, command)
            for pid, command in descendants
            if any(word in command.lower() for word in ("python", "java"))
        ]
        if forbidden:
            raise AssertionError(
                f"native server started a Python/JVM child process: {forbidden}"
            )

        client_run_env = client_env | {"SPARKLESS_CONNECT_URL": connect_url}
        run_logged(
            [str(client_python), "tests/connect/smoke_public_client.py"],
            run_dir / "public-client-smoke.log",
            client_run_env,
        )
        run_logged(
            [str(client_python), "tests/connect/smoke_lifecycle_public_client.py"],
            run_dir / "public-client-lifecycle.log",
            client_run_env | {"SPARKLESS_CONNECT_ADDRESS": connect_address},
        )

        proxy_port = free_port()
        proxy_address = f"127.0.0.1:{proxy_port}"
        proxy_log = (run_dir / "fault-proxy.log").open("w", encoding="utf-8")
        proxy_env = client_run_env.copy()
        proxy = subprocess.Popen(
            [
                str(client_python),
                "tests/connect/fault_proxy.py",
                "--listen",
                proxy_address,
                "--upstream",
                connect_address,
            ],
            cwd=ROOT,
            env=proxy_env,
            stdout=proxy_log,
            stderr=subprocess.STDOUT,
            text=True,
        )
        try:
            wait_for_port(proxy_address, proxy, run_dir / "fault-proxy.log")
            producer_before = count_trace(evidence_log, "producer_started")
            reattach_before = count_trace(evidence_log, "rpc", "method=ReattachExecute")
            run_logged(
                [
                    str(client_python),
                    "tests/connect/slice6_retry_client.py",
                    f"sc://{proxy_address}",
                ],
                run_dir / "default-retry-client.log",
                client_run_env | {"SPARKLESS_CONNECT_URL": f"sc://{proxy_address}"},
                timeout=240,
            )
            if count_trace(evidence_log, "producer_started") - producer_before != 1:
                raise AssertionError(
                    "default client recovery started more than one native producer"
                )
            if (
                count_trace(evidence_log, "rpc", "method=ReattachExecute")
                <= reattach_before
            ):
                raise AssertionError(
                    "default client did not use ReattachExecute after the injected loss"
                )
            proxy_log.flush()
            proxy_text = (run_dir / "fault-proxy.log").read_text(encoding="utf-8")
            if "injected UNAVAILABLE" not in proxy_text:
                raise AssertionError(
                    "the fault proxy did not drop an ExecutePlan stream"
                )
        finally:
            proxy.terminate()
            try:
                proxy.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proxy.kill()
                proxy.wait(timeout=10)
            proxy_log.close()

        error_rpc_before = count_trace(evidence_log, "rpc", "method=FetchErrorDetails")
        run_logged(
            [str(client_python), "tests/connect/slice6_client_errors.py", connect_url],
            run_dir / "client-errors.log",
            client_run_env,
            timeout=360,
        )
        if (
            count_trace(evidence_log, "rpc", "method=FetchErrorDetails")
            <= error_rpc_before
        ):
            raise AssertionError("public client did not fetch enriched error details")

        if args.reference_url:
            ref_url = args.reference_url
        elif reference_home:
            start_script = reference_home / "sbin/start-connect-server.sh"
            if not start_script.is_file():
                raise FileNotFoundError(
                    f"Apache Spark Connect startup script not found: {start_script}"
                )
            reference_port = free_port()
            ref_url = f"sc://127.0.0.1:{reference_port}"
            reference_log = run_dir / "reference-spark.log"
            reference_log_file = reference_log.open("w", encoding="utf-8")
            spark_pid_dir = run_dir / "spark-pids"
            spark_log_dir = run_dir / "spark-logs"
            spark_pid_dir.mkdir()
            spark_log_dir.mkdir()
            reference_env = client_env.copy()
            reference_env.update(
                {
                    "SPARK_HOME": str(reference_home),
                    "SPARK_PID_DIR": str(spark_pid_dir),
                    "SPARK_LOG_DIR": str(spark_log_dir),
                    "SPARK_IDENT_STRING": f"sparkless-slice6-{os.getpid()}",
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
                ],
                cwd=reference_home,
                env=reference_env,
                stdout=reference_log_file,
                stderr=subprocess.STDOUT,
                text=True,
                start_new_session=True,
            )
            wait_for_port(
                f"127.0.0.1:{reference_port}",
                reference_process,
                reference_log,
                timeout=240,
            )
        else:
            ref_url = None

        native_result = run_dir / "native-workload.json"
        run_logged(
            [
                str(client_python),
                "tests/connect/slice6_public_workload.py",
                "--url",
                connect_url,
                "--output",
                str(native_result),
            ],
            run_dir / "native-workload.log",
            client_run_env,
            timeout=300,
        )
        native_values = json.loads(native_result.read_text(encoding="utf-8"))
        if native_values["server_reported_spark_version"] != "4.0.5":
            raise AssertionError(
                "native Spark Connect implementation reported an unexpected semantic profile: "
                f"{native_values['server_reported_spark_version']}"
            )
        arrow_rows = [
            (int(size), int(rows), int(offset))
            for size, rows, offset in re.findall(
                r"event=arrow_response bytes=(\d+) rows=(\d+) start_offset=(\d+)",
                evidence_log.read_text(encoding="utf-8"),
            )
        ]
        if not arrow_rows:
            raise AssertionError(
                "native result trace did not contain Arrow response evidence"
            )
        if max(size for size, _, _ in arrow_rows) > 1024 * 1024:
            raise AssertionError(
                "native Arrow response exceeded the public response-byte limit"
            )
        if max(size for size, _, _ in arrow_rows) < 900_000:
            raise AssertionError(
                "byte-splitting workload did not approach the configured response-byte limit"
            )
        if max(rows for _, rows, _ in arrow_rows) > 16_384:
            raise AssertionError(
                "native Arrow response exceeded the public response row limit"
            )

        comparison = "skipped"
        if ref_url:
            oracle_result = run_dir / "reference-workload.json"
            reference_python = (
                oracle_python if oracle_python.is_file() else client_python
            )
            run_logged(
                [
                    str(reference_python),
                    "tests/connect/slice6_public_workload.py",
                    "--url",
                    ref_url,
                    "--output",
                    str(oracle_result),
                ],
                run_dir / "reference-workload.log",
                client_env,
                timeout=240,
            )
            oracle_values = json.loads(oracle_result.read_text(encoding="utf-8"))
            if native_values != oracle_values:
                raise AssertionError(
                    "native results or schemas differ from Spark 4.0.5 reference; "
                    f"native={native_result}, reference={oracle_result}"
                )
            if oracle_values["server_reported_spark_version"] != "4.0.5":
                raise AssertionError("reference server did not report Spark 4.0.5")
            comparison = "passed"

        concurrency = "skipped"
        health_count = 0
        http_count = 0
        if not args.skip_concurrent:
            worker_count = 4
            barrier = run_dir / "concurrent-go"
            workers = []
            for index in range(worker_count):
                ready_file = run_dir / f"worker-{index}.ready"
                log_path = run_dir / f"worker-{index}.log"
                log = log_path.open("w", encoding="utf-8")
                process = subprocess.Popen(
                    [
                        str(client_python),
                        "tests/connect/slice6_concurrent_client.py",
                        "--url",
                        connect_url,
                        "--ready-file",
                        str(ready_file),
                        "--barrier-file",
                        str(barrier),
                    ],
                    cwd=ROOT,
                    env=client_run_env,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    text=True,
                )
                workers.append((process, log, ready_file, log_path))
            deadline = time.monotonic() + 90
            while time.monotonic() < deadline and not all(
                item[2].exists() for item in workers
            ):
                if any(item[0].poll() is not None for item in workers):
                    break
                time.sleep(0.05)
            if not all(item[2].exists() for item in workers):
                for process, log, _, _ in workers:
                    if process.poll() is None:
                        process.terminate()
                        try:
                            process.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            process.kill()
                            process.wait(timeout=5)
                    log.close()
                raise RuntimeError(
                    "concurrent public clients did not all initialize; inspect worker logs"
                )
            barrier.write_text("go\n", encoding="utf-8")
            next_http_at = 0.0
            http_sequence = 0
            deadline = time.monotonic() + 240
            try:
                while any(item[0].poll() is None for item in workers):
                    if time.monotonic() >= deadline:
                        raise TimeoutError(
                            "concurrent public clients did not finish within four minutes"
                        )
                    if health(http_url):
                        health_count += 1
                    now = time.monotonic()
                    if now >= next_http_at:
                        http_witness(http_url, http_sequence)
                        http_sequence += 1
                        http_count += 1
                        next_http_at = now + 0.2
                    time.sleep(0.025)
                for process, log, _, log_path in workers:
                    if process.wait(timeout=10) != 0:
                        raise RuntimeError(f"concurrent client failed; see {log_path}")
                    log.close()
            finally:
                for process, log, _, _ in workers:
                    if process.poll() is None:
                        process.terminate()
                        try:
                            process.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            process.kill()
                            process.wait(timeout=5)
                    if not log.closed:
                        log.close()
            if health_count < 10 or http_count < 3:
                raise AssertionError(
                    f"insufficient concurrent health/HTTP progress: health={health_count}, HTTP={http_count}"
                )
            for _, _, _, log_path in workers:
                text = log_path.read_text(encoding="utf-8")
                if text.count("passed") < 4:
                    raise AssertionError(f"client result missing from {log_path}")
            concurrency = "passed"

        if reference_process and reference_home:
            stop_process_group(reference_process, "Spark Connect server", 30)
            wait_for_port_closed(f"127.0.0.1:{reference_port}")

        native_server.terminate()
        wait_for_clean_exit(native_server, "native integration server", 20)

        server_log.flush()
        server_log_text = evidence_log.read_text(encoding="utf-8")
        if (
            "panicked at" in server_log_text
            or "not yet implemented: unimplemented: AnonymousScan" in server_log_text
        ):
            raise AssertionError(
                "native Connect execution triggered a Polars panic; inspect native-server.log"
            )
        producer_count = count_trace(evidence_log, "producer_started")
        summary = {
            "slice": "418-06",
            "date_utc": datetime.now(UTC).isoformat(),
            "platform": sys.platform,
            "client_python": str(client_python),
            "client_version": client_version,
            "server_binary": str(server_bin),
            "server_cli_mode": args.server_cli_mode,
            "server_response_timeout_ms": 300_000,
            "http_url": http_url,
            "connect_url": connect_url,
            "reference_comparison": comparison,
            "reference_url": ref_url,
            "oracle_server_home": str(reference_home) if reference_home else None,
            "oracle_client_version": oracle_client_version,
            "oracle_client_distribution": oracle_distribution,
            "concurrent_clients": concurrency,
            "concurrent_health_responses": health_count,
            "concurrent_http_statements": http_count,
            "native_producer_starts": producer_count,
            "native_arrow_response_count": len(arrow_rows),
            "native_arrow_response_max_bytes": max(size for size, _, _ in arrow_rows),
            "native_arrow_response_max_rows": max(rows for _, rows, _ in arrow_rows),
            "default_retry_producer_delta": 1,
            "error_detail_rpc_observed": True,
            "native_server_python_or_jvm_children": descendants,
        }
        (run_dir / "summary.json").write_text(
            json.dumps(summary, indent=2) + "\n", encoding="utf-8"
        )
        print(json.dumps(summary, indent=2))
    finally:
        try:
            if reference_process:
                stop_process_group(reference_process, "Spark Connect server", 15)
        finally:
            try:
                stop_process(native_server, "native integration server", 20)
            finally:
                server_log.close()
                if reference_log_file:
                    reference_log_file.close()


if __name__ == "__main__":
    main()
