"""Exercise the packaged DuckDB backend through public sync/async launchers."""

from __future__ import annotations

import argparse
import asyncio
from datetime import UTC, datetime
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from urllib.parse import urlsplit
import urllib.request

from sparkless_server_duckdb import AsyncSparklessServer, SparklessServer


ROOT = Path(__file__).resolve().parents[2]
PUBLIC_SMOKE = ROOT / "tests/connect/smoke_public_client.py"
LIFECYCLE_SMOKE = ROOT / "tests/connect/smoke_lifecycle_public_client.py"
RETRY_SMOKE = ROOT / "tests/connect/slice6_retry_client.py"
CONCURRENT_CLIENT = ROOT / "tests/connect/slice6_concurrent_client.py"
FAULT_PROXY = ROOT / "tests/connect/fault_proxy.py"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def wait_for_port(address: str, process: subprocess.Popen[str]) -> None:
    host, port_text = address.rsplit(":", 1)
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"fault proxy exited with status {process.returncode}")
        try:
            with socket.create_connection((host, int(port_text)), timeout=0.25):
                return
        except OSError:
            time.sleep(0.05)
    raise TimeoutError(f"fault proxy did not start on {address}")


def stop_process(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def run_client(
    script: Path,
    connect_url: str,
    log_path: Path,
    *,
    address: str | None = None,
    extra_args: tuple[str, ...] = (),
) -> None:
    environment = os.environ.copy()
    for key in (
        "SPARK_HOME",
        "SPARK_CONF_DIR",
        "PYSPARK_PYTHON",
        "PYSPARK_DRIVER_PYTHON",
        "PYSPARK_SUBMIT_ARGS",
        "PYTHONPATH",
    ):
        environment.pop(key, None)
    environment.update({"SPARKLESS_CONNECT_URL": connect_url, "TZ": "UTC"})
    if address is not None:
        environment["SPARKLESS_CONNECT_ADDRESS"] = address
    command = [sys.executable, str(script), *extra_args]
    with log_path.open("w", encoding="utf-8") as log:
        completed = subprocess.run(
            command,
            cwd=ROOT,
            env=environment,
            stdout=log,
            stderr=subprocess.STDOUT,
            timeout=360,
            check=False,
        )
    if completed.returncode != 0:
        raise RuntimeError(
            f"{script.name} failed with status {completed.returncode}; see {log_path}"
        )


def verify_identity(
    server: SparklessServer | AsyncSparklessServer,
) -> dict[str, object]:
    record = server._ready_record
    assert isinstance(record, dict), "launcher should retain its readiness record"
    with urllib.request.urlopen(server.http_url + "/health", timeout=10) as response:
        health = json.load(response)
    with urllib.request.urlopen(
        server.http_url + "/capabilities", timeout=10
    ) as response:
        capabilities = json.load(response)
    assert response.status == 200
    assert health["status"] == "ready", health
    assert capabilities == record["capabilities"]
    assert record["server_product"] == "duckdb"
    assert record["execution_backend"] == "duckdb"
    assert capabilities["server_product"] == "duckdb"
    assert capabilities["execution_backend"] == "duckdb"
    assert capabilities["server_instance_id"] == record["server_instance_id"]
    assert capabilities["spark_connect_supported"] is True
    assert capabilities["duckdb_execution_owner_mode"] == "local"
    assert "spark_connect" in capabilities["transports"]
    return {
        "server_product": "duckdb",
        "execution_backend": "duckdb",
        "owner_mode": "local",
        "readiness_capabilities_match": True,
        "health_ready": True,
        "spark_connect_supported": True,
    }


def run_concurrent_clients(
    connect_url: str, address: str, run_dir: Path
) -> dict[str, int]:
    worker_count = 4
    barrier = run_dir / "concurrent-go"
    workers: list[tuple[subprocess.Popen[bytes], Path, Path]] = []
    for index in range(worker_count):
        ready = run_dir / f"worker-{index}.ready"
        log_path = run_dir / f"worker-{index}.log"
        environment = os.environ.copy()
        for key in (
            "SPARK_HOME",
            "SPARK_CONF_DIR",
            "PYSPARK_PYTHON",
            "PYSPARK_DRIVER_PYTHON",
            "PYSPARK_SUBMIT_ARGS",
            "PYTHONPATH",
        ):
            environment.pop(key, None)
        environment.update({"SPARKLESS_CONNECT_URL": connect_url, "TZ": "UTC"})
        with log_path.open("wb") as log:
            process = subprocess.Popen(
                [
                    sys.executable,
                    str(CONCURRENT_CLIENT),
                    "--url",
                    connect_url,
                    "--ready-file",
                    str(ready),
                    "--barrier-file",
                    str(barrier),
                    "--queries",
                    "4",
                    "--rows",
                    "1000000",
                ],
                cwd=ROOT,
                env=environment,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
        workers.append((process, ready, log_path))

    deadline = time.monotonic() + 90
    while time.monotonic() < deadline and not all(
        ready.exists() for _, ready, _ in workers
    ):
        if any(process.poll() is not None for process, _, _ in workers):
            break
        time.sleep(0.05)
    if not all(ready.exists() for _, ready, _ in workers):
        for process, _, _ in workers:
            stop_process(process)
        raise RuntimeError("four public Connect client processes did not initialize")

    barrier.write_text("go\n", encoding="utf-8")
    health_responses = 0
    deadline = time.monotonic() + 240
    try:
        while any(process.poll() is None for process, _, _ in workers):
            if time.monotonic() >= deadline:
                raise TimeoutError("concurrent DuckDB Connect clients timed out")
            with urllib.request.urlopen(address + "/health", timeout=5) as response:
                health = json.load(response)
            if response.status == 200 and health.get("status") == "ready":
                health_responses += 1
            time.sleep(0.05)
        for process, _, log_path in workers:
            if process.returncode != 0:
                raise RuntimeError(f"concurrent client failed; see {log_path}")
    finally:
        for process, _, _ in workers:
            stop_process(process)
    if health_responses < 2:
        raise AssertionError(
            f"health made insufficient progress during client load: {health_responses}"
        )
    return {
        "client_processes": worker_count,
        "health_responses_during_load": health_responses,
    }


async def exercise_async_launcher(
    owner_config: str, run_dir: Path
) -> dict[str, object]:
    server = AsyncSparklessServer(
        startup_timeout=60,
        shutdown_timeout=20,
        kill_timeout=5,
        connect_port=0,
        env={
            "SPARKLESS_EXECUTION_BACKEND": "duckdb",
            "SPARKLESS_DUCKDB_EXECUTION_OWNER": owner_config,
        },
    )
    await server.start()
    try:
        result = verify_identity(server)
        address = urlsplit(server.spark_connect_url).netloc
        run_client(
            PUBLIC_SMOKE,
            server.spark_connect_url + "/;user_id=phase420-async-launcher",
            run_dir / "async-public-client.log",
            address=address,
        )
        result["public_client_smoke"] = "passed"
        return result
    finally:
        await server.stop()


def exercise_sync_launcher(owner_config: str, run_dir: Path) -> dict[str, object]:
    server = SparklessServer(
        startup_timeout=60,
        shutdown_timeout=20,
        kill_timeout=5,
        connect_port=0,
        env={
            "SPARKLESS_EXECUTION_BACKEND": "duckdb",
            "SPARKLESS_DUCKDB_EXECUTION_OWNER": owner_config,
            "SPARKLESS_CONNECT_TRACE": "1",
            "SPARKLESS_RESPONSE_TIMEOUT_MS": "300000",
            "TZ": "UTC",
        },
    )
    server.start()
    try:
        identity = verify_identity(server)
        connect_address = urlsplit(server.spark_connect_url).netloc
        client_url = server.spark_connect_url + "/;user_id=phase420-sync-launcher"
        run_client(
            PUBLIC_SMOKE,
            client_url,
            run_dir / "public-client-smoke.log",
            address=connect_address,
        )
        run_client(
            LIFECYCLE_SMOKE,
            client_url,
            run_dir / "public-client-lifecycle.log",
            address=connect_address,
        )

        proxy_address = f"127.0.0.1:{free_port()}"
        proxy_log_path = run_dir / "fault-proxy.log"
        with proxy_log_path.open("w", encoding="utf-8") as proxy_log:
            proxy = subprocess.Popen(
                [
                    sys.executable,
                    str(FAULT_PROXY),
                    "--listen",
                    proxy_address,
                    "--upstream",
                    connect_address,
                ],
                cwd=ROOT,
                stdout=proxy_log,
                stderr=subprocess.STDOUT,
            )
            try:
                wait_for_port(proxy_address, proxy)
                run_client(
                    RETRY_SMOKE,
                    f"sc://{proxy_address}/;user_id=phase420-retry-launcher",
                    run_dir / "default-retry-client.log",
                    address=proxy_address,
                )
            finally:
                stop_process(proxy)
        proxy_text = proxy_log_path.read_text(encoding="utf-8")
        assert "injected UNAVAILABLE" in proxy_text, "retry fault was not injected"

        concurrent = run_concurrent_clients(client_url, server.http_url, run_dir)
        identity.update(
            {
                "public_client_smoke": "passed",
                "multi_batch_prefix_release_reattach_release_all": "passed",
                "default_client_retry_after_disconnect": "passed",
                **concurrent,
            }
        )
        return identity
    except BaseException:
        if server.diagnostics:
            print(server.diagnostics[-8192:], file=sys.stderr)
        raise
    finally:
        server.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()

    binary = Path(__import__("sparkless_server_duckdb").executable_path()).resolve()
    version = importlib.metadata.version("sparkless-server-duckdb")
    assert version == "4.20.0", f"expected exact candidate wheel 4.20.0, got {version}"
    client_version = importlib.metadata.version("pyspark-client")
    assert client_version == "4.0.5", (
        f"expected pyspark-client 4.0.5, got {client_version}"
    )
    args.report.parent.mkdir(parents=True, exist_ok=True)

    log_dir = args.report.with_suffix("")
    if log_dir.exists():
        shutil.rmtree(log_dir)
    log_dir.mkdir(parents=True)
    with tempfile.TemporaryDirectory(prefix="sparkless-phase420-launcher-") as temp:
        database = Path(temp) / "execution.duckdb"
        owner_config = json.dumps({"mode": "local", "database_path": str(database)})
        sync_result = exercise_sync_launcher(owner_config, log_dir)
        async_result = asyncio.run(exercise_async_launcher(owner_config, log_dir))

    report = {
        "format_version": 1,
        "phase": "4.20",
        "date_utc": datetime.now(UTC).isoformat(),
        "result": "passed",
        "platform": platform.platform(),
        "server_distribution": "sparkless-server-duckdb",
        "server_version": version,
        "server_binary": str(binary),
        "server_binary_sha256": sha256(binary),
        "client_distribution": "pyspark-client",
        "client_version": client_version,
        "runner_sha256": sha256(Path(__file__).resolve()),
        "logs_directory": str(log_dir),
        "sync_launcher": sync_result,
        "async_launcher": async_result,
    }
    args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"Packaged DuckDB Connect lifecycle passed; report: {args.report}")


if __name__ == "__main__":
    main()
