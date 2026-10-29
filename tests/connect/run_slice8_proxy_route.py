"""Run V25 proxy certification against an externally pinned Connect route.

The proxy revision and URL are mandatory. This is deliberately separate from the
standalone benchmark runner: local conformance cannot certify an emulator route.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import socket
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

from slice8_proxy_url import url_with_token


ROOT = Path(__file__).resolve().parents[2]
PROXY_CLIENT = ROOT / "tests/connect/slice8_proxy_client.py"
RETRY_CLIENT = ROOT / "tests/connect/slice6_retry_client.py"
ERROR_CLIENT = ROOT / "tests/connect/slice6_client_errors.py"
FAULT_PROXY = ROOT / "tests/connect/fault_proxy.py"


def require_sc_url(value: str) -> tuple[str, int, str]:
    parsed = urlsplit(value)
    try:
        port = parsed.port
    except ValueError as error:
        raise ValueError("proxy URL has an invalid port") from error
    if parsed.scheme != "sc" or not parsed.hostname or port is None:
        raise ValueError("proxy URL must be sc://host:port")
    if not 1 <= port <= 65_535:
        raise ValueError("proxy URL port must be between 1 and 65535")
    if parsed.path not in ("", "/") or parsed.query or parsed.fragment:
        raise ValueError("proxy URL must not contain a route path, query, or fragment")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("proxy URL must not contain credentials")
    # Spark 4.0.5 reads connection parameters after /; in the URI rather than '?'.
    host = parsed.hostname
    if ":" in host:
        raise ValueError(
            "pyspark-client==4.0.5 requires a DNS or IPv4 host in the sc:// URL"
        )
    return host, port, f"sc://{parsed.netloc}"


def public_client_version(python: Path) -> str:
    result = subprocess.run(
        [
            str(python),
            "-c",
            "import importlib.metadata as m; print(m.version('pyspark-client'))",
        ],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    version = result.stdout.strip()
    if version != "4.0.5":
        raise RuntimeError(f"V25 requires pyspark-client==4.0.5, got {version}")
    return version


def free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def run_client(
    python: Path,
    script: Path,
    mode: str,
    environment: dict[str, str],
    timeout: float = 240,
) -> dict[str, object]:
    result = subprocess.run(
        [str(python), str(script), mode],
        cwd=ROOT,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    try:
        output = json.loads(result.stdout.strip().splitlines()[-1])
    except (IndexError, json.JSONDecodeError) as error:
        raise RuntimeError(
            f"proxy client returned invalid JSON: {result.stdout[-2000:]}"
        ) from error
    return {"result": output, "stdout": result.stdout.strip()}


def start_fault_proxy(
    python: Path,
    output_dir: Path,
    upstream: str,
    tls_upstream: bool,
    local_auth_upstream: bool,
) -> tuple[subprocess.Popen[str], Path, int]:
    port = free_port()
    listen = f"localhost:{port}"
    log_path = output_dir / "authenticated-fault-proxy.log"
    log = log_path.open("w", encoding="utf-8")
    command = [
        str(python),
        str(FAULT_PROXY),
        "--listen",
        listen,
        "--upstream",
        upstream,
        "--local-auth",
    ]
    if tls_upstream:
        command.append("--upstream-tls")
    elif local_auth_upstream:
        command.append("--upstream-local-auth")
    process = subprocess.Popen(
        command,
        cwd=ROOT,
        env={**os.environ, "TZ": "UTC"},
        stdout=log,
        stderr=subprocess.STDOUT,
        text=True,
    )
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if process.poll() is not None:
            log.close()
            raise RuntimeError(
                f"authenticated fault proxy exited with {process.returncode}"
            )
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return process, log_path, port
        except OSError:
            time.sleep(0.05)
    process.terminate()
    process.wait(timeout=5)
    log.close()
    raise TimeoutError("authenticated fault proxy did not listen within 30 seconds")


def stop_fault_proxy(process: subprocess.Popen[str]) -> dict[str, object]:
    started = time.perf_counter()
    forced_kill = False
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=10)
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


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--proxy-url", default=os.environ.get("SPARKLESS_PROXY_URL"))
    parser.add_argument(
        "--proxy-revision", default=os.environ.get("SPARKLESS_PROXY_REVISION")
    )
    parser.add_argument("--client-python", type=Path, required=True)
    parser.add_argument(
        "--auth-token-env",
        default="SPARKLESS_PROXY_TOKEN",
        help="environment variable containing the valid proxy bearer token",
    )
    parser.add_argument(
        "--output", type=Path, default=ROOT / "target/connect-slice8/proxy"
    )
    args = parser.parse_args()

    if not args.proxy_url:
        parser.error(
            "--proxy-url is required; V25 cannot be skipped or inferred from standalone tests"
        )
    if not args.proxy_revision or not re.fullmatch(
        r"[0-9a-fA-F]{40,64}", args.proxy_revision
    ):
        parser.error(
            "--proxy-revision must be the immutable 40-64 character source commit SHA"
        )
    token = os.environ.get(args.auth_token_env)
    if not token:
        parser.error(
            f"authentication token environment variable {args.auth_token_env!r} is empty"
        )

    client_python = args.client_python.absolute()
    if not client_python.is_file():
        raise FileNotFoundError(f"pinned client interpreter not found: {client_python}")
    client_version = public_client_version(client_python)
    host, port, base_url = require_sc_url(args.proxy_url)
    args.output.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now(UTC).strftime("run-%Y%m%dT%H%M%SZ") + f"-{os.getpid()}"
    output_dir = args.output / run_id
    output_dir.mkdir()

    environment = os.environ.copy()
    for key in (
        "SPARK_HOME",
        "SPARK_CONF_DIR",
        "PYSPARK_PYTHON",
        "PYSPARK_DRIVER_PYTHON",
        "PYSPARK_SUBMIT_ARGS",
        "PYTHONPATH",
        "SPARK_REMOTE",
    ):
        environment.pop(key, None)
    environment.update(
        {
            "SPARKLESS_PROXY_URL": base_url,
            "SPARKLESS_PROXY_TOKEN": token,
            "SPARKLESS_CONNECT_URL": url_with_token(base_url, token),
            "TZ": "UTC",
        }
    )

    # Check reachability before the authenticated client run so a network error cannot
    # be mistaken for a successful auth rejection.
    try:
        with socket.create_connection((host, port), timeout=5):
            pass
    except OSError as error:
        raise RuntimeError(
            f"pinned proxy endpoint {host}:{port} is unavailable"
        ) from error

    started = time.perf_counter()
    invalid_auth = run_client(
        client_python, PROXY_CLIENT, "reject-invalid-token", environment
    )
    authenticated = run_client(client_python, PROXY_CLIENT, "workload", environment)
    enriched_errors = subprocess.run(
        [str(client_python), str(ERROR_CLIENT)],
        cwd=ROOT,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
        timeout=360,
    )

    proxy_host = host
    tls_upstream = host != "localhost"
    retry_proxy, retry_log_path, retry_port = start_fault_proxy(
        client_python,
        output_dir,
        f"{proxy_host}:{port}",
        tls_upstream,
        host == "localhost",
    )
    retry_proxy_shutdown: dict[str, object] | None = None
    try:
        retry_url = url_with_token(f"sc://localhost:{retry_port}", token)
        retry_environment = environment.copy()
        retry_environment["SPARKLESS_CONNECT_URL"] = retry_url
        retry_started = time.perf_counter()
        retry = subprocess.run(
            [str(client_python), str(RETRY_CLIENT)],
            cwd=ROOT,
            env=retry_environment,
            check=True,
            capture_output=True,
            text=True,
            timeout=300,
        )
        retry_elapsed = time.perf_counter() - retry_started
        proxy_log_text = retry_log_path.read_text(encoding="utf-8")
        if "injected UNAVAILABLE" not in proxy_log_text:
            raise AssertionError(
                "the fault proxy did not inject the expected mid-stream disconnect"
            )
        if "response reattachment passed" not in retry.stdout:
            raise AssertionError(
                "the stock public client did not finish after the injected disconnect"
            )
    finally:
        retry_proxy_shutdown = stop_fault_proxy(retry_proxy)
    if not retry_proxy_shutdown["graceful"]:
        raise RuntimeError(
            "local authenticated fault proxy did not shut down gracefully"
        )

    report = {
        "schema_version": 1,
        "slice": "418-08-v25-proxy-route",
        "run_id": run_id,
        "proxy": {
            "revision": args.proxy_revision.lower(),
            "endpoint": f"{host}:{port}",
            "authentication": "Spark Connect bearer token via the pinned client's token URI parameter",
            "auth_token_value_recorded": False,
        },
        "client": {"distribution": "pyspark-client", "version": client_version},
        "checks": {
            "invalid_bearer_rejected": invalid_auth["result"],
            "authenticated_values_sessions_and_affinity": authenticated["result"],
            "enriched_error_and_status_forwarding": enriched_errors.stdout.strip().splitlines(),
            "stock_client_midstream_reconnect": {
                "passed": True,
                "elapsed_seconds": retry_elapsed,
                "fault_proxy_log": str(retry_log_path),
                "output": retry.stdout.strip(),
                "local_fault_proxy_shutdown": retry_proxy_shutdown,
            },
        },
        "not_asserted": [
            "cluster target selection requires a proxy-specific backend routing witness",
            "external proxy shutdown is owned by the proxy deployment and is not exercised by this client harness",
        ],
        "elapsed_seconds": time.perf_counter() - started,
        "artifacts": {
            "proxy_client_stdout": authenticated["stdout"],
            "report_revision_source": "--proxy-revision supplied by caller; verify against proxy checkout",
        },
    }
    report_path = output_dir / "proxy-route.json"
    report_path.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(
        json.dumps(
            {"report": str(report_path), "checks": report["checks"]},
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
