"""Verify graceful shutdown cancels an active DuckDB Connect backend query."""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
import hashlib
import importlib.metadata
import json
from pathlib import Path
import platform
from queue import Empty, Queue
import selectors
import socket
import subprocess
import tempfile
import sys
from threading import Thread
import time
from typing import Any

from pyspark.sql import SparkSession
from sparkless_server_duckdb import SparklessServer


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def read_owner_line(owner: subprocess.Popen[str], timeout: float) -> str:
    if owner.stdout is None:
        raise RuntimeError("Quack fixture owner stdout is unavailable")
    with selectors.DefaultSelector() as selector:
        selector.register(owner.stdout, selectors.EVENT_READ)
        if not selector.select(timeout=timeout):
            raise TimeoutError("Quack fixture owner did not answer its control request")
        line = owner.stdout.readline()
    if not line:
        raise RuntimeError(f"Quack owner exited with status {owner.poll()}")
    return line.strip()


def owner_command(owner: subprocess.Popen[str], command: str) -> dict[str, Any]:
    if owner.stdin is None:
        raise RuntimeError("Quack fixture owner stdin is unavailable")
    owner.stdin.write(f"{command}\n")
    owner.stdin.flush()
    return json.loads(read_owner_line(owner, timeout=3))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server-bin", type=Path, required=True)
    parser.add_argument("--quack-owner-bin", type=Path, required=True)
    parser.add_argument("--quack-extension-path", type=Path, required=True)
    parser.add_argument(
        "--report",
        type=Path,
        default=Path(
            "docs/plans/sparkless-server/evidence/phase-420-duckdb-backend-shutdown-local.json"
        ),
    )
    args = parser.parse_args()
    for label, path in (
        ("DuckDB server", args.server_bin),
        ("Quack fixture owner", args.quack_owner_bin),
        ("Quack extension", args.quack_extension_path),
    ):
        if not path.is_file():
            parser.error(f"{label} does not exist: {path}")
    if importlib.metadata.version("pyspark-client") != "4.0.5":
        parser.error("the unmodified Connect client must be pyspark-client==4.0.5")

    args.server_bin = args.server_bin.resolve()
    args.quack_owner_bin = args.quack_owner_bin.resolve()
    args.quack_extension_path = args.quack_extension_path.resolve()
    server: SparklessServer | None = None
    owner: subprocess.Popen[str] | None = None
    session: Any = None
    worker: Thread | None = None
    result_queue: Queue[tuple[str, int | None]] = Queue(maxsize=1)
    server_exit_code: int | None = None
    owner_exit_code: int | None = None
    try:
        with tempfile.TemporaryDirectory(
            prefix="sparkless-phase420-backend-stop-"
        ) as directory:
            owner_database = Path(directory) / "quack-owner.duckdb"
            # Let the fixture select an ephemeral port, then start it in persistent mode.
            with socket.socket() as listener:
                listener.bind(("127.0.0.1", 0))
                owner_port = listener.getsockname()[1]
            owner = subprocess.Popen(
                [
                    str(args.quack_owner_bin),
                    str(args.quack_extension_path),
                    "--persistent",
                    str(owner_database),
                    str(owner_port),
                ],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=None,
                text=True,
                bufsize=1,
            )
            owner_info = json.loads(read_owner_line(owner, timeout=30))
            owner_config = json.dumps(
                {
                    "mode": "quack",
                    "quack_uri": owner_info["uri"],
                    "token": owner_info["token"],
                    "extension_path": str(args.quack_extension_path),
                    "disable_ssl": True,
                }
            )
            server = SparklessServer(
                executable_path=str(args.server_bin),
                startup_timeout=90,
                connect_port=0,
                env={
                    "SPARKLESS_EXECUTION_BACKEND": "duckdb",
                    "SPARKLESS_DUCKDB_EXECUTION_OWNER": owner_config,
                    "SPARKLESS_CONNECT_TRACE": "1",
                    "SPARKLESS_MAX_ACTIVE_OPERATIONS": "2",
                    "SPARKLESS_MAX_PENDING_OPERATIONS": "16",
                    "SPARKLESS_QUEUE_TIMEOUT_MS": "5000",
                    "SPARKLESS_RESPONSE_TIMEOUT_MS": "60000",
                    "POLARS_MAX_THREADS": "4",
                    "RAYON_NUM_THREADS": "4",
                    "OMP_NUM_THREADS": "4",
                    "TZ": "UTC",
                },
            )
            server.start()
            session = SparkSession.builder.remote(server.spark_connect_url).create()
            # This scan is intentionally stopped; do not reconnect or replay it.
            session.client._use_reattachable_execute = False
            session.client._retry_policies[0].max_retries = 0
            scan = session.table("shutdown_events")
            if scan.schema.fieldNames() != ["id"]:
                raise AssertionError(f"unexpected scan schema: {scan.schema}")

            def consume_scan() -> tuple[str, int | None]:
                consumed = 0
                try:
                    for _ in scan.toLocalIterator():
                        consumed += 1
                except BaseException as error:
                    return f"{type(error).__name__}: {error}", None
                return "completed", consumed

            def consume_and_record() -> None:
                result_queue.put(consume_scan())

            worker = Thread(
                target=consume_and_record,
                name="phase420-duckdb-shutdown-scan",
                daemon=True,
            )
            worker.start()
            active_states = {"running", "active", "executing"}
            deadline = time.monotonic() + 20
            active_owner_states: set[str] = set()
            while time.monotonic() < deadline:
                states_response = owner_command(owner, "states")
                states = set(
                    value.strip()
                    for value in (states_response.get("states") or "").split(",")
                    if value.strip()
                )
                active_owner_states = states & active_states
                if active_owner_states:
                    break
                if not worker.is_alive():
                    try:
                        result = result_queue.get_nowait()
                    except Empty:
                        result = ("worker ended without a result", None)
                    raise AssertionError(
                        f"the billion-row query ended before shutdown: {result!r}"
                    )
                time.sleep(0.05)
            else:
                raise TimeoutError(
                    "Quack owner never reported the Connect query active"
                )

            shutdown_started = time.monotonic()
            server_process = server._process
            server.stop()
            shutdown_seconds = time.monotonic() - shutdown_started
            server_exit_code = None if server_process is None else server_process.poll()
            if server_exit_code != 0:
                raise RuntimeError(
                    f"server did not exit cleanly after shutdown: {server_exit_code}"
                )
            diagnostics = server.diagnostics
            if (
                "event=duckdb_source_cancel_requested" not in diagnostics
                or "event=duckdb_source_remote_cancel accepted=true" not in diagnostics
            ):
                raise AssertionError(
                    "DuckDB backend shutdown did not confirm remote Quack cancellation; "
                    f"owner_states={sorted(active_owner_states)}, "
                    f"diagnostics={diagnostics!r}"
                )

            worker.join(timeout=10)
            if worker.is_alive():
                session.client.close()
                session = None
                worker.join(timeout=5)
            if worker.is_alive():
                raise TimeoutError(
                    "Connect scan remained blocked after server shutdown"
                )
            try:
                client_result = result_queue.get_nowait()
            except Empty as error:
                raise AssertionError(
                    "Connect scan did not report its shutdown result"
                ) from error
            if client_result == ("completed", 1_000_000_000):
                raise AssertionError("the billion-row query completed during shutdown")
            if client_result[1] is not None:
                raise AssertionError(
                    f"the query unexpectedly returned {client_result[1]} rows"
                )
            final_states = owner_command(owner, "states").get("states") or ""
            remaining_active = {
                value.strip() for value in final_states.split(",") if value.strip()
            } & active_states
            if remaining_active:
                raise AssertionError(
                    f"Quack still reports active queries after shutdown: {sorted(remaining_active)}"
                )

            if owner.stdin is not None:
                owner.stdin.write("quit\n")
                owner.stdin.flush()
                owner.stdin.close()
            owner.wait(timeout=8)
            owner_exit_code = owner.returncode
            if owner_exit_code != 0:
                raise RuntimeError(
                    f"Quack owner did not exit cleanly: {owner_exit_code}"
                )
            owner = None

            report = {
                "schema_version": 1,
                "result": "passed",
                "timestamp_utc": datetime.now(UTC).isoformat(),
                "source_revision": subprocess.check_output(
                    ["git", "rev-parse", "HEAD"], text=True
                ).strip(),
                "platform": platform.platform(),
                "python": sys.version.split()[0],
                "pyspark_client": importlib.metadata.version("pyspark-client"),
                "backend": "duckdb",
                "server_version": subprocess.check_output(
                    [str(args.server_bin), "--version"], text=True
                ).strip(),
                "server_sha256": sha256(args.server_bin),
                "server_exit_code": server_exit_code,
                "quack_owner_sha256": sha256(args.quack_owner_bin),
                "quack_owner_exit_code": owner_exit_code,
                "quack_extension_sha256": sha256(args.quack_extension_path),
                "active_owner_states_before_shutdown": sorted(active_owner_states),
                "remote_cancel_accepted": True,
                "remaining_active_owner_states": sorted(remaining_active),
                "shutdown_seconds": shutdown_seconds,
                "client_result": client_result[0],
                "server_diagnostics": diagnostics.splitlines(),
            }
            args.report.parent.mkdir(parents=True, exist_ok=True)
            rendered = json.dumps(report, indent=2) + "\n"
            args.report.write_text(rendered, encoding="utf-8")
            print(rendered, end="")
    finally:
        if server is not None:
            server.stop()
        if session is not None:
            try:
                session.client.close()
            except Exception:
                pass
        if owner is not None:
            if owner.stdin is not None:
                try:
                    owner.stdin.write("quit\n")
                    owner.stdin.flush()
                    owner.stdin.close()
                except OSError:
                    pass
            try:
                owner.wait(timeout=8)
            except subprocess.TimeoutExpired:
                owner.kill()
                owner.wait(timeout=5)


if __name__ == "__main__":
    main()
