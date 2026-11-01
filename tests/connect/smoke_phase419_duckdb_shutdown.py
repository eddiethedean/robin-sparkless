"""Verify graceful Spark Connect shutdown cancels an active DuckDB Quack scan."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from queue import Empty, Queue
import selectors
import subprocess
from threading import Thread
import time
from typing import Any

from pyspark.sql.connect.session import SparkSession

from sparkless.server import SparklessServer


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


def owner_states(owner: subprocess.Popen[str]) -> set[str]:
    if owner.stdin is None:
        raise RuntimeError("Quack fixture owner stdin is unavailable")
    owner.stdin.write("states\n")
    owner.stdin.flush()
    response: dict[str, Any] = json.loads(read_owner_line(owner, timeout=2))
    states = response.get("states")
    return set() if states is None else {value.strip() for value in states.split(",")}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server-bin", type=Path, required=True)
    parser.add_argument("--quack-owner-bin", type=Path, required=True)
    parser.add_argument("--quack-extension-path", type=Path, required=True)
    args = parser.parse_args()
    for path in (args.server_bin, args.quack_owner_bin, args.quack_extension_path):
        if not path.is_file():
            parser.error(f"required file does not exist: {path}")

    owner = subprocess.Popen(
        [
            str(args.quack_owner_bin.absolute()),
            str(args.quack_extension_path.absolute()),
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=None,
        text=True,
        bufsize=1,
    )
    server: SparklessServer | None = None
    session: SparkSession | None = None
    worker: Thread | None = None
    result_queue: Queue[tuple[str, int | None]] = Queue(maxsize=1)
    try:
        owner_info = json.loads(read_owner_line(owner, timeout=30))
        source_config = [
            {
                "alias": "warehouse",
                "quack_uri": owner_info["uri"],
                "token": owner_info["token"],
                "extension_path": str(args.quack_extension_path.absolute()),
                "disable_ssl": True,
            }
        ]
        server_env = {
            "SPARKLESS_DUCKDB_SOURCES": json.dumps(source_config),
            "SPARKLESS_MAX_ACTIVE_OPERATIONS": "2",
            "SPARKLESS_MAX_PENDING_OPERATIONS": "16",
            "SPARKLESS_QUEUE_TIMEOUT_MS": "5000",
            "SPARKLESS_RESPONSE_TIMEOUT_MS": "60000",
            "SPARKLESS_CONNECT_TRACE": "1",
            "SPARKLESS_ARROW_TRACE": "0",
            "POLARS_MAX_THREADS": "4",
            "RAYON_NUM_THREADS": "4",
            "OMP_NUM_THREADS": "4",
            "TZ": "UTC",
        }
        server = SparklessServer(
            executable_path=str(args.server_bin.absolute()), env=server_env
        ).start()
        session = SparkSession.builder.remote(server.spark_connect_url).create()
        # Avoid reconnecting to a server that this lifecycle check intentionally stops.
        session.client._use_reattachable_execute = False
        session.client._retry_policies[0].max_retries = 0
        source_table = session.table("warehouse.main.shutdown_events")
        if source_table.schema.fieldNames() != ["id"]:
            raise AssertionError(f"unexpected schema: {source_table.schema}")

        baseline_states = owner_states(owner)

        def collect_until_shutdown() -> tuple[str, int | None]:
            try:
                return "completed", len(source_table.collect())
            except Exception as error:
                return str(error), None

        def collect_and_record() -> None:
            result_queue.put(collect_until_shutdown())

        worker = Thread(
            target=collect_and_record, name="quack-shutdown-client", daemon=True
        )
        worker.start()
        active_states = {"running", "active", "executing"}
        deadline = time.monotonic() + 15
        observed_active: set[str] = set()
        while time.monotonic() < deadline:
            states = owner_states(owner)
            observed_active = states - baseline_states
            if states & active_states or observed_active & active_states:
                break
            if not worker.is_alive():
                result = result_queue.get_nowait()
                raise AssertionError(
                    f"the long source read ended before shutdown: {result!r}; owner states={states}"
                )
            time.sleep(0.05)
        else:
            raise TimeoutError(
                f"Quack owner never reported the source scan active; states={observed_active}"
            )

        shutdown_started = time.monotonic()
        server.stop()
        shutdown_seconds = time.monotonic() - shutdown_started
        diagnostics = server.diagnostics
        if (
            "event=duckdb_source_cancel_requested" not in diagnostics
            or "event=duckdb_source_remote_cancel accepted=true" not in diagnostics
        ):
            raise AssertionError(
                "server shutdown did not confirm remote Quack cancellation; "
                f"owner_before={observed_active}, diagnostics={diagnostics!r}"
            )

        worker.join(timeout=10)
        if worker.is_alive():
            # PySpark serializes stop() behind an active collect; close its channel first.
            session.client.close()
            session = None
            worker.join(timeout=5)
        if worker.is_alive():
            raise TimeoutError(
                "the Spark Connect action remained blocked after shutdown"
            )
        try:
            result = result_queue.get_nowait()
        except Empty as error:
            raise AssertionError(
                "the Spark Connect action did not report its shutdown result"
            ) from error
        if result == ("completed", 1_000_000_000):
            raise AssertionError("the billion-row Quack scan completed during shutdown")
        if result[1] is not None:
            raise AssertionError(
                f"the Quack scan unexpectedly returned {result[1]} rows"
            )

        print(
            json.dumps(
                {
                    "shutdown_cancelled_remote_query": True,
                    "shutdown_seconds": shutdown_seconds,
                    "owner_states_before_shutdown": sorted(observed_active),
                    "remote_cancel_accepted": True,
                    "server_diagnostics": diagnostics.splitlines(),
                    "client_result": result[0],
                },
                indent=2,
            )
        )
    finally:
        if server is not None:
            server.stop()
        if session is not None:
            try:
                session.client.close()
            except Exception:
                # The managed server has already stopped as part of this lifecycle check.
                pass
        if owner.stdin is not None:
            owner.stdin.write("quit\n")
            owner.stdin.flush()
            owner.stdin.close()
        try:
            owner.wait(timeout=8)
        except subprocess.TimeoutExpired:
            owner.kill()
            owner.wait(timeout=5)


if __name__ == "__main__":
    main()
