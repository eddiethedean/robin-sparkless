"""Verify lost-response writes survive reattachment and a server process restart.

The local database has one writable process at a time. The standalone DuckDB client creates the
fixture before Sparkless starts, verifies the commit after a forced server exit, and checks that a
retry on a new server process replays the exact durable operation receipt.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import socket
import subprocess
import sys
import tempfile
import time
from urllib.parse import urlparse
import uuid

import duckdb
import grpc
from pyspark.sql.connect.proto import base_pb2, base_pb2_grpc
from sparkless_server_duckdb import SparklessServer


def free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def operation_receipt_identity(user_id: str, session_id: str, operation_id: str) -> str:
    def framed_digest(parts: list[str]) -> str:
        digest = hashlib.sha256()
        for part in parts:
            encoded = part.encode("utf-8")
            digest.update(len(encoded).to_bytes(8, "big"))
            digest.update(encoded)
        return digest.hexdigest()

    session_key = "connect:" + framed_digest(["connect", user_id, session_id])
    return "connect-operation:" + framed_digest([session_key, operation_id])


def start_fault_proxy(
    proxy_path: Path, proxy_address: str, upstream: str, output_path: Path
) -> subprocess.Popen:
    with output_path.open("w", encoding="utf-8") as log:
        return subprocess.Popen(
            [
                sys.executable,
                str(proxy_path),
                "--listen",
                proxy_address,
                "--upstream",
                upstream,
            ],
            stdout=log,
            stderr=subprocess.STDOUT,
        )


def stop_process(process: subprocess.Popen | None) -> None:
    if process is None or process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def execute_and_check_completion(stub, request) -> list:
    responses = list(stub.ExecutePlan(request, timeout=30))
    assert any(response.HasField("result_complete") for response in responses), (
        "the operation response should contain result_complete"
    )
    return responses


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--server-bin", type=Path, required=True)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    if not args.server_bin.is_file():
        parser.error(f"server binary does not exist: {args.server_bin}")

    fault_proxy_path = Path(__file__).with_name("fault_proxy.py").resolve()
    with tempfile.TemporaryDirectory(
        prefix="sparkless-phase420-write-reattach-"
    ) as temp_dir:
        database_path = Path(temp_dir) / "shared.duckdb"
        with duckdb.connect(str(database_path)) as connection:
            connection.execute("CREATE TABLE replay_items (id BIGINT)")

        server = SparklessServer(
            executable_path=str(args.server_bin.resolve()),
            startup_timeout=90.0,
            connect_port=0,
            env={
                "SPARKLESS_EXECUTION_BACKEND": "duckdb",
                "SPARKLESS_DUCKDB_EXECUTION_OWNER": json.dumps(
                    {"mode": "local", "database_path": str(database_path)}
                ),
            },
        )
        proxy_outputs = [
            Path(temp_dir) / "fault-proxy-live-reattach.log",
            Path(temp_dir) / "fault-proxy-process-restart.log",
        ]
        proxies: list[subprocess.Popen] = []
        channels: list[grpc.Channel] = []
        report: dict[str, object] = {}
        try:
            server.start()
            first_instance_id = server.server_instance_id
            proxy_port = free_port()
            proxy_address = f"127.0.0.1:{proxy_port}"
            proxy = start_fault_proxy(
                fault_proxy_path,
                proxy_address,
                urlparse(server.spark_connect_url).netloc,
                proxy_outputs[0],
            )
            proxies.append(proxy)
            channel = grpc.insecure_channel(proxy_address)
            channels.append(channel)
            grpc.channel_ready_future(channel).result(timeout=15)
            stub = base_pb2_grpc.SparkConnectServiceStub(channel)

            session_id = str(uuid.uuid4())
            operation_id = str(uuid.uuid4())
            user_id = "phase420-write-reattach"
            user_context = base_pb2.UserContext(user_id=user_id)
            request = base_pb2.ExecutePlanRequest(
                session_id=session_id,
                operation_id=operation_id,
                user_context=user_context,
            )
            request.plan.command.sql_command.input.sql.query = (
                "INSERT INTO replay_items VALUES (42)"
            )
            request.request_options.add().reattach_options.reattachable = True

            first_responses = []
            try:
                first_responses.extend(stub.ExecutePlan(request, timeout=30))
            except grpc.RpcError as error:
                if error.code() is not grpc.StatusCode.UNAVAILABLE:
                    raise
            assert first_responses, "the proxy should drop after a real response"

            # Reattach while the first process is alive to retain the existing live-retry case.
            time.sleep(0.1)
            reattach = base_pb2.ReattachExecuteRequest(
                session_id=session_id,
                operation_id=operation_id,
                user_context=user_context,
            )
            replayed_responses = list(stub.ReattachExecute(reattach, timeout=30))
            assert replayed_responses, "the retained operation should be reattachable"
            assert any(
                response.HasField("result_complete") for response in replayed_responses
            ), "reattachment should include the operation completion"
            stub.ReleaseExecute(
                base_pb2.ReleaseExecuteRequest(
                    session_id=session_id,
                    operation_id=operation_id,
                    user_context=user_context,
                    release_all=base_pb2.ReleaseExecuteRequest.ReleaseAll(),
                ),
                timeout=15,
            )

            stop_process(proxy)
            server.restart()
            assert server.server_instance_id != first_instance_id

            # A second lost response is followed by a hard process kill. Check the receipt and
            # row before retrying so the next server must replay, not execute the insert anew.
            session_id = str(uuid.uuid4())
            operation_id = str(uuid.uuid4())
            crash_request = base_pb2.ExecutePlanRequest(
                session_id=session_id,
                operation_id=operation_id,
                user_context=user_context,
            )
            crash_request.plan.command.sql_command.input.sql.query = (
                "INSERT INTO replay_items VALUES (43)"
            )
            crash_request.request_options.add().reattach_options.reattachable = True
            receipt_key = operation_receipt_identity(user_id, session_id, operation_id)
            request_fingerprint = hashlib.sha256(
                crash_request.SerializeToString()
            ).hexdigest()
            second_instance_id = server.server_instance_id
            proxy_port = free_port()
            proxy_address = f"127.0.0.1:{proxy_port}"
            proxy = start_fault_proxy(
                fault_proxy_path,
                proxy_address,
                urlparse(server.spark_connect_url).netloc,
                proxy_outputs[1],
            )
            proxies.append(proxy)
            channel = grpc.insecure_channel(proxy_address)
            channels.append(channel)
            grpc.channel_ready_future(channel).result(timeout=15)
            stub = base_pb2_grpc.SparkConnectServiceStub(channel)
            crash_responses = []
            try:
                crash_responses.extend(stub.ExecutePlan(crash_request, timeout=30))
            except grpc.RpcError as error:
                if error.code() is not grpc.StatusCode.UNAVAILABLE:
                    raise
            assert crash_responses, "the proxy should drop after a real response"
            time.sleep(0.1)

            for channel in channels:
                channel.close()
            channels.clear()
            stop_process(proxy)
            server._stop_owned(force=True)
            assert not server.is_running, "the old server process should be killed"

            with duckdb.connect(str(database_path)) as connection:
                rows_before_retry = connection.execute(
                    "SELECT id FROM replay_items ORDER BY id"
                ).fetchall()
                receipt_rows = connection.execute(
                    'SELECT request_fingerprint FROM "__sparkless_meta"."connect_operation_receipts" '
                    "WHERE operation_key = ?",
                    [receipt_key],
                ).fetchall()
            assert rows_before_retry == [(42,), (43,)], (
                "both writes should be committed before restart, "
                f"found {rows_before_retry!r}"
            )
            assert receipt_rows == [(request_fingerprint,)], (
                "the exact durable receipt should exist before retry, "
                f"found {receipt_rows!r}"
            )

            server.start()
            third_instance_id = server.server_instance_id
            assert third_instance_id != second_instance_id
            retry_channel = grpc.insecure_channel(
                urlparse(server.spark_connect_url).netloc
            )
            channels.append(retry_channel)
            grpc.channel_ready_future(retry_channel).result(timeout=15)
            retry_stub = base_pb2_grpc.SparkConnectServiceStub(retry_channel)
            retry_responses = execute_and_check_completion(retry_stub, crash_request)
            retry_stub.ReleaseExecute(
                base_pb2.ReleaseExecuteRequest(
                    session_id=session_id,
                    operation_id=operation_id,
                    user_context=user_context,
                    release_all=base_pb2.ReleaseExecuteRequest.ReleaseAll(),
                ),
                timeout=15,
            )
            report = {
                "server_instance_ids": [
                    first_instance_id,
                    second_instance_id,
                    third_instance_id,
                ],
                "live_reattachment": {
                    "result_complete_received": True,
                    "single_application_checked": True,
                },
                "process_restart_replay": {
                    "fault": "UNAVAILABLE after the first ExecutePlan response",
                    "server_killed": True,
                    "receipt_key": receipt_key,
                    "request_fingerprint": request_fingerprint,
                    "rows_before_retry": [list(row) for row in rows_before_retry],
                    "durable_receipt_before_retry": True,
                    "retry_result_complete_received": any(
                        response.HasField("result_complete")
                        for response in retry_responses
                    ),
                    "identity_replayed_on_new_instance": True,
                },
            }
        finally:
            for channel in channels:
                channel.close()
            for proxy in proxies:
                stop_process(proxy)
            server.stop()

        for proxy_output in proxy_outputs:
            proxy_log = proxy_output.read_text(encoding="utf-8")
            assert "injected UNAVAILABLE after first ExecutePlan response" in proxy_log
        with duckdb.connect(str(database_path)) as connection:
            rows = connection.execute(
                "SELECT id FROM replay_items ORDER BY id"
            ).fetchall()
        assert rows == [(42,), (43,)], (
            f"replay must not duplicate either write: {rows!r}"
        )

    if args.report is not None:
        report_payload = {
            "format_version": 1,
            "phase": "4.20",
            "gate": "V38 partial: lost-response reattachment and durable replay after process restart with a local owner",
            "result": "passed",
            "run_date_utc": datetime.now(timezone.utc).date().isoformat(),
            "source_revision": subprocess.run(
                ["git", "rev-parse", "HEAD"],
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip(),
            "working_tree_modified": True,
            "platform": platform.platform(),
            "runtime": {
                "python": platform.python_version(),
                "pyspark_client": importlib.metadata.version("pyspark-client"),
                "duckdb": duckdb.__version__,
                "grpcio": grpc.__version__,
            },
            "server": {
                "path": str(args.server_bin),
                "sha256": sha256_file(args.server_bin),
            },
            "runner": {
                "path": str(Path(__file__).relative_to(Path.cwd())),
                "sha256": sha256_file(Path(__file__)),
            },
            "workload": report,
            "command": "PYTHONPATH=python/server-duckdb:python/connect /tmp/sparkless-phase420-client-py311/bin/python tests/connect/smoke_phase420_duckdb_write_reattach.py --server-bin target/debug/sparkless-server-duckdb --report docs/plans/sparkless-server/evidence/phase-420-duckdb-write-reattach-local.json",
            "limitations": [
                "The local owner is exercised; shared Quack crash recovery is not exercised.",
                "The process crash is injected only after the committed row and receipt are verified; this smoke does not induce a DuckDB COMMIT failure.",
                "A separate owner-level unit test simulates a lost acknowledgement after successful COMMIT and verifies unknown-outcome replay.",
                "This smoke does not close the complete V38 lifecycle matrix.",
            ],
        }
        args.report.write_text(
            json.dumps(report_payload, indent=2) + os.linesep, encoding="utf-8"
        )

    print(
        "Phase 4.20 DuckDB write reattachment and process-restart replay smoke passed"
    )


def sha256_file(path: Path) -> str:
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


if __name__ == "__main__":
    main()
