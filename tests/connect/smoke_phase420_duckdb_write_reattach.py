"""Verify a committed DuckDB Connect write survives a dropped response stream.

The local database has one writable process at a time. The standalone DuckDB client creates the
fixture before Sparkless starts, and checks the committed result after Sparkless exits.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
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


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--server-bin", type=Path, required=True)
    args = parser.parse_args()
    if not args.server_bin.is_file():
        parser.error(f"server binary does not exist: {args.server_bin}")

    fault_proxy_path = Path(__file__).with_name("fault_proxy.py").resolve()
    with tempfile.TemporaryDirectory(prefix="sparkless-phase420-write-reattach-") as temp_dir:
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
        proxy_port = free_port()
        proxy_address = f"127.0.0.1:{proxy_port}"
        proxy_output = Path(temp_dir) / "fault-proxy.log"
        proxy = None
        channel = None
        try:
            server.start()
            upstream = urlparse(server.spark_connect_url).netloc
            with proxy_output.open("w", encoding="utf-8") as log:
                proxy = subprocess.Popen(
                    [
                        sys.executable,
                        str(fault_proxy_path),
                        "--listen",
                        proxy_address,
                        "--upstream",
                        upstream,
                    ],
                    stdout=log,
                    stderr=subprocess.STDOUT,
                )
                channel = grpc.insecure_channel(proxy_address)
                grpc.channel_ready_future(channel).result(timeout=15)
                stub = base_pb2_grpc.SparkConnectServiceStub(channel)

                session_id = str(uuid.uuid4())
                operation_id = str(uuid.uuid4())
                user_context = base_pb2.UserContext(user_id="phase420-write-reattach")
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

                # Give the server transport time to observe cancellation before reattaching.
                time.sleep(0.1)
                reattach = base_pb2.ReattachExecuteRequest(
                    session_id=session_id,
                    operation_id=operation_id,
                    user_context=user_context,
                )
                replayed_responses = list(stub.ReattachExecute(reattach, timeout=30))
                assert replayed_responses, "the retained operation should be reattachable"
                assert any(
                    response.HasField("result_complete")
                    for response in replayed_responses
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
        finally:
            if channel is not None:
                channel.close()
            if proxy is not None:
                proxy.terminate()
                try:
                    proxy.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proxy.kill()
                    proxy.wait(timeout=5)
            server.stop()

        proxy_log = proxy_output.read_text(encoding="utf-8")
        assert "injected UNAVAILABLE after first ExecutePlan response" in proxy_log
        with duckdb.connect(str(database_path)) as connection:
            rows = connection.execute(
                "SELECT id FROM replay_items ORDER BY id"
            ).fetchall()
        assert rows == [(42,)], f"reattachment must not duplicate the write: {rows!r}"

    print("Phase 4.20 DuckDB committed-write reattachment smoke passed")


if __name__ == "__main__":
    main()
