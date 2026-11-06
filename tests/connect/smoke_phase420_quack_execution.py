"""Probe DuckDB Connect execution against the shared Quack owner fixture."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import selectors
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse
import urllib.request
import uuid

import grpc
from pyspark.sql.connect.session import SparkSession
from pyspark.sql.connect.proto import base_pb2, base_pb2_grpc
from sparkless_server_duckdb import SparklessConnect


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def read_owner_line(owner: subprocess.Popen[str], timeout: float) -> str:
    if owner.stdout is None:
        raise RuntimeError("Quack fixture owner has no stdout pipe")
    with selectors.DefaultSelector() as selector:
        selector.register(owner.stdout, selectors.EVENT_READ)
        if not selector.select(timeout=timeout):
            raise TimeoutError("Quack fixture owner did not answer in time")
        line = owner.stdout.readline()
    if not line:
        raise RuntimeError(f"Quack fixture owner exited with {owner.poll()}")
    return line.strip()


def owner_command(owner: subprocess.Popen[str], command: str) -> str:
    if owner.stdin is None:
        raise RuntimeError("Quack fixture owner has no stdin pipe")
    owner.stdin.write(f"{command}\n")
    owner.stdin.flush()
    return read_owner_line(owner, timeout=30)


def start_persistent_owner(
    owner_bin: Path, extension: Path, database_path: Path, port: int
) -> tuple[subprocess.Popen[str], dict[str, str]]:
    owner = subprocess.Popen(
        [
            str(owner_bin),
            str(extension),
            "--persistent",
            str(database_path),
            str(port),
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=None,
        text=True,
        bufsize=1,
    )
    try:
        return owner, json.loads(read_owner_line(owner, timeout=30))
    except BaseException:
        owner.kill()
        owner.wait(timeout=5)
        raise


def external_quack_client(
    owner_bin: Path,
    extension: Path,
    mode: str,
    uri: str,
    token: str,
    *extra_args: str,
) -> dict[str, Any] | None:
    result = subprocess.run(
        [str(owner_bin), str(extension), mode, uri, token, *extra_args],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Quack fixture {mode} failed ({result.returncode}): {result.stderr.strip()}"
        )
    if not result.stdout.strip():
        return None
    return json.loads(result.stdout)


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


def assert_rejected(label: str, operation: Any, expected_message: str) -> str:
    try:
        operation()
    except Exception as error:
        if expected_message not in str(error).lower():
            raise AssertionError(
                f"{label} failed for an unrelated reason: {error}"
            ) from error
        return type(error).__name__
    raise AssertionError(f"{label} unexpectedly succeeded")


def free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def verify_write_reattachment(server: SparklessConnect) -> None:
    if server._server is None:
        raise RuntimeError("Connect server is not running")
    fault_proxy_path = Path(__file__).with_name("fault_proxy.py").resolve()
    upstream = urlparse(server._server.spark_connect_url).netloc
    with tempfile.TemporaryDirectory(
        prefix="sparkless-phase420-quack-reattach-"
    ) as directory:
        log_path = Path(directory) / "fault-proxy.log"
        port = free_port()
        with log_path.open("w", encoding="utf-8") as proxy_log:
            proxy = subprocess.Popen(
                [
                    sys.executable,
                    str(fault_proxy_path),
                    "--listen",
                    f"127.0.0.1:{port}",
                    "--upstream",
                    upstream,
                ],
                stdout=proxy_log,
                stderr=subprocess.STDOUT,
                text=True,
            )
            channel = grpc.insecure_channel(f"127.0.0.1:{port}")
            try:
                grpc.channel_ready_future(channel).result(timeout=15)
                stub = base_pb2_grpc.SparkConnectServiceStub(channel)
                session_id = str(uuid.uuid4())
                operation_id = str(uuid.uuid4())
                user_context = base_pb2.UserContext(user_id="phase420-quack-reattach")
                request = base_pb2.ExecutePlanRequest(
                    session_id=session_id,
                    operation_id=operation_id,
                    user_context=user_context,
                )
                request.plan.command.sql_command.input.sql.query = (
                    "INSERT INTO events VALUES (6, 60, 'reattach', 10.99, true)"
                )
                request.request_options.add().reattach_options.reattachable = True
                first_responses = []
                try:
                    first_responses.extend(stub.ExecutePlan(request, timeout=30))
                except grpc.RpcError as error:
                    if error.code() is not grpc.StatusCode.UNAVAILABLE:
                        raise
                if not first_responses:
                    raise AssertionError(
                        "fault proxy should drop after a real response"
                    )
                time.sleep(0.1)
                reattach = base_pb2.ReattachExecuteRequest(
                    session_id=session_id,
                    operation_id=operation_id,
                    user_context=user_context,
                )
                replayed = list(stub.ReattachExecute(reattach, timeout=30))
                if not replayed or not any(
                    response.HasField("result_complete") for response in replayed
                ):
                    raise AssertionError(
                        "reattachment did not return operation completion"
                    )
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
                channel.close()
                proxy.terminate()
                try:
                    proxy.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proxy.kill()
                    proxy.wait(timeout=5)
        if (
            "injected UNAVAILABLE after first ExecutePlan response"
            not in log_path.read_text(encoding="utf-8")
        ):
            raise AssertionError("fault proxy did not inject the expected disconnect")


def verify_quack_query_cancellation(spark: Any) -> dict[str, Any]:
    finished = threading.Event()
    failures: list[BaseException | None] = []

    def consume_long_scan() -> None:
        try:
            for _ in spark.table("shutdown_events").toLocalIterator():
                pass
        except BaseException as error:
            failures.append(error)
        else:
            failures.append(None)
        finally:
            finished.set()

    worker = threading.Thread(target=consume_long_scan, daemon=True)
    worker.start()
    deadline = time.monotonic() + 30
    interrupted_ids: list[str] = []
    while time.monotonic() < deadline and not finished.is_set():
        interrupted_ids = spark._client.interrupt_all()
        if interrupted_ids:
            break
        time.sleep(0.1)
    if not interrupted_ids:
        raise TimeoutError("Connect did not report the long Quack scan as active")
    if not finished.wait(timeout=30):
        raise TimeoutError("cancelled Quack scan did not stop within 30 seconds")
    error = failures[0]
    if error is None:
        raise AssertionError("long Quack scan completed instead of being cancelled")
    message = f"{error} {error!r}".upper()
    if "CANCELLED" not in message and "INTERRUPTED" not in message:
        raise AssertionError(
            f"Quack scan stopped for an unrelated reason: {error}"
        ) from error
    control_count = spark.range(3).count()
    if control_count != 3:
        raise AssertionError(
            f"DuckDB execution owner did not recover after cancellation: {control_count}"
        )
    return {
        "cancelled": True,
        "interrupted_operation_count": len(interrupted_ids),
        "error_type": type(error).__name__,
        "post_cancel_control_count": control_count,
    }


def verify_quack_owner_loss_during_stream(
    server: SparklessConnect,
    spark: Any,
    owner: subprocess.Popen[str],
    owner_bin: Path,
    extension: Path,
    database_path: Path,
    owner_port: int,
    owner_info: dict[str, str],
) -> tuple[subprocess.Popen[str], Any, dict[str, Any]]:
    if server._server is None:
        raise RuntimeError("Connect server is not running")
    reader_path = Path(__file__).with_name("slice8_raw_slow_reader.py").resolve()
    stream_row_count = 250_000
    with tempfile.TemporaryDirectory(
        prefix="sparkless-phase420-quack-owner-loss-"
    ) as directory:
        root = Path(directory)
        ready_file = root / "reader.ready"
        first_batch_file = root / "reader.first-batch.json"
        reader = subprocess.Popen(
            [
                sys.executable,
                str(reader_path),
                "--url",
                server._server.spark_connect_url,
                "--table",
                "shutdown_events",
                "--rows",
                str(stream_row_count),
                "--pause-ms",
                "100",
                "--timeout-seconds",
                "60",
                "--ready-file",
                str(ready_file),
                "--first-batch-file",
                str(first_batch_file),
            ],
            cwd=Path(__file__).resolve().parents[2],
            env=os.environ.copy(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        concurrent_client_count = 4
        client_barrier = threading.Barrier(concurrent_client_count + 1)
        client_outcomes: list[dict[str, Any] | None] = [
            None for _ in range(concurrent_client_count)
        ]

        def run_queued_client(index: int) -> None:
            client = None
            try:
                client = SparkSession.builder.remote(
                    server._server.spark_connect_url
                ).create()
                client_barrier.wait(timeout=30)
                count = client.table("events").count()
                client_outcomes[index] = {"result_count": int(count)}
            except BaseException as error:
                message = str(error).replace(owner_info["token"], "[redacted]")
                client_outcomes[index] = {
                    "error_type": type(error).__name__,
                    "error": message[-1000:],
                }
            finally:
                if client is not None:
                    try:
                        client.stop()
                    except BaseException:
                        pass

        clients = [
            threading.Thread(target=run_queued_client, args=(index,), daemon=True)
            for index in range(concurrent_client_count)
        ]
        deadline = time.monotonic() + 90
        try:
            while not first_batch_file.exists():
                if reader.poll() is not None:
                    stdout, stderr = reader.communicate(timeout=5)
                    raise RuntimeError(
                        "slow Quack reader exited before its first Arrow batch: "
                        f"{stderr[-2000:]} {stdout[-1000:]}"
                    )
                if time.monotonic() >= deadline:
                    raise TimeoutError(
                        "slow Quack reader did not receive its first Arrow batch"
                    )
                time.sleep(0.01)
            first_batch = json.loads(first_batch_file.read_text(encoding="utf-8"))
            for client_thread in clients:
                client_thread.start()
            client_barrier.wait(timeout=30)
            # Let all four independent Connect sessions submit their reads while the long
            # stream occupies the serialized owner, then fault the owner beneath that work.
            time.sleep(0.1)
            owner.kill()
            owner.wait(timeout=10)
            if owner.stdin is not None:
                owner.stdin.close()
            if owner.stdout is not None:
                owner.stdout.close()
            try:
                reader_stdout, reader_stderr = reader.communicate(timeout=75)
            except subprocess.TimeoutExpired:
                reader.kill()
                reader_stdout, reader_stderr = reader.communicate(timeout=5)
                raise TimeoutError(
                    "Quack-backed stream did not settle within 75 seconds of owner loss: "
                    f"{reader_stderr[-2000:]}"
                )
            failure = reader_stderr.replace(owner_info["token"], "[redacted]")
            for client_thread in clients:
                client_thread.join(timeout=90)
            if any(client_thread.is_alive() for client_thread in clients):
                raise TimeoutError(
                    "concurrent Connect reads did not settle after Quack owner loss"
                )
            if any(outcome is None for outcome in client_outcomes):
                raise AssertionError(
                    f"concurrent Connect reads returned no outcome: {client_outcomes!r}"
                )
            completed_counts = [
                outcome["result_count"]
                for outcome in client_outcomes
                if outcome is not None and "result_count" in outcome
            ]
            failed_client_outcomes = [
                outcome
                for outcome in client_outcomes
                if outcome is not None and "error" in outcome
            ]
            if any(count != 4 for count in completed_counts):
                raise AssertionError(
                    f"concurrent read returned incorrect data: {client_outcomes!r}"
                )
            for outcome in failed_client_outcomes:
                error_lower = outcome["error"].lower()
                if not any(
                    marker in error_lower
                    for marker in (
                        "quack",
                        "duckdb",
                        "connection",
                        "unavailable",
                        "internal",
                    )
                ):
                    raise AssertionError(
                        "concurrent owner-loss read failed for an unrelated reason: "
                        f"{outcome!r}"
                    )
            stream_rows = None
            stream_result = None
            if reader.returncode == 0:
                for line in reversed(reader_stdout.splitlines()):
                    try:
                        candidate = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(candidate, dict) and "rows" in candidate:
                        stream_result = candidate
                        break
                if (
                    stream_result is None
                    or stream_result.get("rows") != stream_row_count
                ):
                    raise AssertionError(
                        "stream completed after owner loss with an invalid result: "
                        f"{reader_stdout[-2000:]}"
                    )
                stream_rows = stream_result["rows"]
            else:
                failure_lower = failure.lower()
                if not any(
                    marker in failure_lower
                    for marker in (
                        "quack",
                        "duckdb",
                        "connection",
                        "unavailable",
                        "internal",
                        "http",
                    )
                ):
                    raise AssertionError(
                        "owner loss failed the stream for an unrelated reason: "
                        f"{failure[-2000:]}"
                    )

            with urllib.request.urlopen(
                server._server.http_url + "/health", timeout=10
            ) as response:
                health = json.load(response)
            if health.get("status") != "ready":
                raise AssertionError(
                    f"server health changed during Quack owner loss: {health!r}"
                )

            restarted_owner, restarted_info = start_persistent_owner(
                owner_bin, extension, database_path, owner_port
            )
            if restarted_info != owner_info:
                raise AssertionError(
                    "restarted Quack owner must retain its URI and token"
                )
            owner_rows = external_quack_client(
                owner_bin,
                extension,
                "--reader",
                owner_info["uri"],
                owner_info["token"],
            )
            expected_ids = [1, 2, 3, 4, 5, 6, 7]
            if owner_rows != {"event_ids": expected_ids}:
                raise AssertionError(
                    "restarted Quack owner did not restore committed data: "
                    f"{owner_rows!r}"
                )
            same_session_error = None
            same_session_retry_error = None
            same_session_retry_succeeded = False
            try:
                recovered_ids = sorted(
                    row.id for row in spark.table("events").select("id").collect()
                )
            except Exception as error:
                same_session_error = str(error).replace(
                    owner_info["token"], "[redacted]"
                )
                if "invalid connection id" not in same_session_error.lower():
                    raise
                try:
                    recovered_ids = sorted(
                        row.id for row in spark.table("events").select("id").collect()
                    )
                    same_session_retry_succeeded = True
                except Exception as retry_error:
                    same_session_retry_error = str(retry_error).replace(
                        owner_info["token"], "[redacted]"
                    )
                    if "invalid connection id" not in same_session_retry_error.lower():
                        raise
                    spark.stop()
                    spark = SparkSession.builder.remote(
                        server._server.spark_connect_url
                    ).create()
                    server._session = spark
                    recovered_ids = sorted(
                        row.id for row in spark.table("events").select("id").collect()
                    )
            if recovered_ids != expected_ids:
                raise AssertionError(
                    "Quack owner restart did not restore the committed data: "
                    f"{recovered_ids!r}"
                )
            return (
                restarted_owner,
                spark,
                {
                    "owner_killed_during_active_stream": True,
                    "first_batch": first_batch,
                    "stream_expected_rows": stream_row_count,
                    "stream_outcome": (
                        "completed_after_owner_loss"
                        if reader.returncode == 0
                        else "failed_during_owner_loss"
                    ),
                    "stream_rows": stream_rows,
                    "reader_exit_code": reader.returncode,
                    "reader_result": stream_result,
                    "reader_failure": failure[-2000:],
                    "concurrent_reads_during_owner_loss": {
                        "client_count": concurrent_client_count,
                        "completed_before_or_during_loss": len(completed_counts),
                        "failed_during_loss": len(failed_client_outcomes),
                        "outcomes": client_outcomes,
                    },
                    "health_while_owner_down": health,
                    "rows_from_independent_client_after_owner_recovery": owner_rows[
                        "event_ids"
                    ],
                    "same_session_recovery_error": same_session_error,
                    "same_session_retry_error": same_session_retry_error,
                    "same_session_retry_succeeded": same_session_retry_succeeded,
                    "fresh_connect_session_after_owner_recovery": same_session_error
                    is not None
                    and not same_session_retry_succeeded,
                    "owner_restarted_with_same_identity": True,
                    "rows_after_owner_recovery": recovered_ids,
                },
            )
        finally:
            if reader.poll() is None:
                reader.terminate()
                try:
                    reader.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    reader.kill()
                    reader.wait(timeout=5)


def verify_quack_replay_after_both_processes_restart(
    server: SparklessConnect,
    owner: subprocess.Popen[str],
    owner_bin: Path,
    extension: Path,
    database_path: Path,
    owner_port: int,
    owner_info: dict[str, str],
) -> tuple[subprocess.Popen[str], dict[str, Any]]:
    if server._server is None:
        raise RuntimeError("Connect server is not running")
    fault_proxy_path = Path(__file__).with_name("fault_proxy.py").resolve()
    session_id = str(uuid.uuid4())
    operation_id = str(uuid.uuid4())
    user_id = "phase420-quack-process-restart"
    user_context = base_pb2.UserContext(user_id=user_id)
    request = base_pb2.ExecutePlanRequest(
        session_id=session_id,
        operation_id=operation_id,
        user_context=user_context,
    )
    request.plan.command.sql_command.input.sql.query = (
        "INSERT INTO events VALUES (8, 80, 'after-both-process-restarts', 12.99, true)"
    )
    request.request_options.add().reattach_options.reattachable = True
    request_fingerprint = hashlib.sha256(request.SerializeToString()).hexdigest()
    receipt_key = operation_receipt_identity(user_id, session_id, operation_id)
    with tempfile.TemporaryDirectory(
        prefix="sparkless-phase420-quack-process-restart-"
    ) as directory:
        log_path = Path(directory) / "fault-proxy.log"
        port = free_port()
        upstream = urlparse(server._server.spark_connect_url).netloc
        with log_path.open("w", encoding="utf-8") as proxy_log:
            proxy = subprocess.Popen(
                [
                    sys.executable,
                    str(fault_proxy_path),
                    "--listen",
                    f"127.0.0.1:{port}",
                    "--upstream",
                    upstream,
                ],
                stdout=proxy_log,
                stderr=subprocess.STDOUT,
                text=True,
            )
            channel = grpc.insecure_channel(f"127.0.0.1:{port}")
            try:
                grpc.channel_ready_future(channel).result(timeout=15)
                stub = base_pb2_grpc.SparkConnectServiceStub(channel)
                first_responses = []
                try:
                    first_responses.extend(stub.ExecutePlan(request, timeout=30))
                except grpc.RpcError as error:
                    if error.code() is not grpc.StatusCode.UNAVAILABLE:
                        raise
                if not first_responses:
                    raise AssertionError(
                        "fault proxy should drop the response after a real Quack write"
                    )
            finally:
                channel.close()
                proxy.terminate()
                try:
                    proxy.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proxy.kill()
                    proxy.wait(timeout=5)
        if (
            "injected UNAVAILABLE after first ExecutePlan response"
            not in log_path.read_text(encoding="utf-8")
        ):
            raise AssertionError("fault proxy did not inject the expected disconnect")

    sparkless_server = server._server
    old_server_instance_id = sparkless_server.server_instance_id
    if server._session is not None:
        server._session.stop()
        server._session = None
    sparkless_server._stop_owned(force=True)
    if sparkless_server.is_running:
        raise AssertionError("Sparkless process should be killed before replay")

    old_owner_pid = owner.pid
    owner.kill()
    owner.wait(timeout=10)
    if owner.stdin is not None:
        owner.stdin.close()
    if owner.stdout is not None:
        owner.stdout.close()
    owner, restarted_owner_info = start_persistent_owner(
        owner_bin, extension, database_path, owner_port
    )
    if restarted_owner_info != owner_info:
        raise AssertionError("restarted Quack owner must retain its URI and token")

    rows_before_retry = external_quack_client(
        owner_bin, extension, "--reader", owner_info["uri"], owner_info["token"]
    )
    expected_ids = list(range(1, 9))
    if rows_before_retry != {"event_ids": expected_ids}:
        raise AssertionError(
            f"committed row missing before retry: {rows_before_retry!r}"
        )
    receipt_before_retry = external_quack_client(
        owner_bin,
        extension,
        "--receipt-reader",
        owner_info["uri"],
        owner_info["token"],
        receipt_key,
    )
    if receipt_before_retry != {"receipts": [[receipt_key, request_fingerprint]]}:
        raise AssertionError(
            f"durable operation receipt missing before retry: {receipt_before_retry!r}"
        )

    sparkless_server.start()
    new_server_instance_id = sparkless_server.server_instance_id
    if new_server_instance_id == old_server_instance_id:
        raise AssertionError("restarted Sparkless process reused its instance ID")
    retry_channel = grpc.insecure_channel(
        urlparse(sparkless_server.spark_connect_url).netloc
    )
    try:
        grpc.channel_ready_future(retry_channel).result(timeout=15)
        retry_stub = base_pb2_grpc.SparkConnectServiceStub(retry_channel)
        conflicting_request = base_pb2.ExecutePlanRequest()
        conflicting_request.CopyFrom(request)
        conflicting_request.plan.command.sql_command.input.sql.query = (
            "INSERT INTO events VALUES "
            "(9, 90, 'conflicting-replay-must-not-apply', 13.99, true)"
        )
        try:
            list(retry_stub.ExecutePlan(conflicting_request, timeout=30))
        except grpc.RpcError as error:
            if "different request content" not in error.details().lower():
                raise AssertionError(
                    "the durable receipt must reject changed content for an existing "
                    f"operation identity, got {error.code()}: {error.details()}"
                ) from error
        else:
            raise AssertionError(
                "the durable receipt accepted changed content for an existing operation identity"
            )
        rows_after_conflict = external_quack_client(
            owner_bin, extension, "--reader", owner_info["uri"], owner_info["token"]
        )
        receipt_after_conflict = external_quack_client(
            owner_bin,
            extension,
            "--receipt-reader",
            owner_info["uri"],
            owner_info["token"],
            receipt_key,
        )
        if rows_after_conflict != {"event_ids": expected_ids}:
            raise AssertionError(
                f"conflicting replay changed shared-owner data: {rows_after_conflict!r}"
            )
        if receipt_after_conflict != receipt_before_retry:
            raise AssertionError("conflicting replay changed the durable receipt")
    finally:
        retry_channel.close()

    # The conflicting request has an in-memory failure record under this operation ID. Restart
    # only Sparkless so the original request can prove it still replays from the durable Quack
    # receipt after the history registry is cleared.
    sparkless_server._stop_owned(force=True)
    sparkless_server.start()
    replay_server_instance_id = sparkless_server.server_instance_id
    if replay_server_instance_id in {old_server_instance_id, new_server_instance_id}:
        raise AssertionError("Sparkless restart reused a prior server instance ID")
    retry_channel = grpc.insecure_channel(
        urlparse(sparkless_server.spark_connect_url).netloc
    )
    try:
        grpc.channel_ready_future(retry_channel).result(timeout=15)
        retry_stub = base_pb2_grpc.SparkConnectServiceStub(retry_channel)
        retry_responses = list(retry_stub.ExecutePlan(request, timeout=30))
        if not any(
            response.HasField("result_complete") for response in retry_responses
        ):
            raise AssertionError(
                "replayed Quack request did not return operation completion"
            )
        retry_stub.ReleaseExecute(
            base_pb2.ReleaseExecuteRequest(
                session_id=session_id,
                operation_id=operation_id,
                user_context=user_context,
                release_all=base_pb2.ReleaseExecuteRequest.ReleaseAll(),
            ),
            timeout=15,
        )
    finally:
        retry_channel.close()

    rows_after_retry = external_quack_client(
        owner_bin, extension, "--reader", owner_info["uri"], owner_info["token"]
    )
    receipt_after_retry = external_quack_client(
        owner_bin,
        extension,
        "--receipt-reader",
        owner_info["uri"],
        owner_info["token"],
        receipt_key,
    )
    if rows_after_retry != {"event_ids": expected_ids}:
        raise AssertionError(f"retry duplicated or lost a row: {rows_after_retry!r}")
    if receipt_after_retry != receipt_before_retry:
        raise AssertionError("retry changed or duplicated the durable receipt")
    return owner, {
        "owner_process_ids": [old_owner_pid, owner.pid],
        "server_instance_ids": [
            old_server_instance_id,
            new_server_instance_id,
            replay_server_instance_id,
        ],
        "receipt_key": receipt_key,
        "request_fingerprint": request_fingerprint,
        "rows_before_retry": rows_before_retry["event_ids"],
        "durable_receipt_before_retry": True,
        "changed_request_under_same_identity_rejected": True,
        "rows_unchanged_after_conflict": rows_after_conflict["event_ids"],
        "receipt_unchanged_after_conflict": True,
        "retry_result_complete_received": True,
        "identity_replayed_on_new_instance": True,
        "rows_after_retry": rows_after_retry["event_ids"],
        "receipt_unchanged_after_retry": True,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--server-bin", type=Path, required=True)
    parser.add_argument("--quack-owner-bin", type=Path, required=True)
    parser.add_argument("--quack-extension-path", type=Path, required=True)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(
            "docs/plans/sparkless-server/evidence/phase-420-quack-execution-local.json"
        ),
    )
    args = parser.parse_args()
    for label, path in (
        ("Sparkless server", args.server_bin),
        ("Quack fixture owner", args.quack_owner_bin),
        ("Quack extension", args.quack_extension_path),
    ):
        if not path.is_file():
            parser.error(f"{label} does not exist: {path}")
    if importlib.metadata.version("pyspark-client") != "4.0.5":
        parser.error("the unmodified Connect client must be pyspark-client==4.0.5")

    owner_bin = args.quack_owner_bin.resolve()
    extension = args.quack_extension_path.resolve()
    owner_temp_dir = Path(tempfile.mkdtemp(prefix="sparkless-phase420-quack-owner-"))
    owner_database_path = owner_temp_dir / "quack-owner.duckdb"
    owner_port = free_port()
    owner, owner_info = start_persistent_owner(
        owner_bin, extension, owner_database_path, owner_port
    )
    try:
        uri = owner_info["uri"]
        token = owner_info["token"]
        inserted = external_quack_client(owner_bin, extension, "--writer", uri, token)
        if inserted is not None:
            raise AssertionError("fixture writer should not emit a result")

        owner_config = json.dumps(
            {
                "mode": "quack",
                "quack_uri": uri,
                "token": token,
                "extension_path": str(extension),
                "disable_ssl": True,
            }
        )
        server = SparklessConnect(
            server_options={
                "executable_path": str(args.server_bin.resolve()),
                "startup_timeout": 90.0,
                "port": 0,
                "connect_port": 0,
                "env": {
                    "SPARKLESS_EXECUTION_BACKEND": "duckdb",
                    "SPARKLESS_CONNECT_TRACE": "1",
                    "SPARKLESS_DUCKDB_EXECUTION_OWNER": owner_config,
                    "SPARKLESS_MAX_ACTIVE_OPERATIONS": "16",
                    "SPARKLESS_MAX_PENDING_OPERATIONS": "16",
                    "SPARKLESS_QUEUE_TIMEOUT_MS": "120000",
                    "SPARKLESS_RESPONSE_TIMEOUT_MS": "120000",
                },
            }
        )
        observations: dict[str, Any] = {}
        diagnostics = ""
        try:
            with server as spark:
                assert server._server is not None
                ready = server._server._ready_record
                assert isinstance(ready, dict)
                assert ready["execution_backend"] == "duckdb", ready
                try:
                    range_count = spark.range(3).count()
                except BaseException:
                    print(
                        "diagnostics_before_cleanup=\n" + server._server.diagnostics,
                        flush=True,
                    )
                    raise
                assert range_count == 3, range_count
                observations["range_execution_control"] = range_count
                observations["non_finite_double_rejected"] = assert_rejected(
                    "non-finite DOUBLE source",
                    lambda: spark.table("non_finite_events").collect(),
                    "non-finite",
                )
                observations["infinite_date_rejected"] = assert_rejected(
                    "infinite DATE source",
                    lambda: spark.table("infinite_date_events").collect(),
                    "infinite value",
                )
                observations["infinite_timestamp_rejected"] = assert_rejected(
                    "infinite TIMESTAMP source",
                    lambda: spark.table("infinite_timestamp_events").collect(),
                    "infinite value",
                )
                observations["nested_type_rejected"] = assert_rejected(
                    "nested LIST source",
                    lambda: spark.table("nested_events").collect(),
                    "outside the initial connect type set",
                )
                initial_ids = sorted(
                    row.id for row in spark.table("events").select("id").collect()
                )
                assert initial_ids == [1, 2, 3, 4], initial_ids
                observations["independent_writer_visible_to_connect"] = initial_ids

                analyzed_before_update = spark.table("catalog_change_events")
                analyzed_schema = analyzed_before_update.schema
                assert analyzed_schema.fieldNames() == ["id", "label"]
                external_quack_client(
                    owner_bin, extension, "--catalog-update", uri, token
                )
                rows_after_catalog_update = sorted(
                    (row.id, row.label) for row in analyzed_before_update.collect()
                )
                assert rows_after_catalog_update == [
                    (1, "before-analysis"),
                    (2, "after-analysis"),
                ], rows_after_catalog_update
                observations["catalog_change_between_analysis_and_execution"] = {
                    "analyzed_schema": analyzed_schema.jsonValue(),
                    "rows_after_update": [
                        list(row) for row in rows_after_catalog_update
                    ],
                }

                spark.sql(
                    "INSERT INTO events VALUES (5, 50, 'connect', 9.99, true)"
                ).collect()
                after_write = sorted(
                    row.id for row in spark.table("events").select("id").collect()
                )
                assert after_write == [1, 2, 3, 4, 5], after_write
                observations["connect_write_visible_to_connect"] = after_write

                independent_rows = external_quack_client(
                    owner_bin, extension, "--reader", uri, token
                )
                assert independent_rows == {"event_ids": [1, 2, 3, 4, 5]}, (
                    independent_rows
                )
                observations["connect_write_visible_to_independent_reader"] = (
                    independent_rows["event_ids"]
                )

                verify_write_reattachment(server)
                after_reattachment = external_quack_client(
                    owner_bin, extension, "--reader", uri, token
                )
                assert after_reattachment == {"event_ids": [1, 2, 3, 4, 5, 6]}, (
                    after_reattachment
                )
                observations["reattached_connect_write_committed_once"] = (
                    after_reattachment["event_ids"]
                )

                pending = spark.table("events")
                assert pending.schema.fieldNames() == [
                    "id",
                    "bucket",
                    "label",
                    "amount",
                    "active",
                ]
                stopped = owner_command(owner, "stop")
                if stopped != "stopped":
                    raise AssertionError(f"unexpected Quack stop response: {stopped!r}")
                try:
                    try:
                        external_quack_client(
                            owner_bin, extension, "--reader", uri, token
                        )
                    except RuntimeError as error:
                        if (
                            "could not connect" not in str(error).lower()
                            and "failed to connect" not in str(error).lower()
                        ):
                            raise
                        observations[
                            "new_independent_connection_rejected_while_owner_stopped"
                        ] = True
                    else:
                        raise AssertionError(
                            "an independent Quack connection succeeded while the owner listener was stopped"
                        )
                    try:
                        pending_rows = sorted(row.id for row in pending.collect())
                    except Exception as error:
                        message = str(error).lower()
                        if not any(
                            marker in message
                            for marker in (
                                "quack",
                                "duckdb",
                                "connection",
                                "http",
                                "server",
                                "invalid connection id",
                            )
                        ):
                            raise AssertionError(
                                "owner stop caused an unrelated query failure"
                            ) from error
                        observations[
                            "existing_execution_connection_failure_while_stopped"
                        ] = type(error).__name__
                    else:
                        assert pending_rows == [1, 2, 3, 4, 5, 6], pending_rows
                        observations[
                            "existing_execution_connection_result_while_stopped"
                        ] = pending_rows
                finally:
                    started = owner_command(owner, "start")
                if started != "started":
                    raise AssertionError(
                        f"unexpected Quack start response: {started!r}"
                    )
                recovered_ids = sorted(
                    row.id for row in spark.table("events").select("id").collect()
                )
                assert recovered_ids == [1, 2, 3, 4, 5, 6], recovered_ids
                observations["fresh_query_after_owner_restart"] = recovered_ids

                process_loss_frame = spark.table("events")
                assert process_loss_frame.schema.fieldNames() == [
                    "id",
                    "bucket",
                    "label",
                    "amount",
                    "active",
                ]
                old_owner = owner
                old_owner.kill()
                old_owner.wait(timeout=10)
                if old_owner.stdin is not None:
                    old_owner.stdin.close()
                if old_owner.stdout is not None:
                    old_owner.stdout.close()
                try:
                    process_loss_frame.collect()
                except Exception as error:
                    message = str(error).lower()
                    if not any(
                        marker in message
                        for marker in (
                            "quack",
                            "duckdb",
                            "connection",
                            "http",
                            "server",
                        )
                    ):
                        raise AssertionError(
                            "owner process loss caused an unrelated query failure"
                        ) from error
                    observations["query_failed_during_owner_process_loss"] = type(
                        error
                    ).__name__
                else:
                    raise AssertionError(
                        "query unexpectedly succeeded while the Quack owner process was down"
                    )

                owner, restarted_owner_info = start_persistent_owner(
                    owner_bin, extension, owner_database_path, owner_port
                )
                if restarted_owner_info != owner_info:
                    raise AssertionError(
                        "restarted fixture must retain the same Quack URI and token"
                    )
                process_recovered_ids = sorted(
                    row.id for row in spark.table("events").select("id").collect()
                )
                assert process_recovered_ids == [1, 2, 3, 4, 5, 6], (
                    process_recovered_ids
                )
                observations["fresh_query_after_owner_process_restart"] = (
                    process_recovered_ids
                )
                spark.sql(
                    "INSERT INTO events VALUES "
                    "(7, 70, 'after-owner-process-restart', 11.99, true)"
                ).collect()
                process_restart_reader = external_quack_client(
                    owner_bin, extension, "--reader", uri, token
                )
                assert process_restart_reader == {"event_ids": [1, 2, 3, 4, 5, 6, 7]}, (
                    process_restart_reader
                )
                observations["write_after_owner_process_restart_visible_to_reader"] = (
                    process_restart_reader["event_ids"]
                )
                (
                    owner,
                    spark,
                    observations["quack_owner_loss_during_active_stream"],
                ) = verify_quack_owner_loss_during_stream(
                    server,
                    spark,
                    owner,
                    owner_bin,
                    extension,
                    owner_database_path,
                    owner_port,
                    owner_info,
                )
                observations["quack_query_cancellation"] = (
                    verify_quack_query_cancellation(spark)
                )
                (
                    owner,
                    observations["quack_receipt_replay_after_both_process_restarts"],
                ) = verify_quack_replay_after_both_processes_restart(
                    server,
                    owner,
                    owner_bin,
                    extension,
                    owner_database_path,
                    owner_port,
                    owner_info,
                )
                if server._server is not None:
                    diagnostics = server._server.diagnostics
        except BaseException:
            if server._server is not None:
                diagnostics = server._server.diagnostics
                print("server_diagnostics:\n" + diagnostics, flush=True)
            raise

        report = {
            "format_version": 1,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "environment": {
                "platform": platform.platform(),
                "python": os.sys.version.split()[0],
                "pyspark_client": importlib.metadata.version("pyspark-client"),
            },
            "artifacts": {
                "server_binary": str(args.server_bin.resolve()),
                "server_sha256": sha256(args.server_bin),
                "quack_fixture_owner": str(owner_bin),
                "quack_fixture_owner_sha256": sha256(owner_bin),
                "quack_extension": str(extension),
                "quack_extension_sha256": sha256(extension),
                "runner_sha256": sha256(Path(__file__).resolve()),
            },
            "owner_mode": "shared Quack service; separate fixture and server processes",
            "cases": observations,
            "duckdb_execution_witness_count": diagnostics.count(
                "connect_trace event=duckdb_execution_witness"
            ),
            "limitations": [
                "This is a local source-binary probe on one host, not a cross-platform or release-artifact check.",
                "The Quack process-restart replay uses a file-backed local fixture owner and does not inject a failure at the transaction commit boundary.",
            ],
        }
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
        print(json.dumps(report, indent=2, sort_keys=True))
    finally:
        if owner.poll() is None:
            owner.terminate()
        try:
            owner.wait(timeout=10)
        except subprocess.TimeoutExpired:
            owner.kill()
            owner.wait(timeout=5)
        shutil.rmtree(owner_temp_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
