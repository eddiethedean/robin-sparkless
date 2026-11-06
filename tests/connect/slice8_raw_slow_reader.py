"""Consume Connect Arrow responses at a controlled pace without JVM row conversion."""

from __future__ import annotations

import argparse
import json
import math
import re
import time
import uuid
from pathlib import Path

import pyarrow as pa
from pyspark.sql.connect.proto import base_pb2
from pyspark.sql.connect.session import SparkSession


def wait_for_barrier(path: Path, timeout_seconds: float = 60) -> None:
    deadline = time.monotonic() + timeout_seconds
    while not path.exists():
        if time.monotonic() >= deadline:
            raise TimeoutError("timed out waiting for the slow-reader barrier")
        time.sleep(0.01)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--rows", type=int, required=True)
    parser.add_argument("--pause-ms", type=float, default=100.0)
    parser.add_argument("--timeout-seconds", type=float, default=120.0)
    parser.add_argument("--non-reattachable", action="store_true")
    parser.add_argument("--ready-file", type=Path)
    parser.add_argument("--barrier-file", type=Path)
    parser.add_argument("--first-batch-file", type=Path)
    parser.add_argument("--table")
    args = parser.parse_args()
    if (
        args.rows < 1
        or not math.isfinite(args.pause_ms)
        or args.pause_ms < 0
        or not math.isfinite(args.timeout_seconds)
        or args.timeout_seconds <= 0
    ):
        raise ValueError(
            "rows and RPC timeout must be positive and pause duration non-negative"
        )
    if args.table is not None and not re.fullmatch(
        r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*", args.table
    ):
        raise ValueError("table must be a plain identifier or multipart identifier")

    session = SparkSession.builder.remote(args.url).create()
    client = session._client
    request = client._execute_plan_request_with_metadata()
    request.operation_id = str(uuid.uuid4())
    frame = (
        session.table(args.table).where(f"id < {args.rows}")
        if args.table is not None
        else session.range(args.rows)
    )
    request.plan.CopyFrom(frame.select("id")._plan.to_proto(client))
    if not args.non_reattachable:
        request.request_options.add().reattach_options.reattachable = True

    try:
        if args.ready_file is not None:
            args.ready_file.write_text("ready\n", encoding="utf-8")
        if args.barrier_file is not None:
            wait_for_barrier(args.barrier_file)

        started = time.perf_counter()
        first_batch_ms: float | None = None
        expected_offset = 0
        response_count = 0
        result_complete = False
        metadata = client._builder.metadata()
        released_responses = 0
        for response in client._stub.ExecutePlan(
            request, metadata=metadata, timeout=args.timeout_seconds
        ):
            client._verify_response_integrity(response)
            if response.HasField("arrow_batch"):
                if first_batch_ms is None:
                    first_batch_ms = (time.perf_counter() - started) * 1_000
                batch = response.arrow_batch
                if batch.start_offset != expected_offset:
                    raise AssertionError(
                        f"expected Arrow offset {expected_offset}, got {batch.start_offset}"
                    )
                with pa.ipc.open_stream(batch.data) as reader:
                    table = reader.read_all()
                values = table.column("id").to_pylist()
                expected = list(
                    range(expected_offset, expected_offset + batch.row_count)
                )
                if values != expected or table.num_rows != batch.row_count:
                    raise AssertionError(
                        f"Arrow response at offset {expected_offset} had incorrect rows"
                    )
                expected_offset += batch.row_count
                response_count += 1
                if not args.non_reattachable:
                    release = base_pb2.ReleaseExecuteRequest(
                        session_id=request.session_id,
                        operation_id=request.operation_id,
                        user_context=request.user_context,
                        client_type=request.client_type,
                    )
                    release.release_until.response_id = response.response_id
                    client._stub.ReleaseExecute(release, metadata=metadata, timeout=15)
                    released_responses += 1
                if response_count == 1 and args.first_batch_file is not None:
                    args.first_batch_file.write_text(
                        json.dumps(
                            {
                                "operation_id": request.operation_id,
                                "rows": batch.row_count,
                                "first_batch_ms": first_batch_ms,
                            },
                            sort_keys=True,
                        )
                        + "\n",
                        encoding="utf-8",
                    )
                # Release the current protobuf and Arrow buffers before applying
                # the pacing delay so the client does not retain one extra batch.
                del reader, table, values, expected, batch, response
                if args.pause_ms:
                    time.sleep(args.pause_ms / 1_000)
            elif response.HasField("result_complete"):
                result_complete = True
                if not args.non_reattachable:
                    release = base_pb2.ReleaseExecuteRequest(
                        session_id=request.session_id,
                        operation_id=request.operation_id,
                        user_context=request.user_context,
                        client_type=request.client_type,
                        release_all=base_pb2.ReleaseExecuteRequest.ReleaseAll(),
                    )
                    client._stub.ReleaseExecute(release, metadata=metadata, timeout=15)
                    released_responses += 1

        elapsed_seconds = time.perf_counter() - started
        if expected_offset != args.rows:
            raise AssertionError(f"read {expected_offset} rows, expected {args.rows}")
        if response_count < 1:
            raise AssertionError("Connect returned no Arrow batches")
        if not args.non_reattachable and not result_complete:
            raise AssertionError("reattachable execution omitted ResultComplete")
        print(
            json.dumps(
                {
                    "operation_id": request.operation_id,
                    "rows": expected_offset,
                    "responses": response_count,
                    "elapsed_seconds": elapsed_seconds,
                    "first_batch_ms": first_batch_ms,
                    "pause_ms_per_response": args.pause_ms,
                    "reattachable": not args.non_reattachable,
                    "release_rpcs": released_responses,
                },
                sort_keys=True,
            ),
            flush=True,
        )
    finally:
        session.stop()


if __name__ == "__main__":
    main()
