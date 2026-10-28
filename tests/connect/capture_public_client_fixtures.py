#!/usr/bin/env python3
"""Capture deterministic Spark Connect requests from the pinned public client.

The fake service records real pyspark-client request messages and stops the
request before any server execution. These fixtures prove what the client
encodes; they do not assert Sparkless server conformance.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import sys
import uuid
from pathlib import Path

import pyspark
from google.protobuf import text_format
from pyspark.sql.connect.client.reattach import ExecutePlanResponseReattachableIterator
from pyspark.sql.connect.plan import LogicalPlan
from pyspark.sql.connect.proto import base_pb2
from pyspark.sql.connect.session import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


CLIENT_VERSION = "4.0.5"
CLIENT_SESSION_ID = "11111111-1111-4111-8111-111111111111"
SERVER_SESSION_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
OPERATION_IDS = {
    "execute-range-filter-count": "22222222-2222-4222-8222-222222222222",
    "execute-range-select-limit": "33333333-3333-4333-8333-333333333333",
    "execute-local-relation": "44444444-4444-4444-8444-444444444444",
    "execute-empty-local-relation": "55555555-5555-4555-8555-555555555555",
    "execute-column-expressions": "88888888-8888-4888-8888-888888888888",
    "execute-take": "88888888-8888-4888-8888-888888888889",
    "execute-head": "88888888-8888-4888-8888-888888888890",
    "execute-to-local-iterator": "88888888-8888-4888-8888-888888888891",
    "execute-cached-local-relation-boundary": "88888888-8888-4888-8888-888888888892",
    "execute-sql-command-excluded": "88888888-8888-4888-8888-888888888893",
    "reattach-execute": "66666666-6666-4666-8666-666666666666",
    "release-execute-all": "66666666-6666-4666-8666-666666666666",
    "release-execute-prefix": "66666666-6666-4666-8666-666666666666",
}
DEFAULT_CONFIG_VALUES = {
    # Harness values used only to let createDataFrame build its public plan.
    # ConfigRequest fixtures preserve the actual client-requested key list.
    "spark.sql.timestampType": "TIMESTAMP_LTZ",
    "spark.sql.session.timeZone": "UTC",
    "spark.sql.session.localRelationCacheThreshold": "67108864",
    "spark.sql.execution.pandas.convertToArrowArraySafely": "false",
    "spark.sql.execution.pandas.inferPandasDictAsMap": "false",
    "spark.sql.pyspark.inferNestedDictAsStruct.enabled": "false",
    "spark.sql.pyspark.legacy.inferArrayTypeFromFirstElement.enabled": "false",
    "spark.sql.pyspark.legacy.inferMapTypeFromFirstPair.enabled": "false",
    "spark.sql.execution.arrow.useLargeVarTypes": "false",
}


class CaptureStop(BaseException):
    """Stop after request construction without entering a retry/error path."""


class AbortIterator:
    def __iter__(self) -> AbortIterator:
        return self

    def __next__(self) -> base_pb2.ExecutePlanResponse:
        raise CaptureStop()


class RecordingStub:
    def __init__(self, config_values: dict[str, str] | None = None) -> None:
        self.requests: dict[str, object] = {}
        self.config_values = {**DEFAULT_CONFIG_VALUES, **(config_values or {})}

    def _capture(self, rpc: str, request: object) -> None:
        request_type = type(request)
        snapshot = request_type()
        snapshot.CopyFrom(request)
        self.requests[rpc] = snapshot

    def Config(
        self, request: base_pb2.ConfigRequest, metadata: object = None
    ) -> base_pb2.ConfigResponse:
        self._capture("Config", request)
        response = base_pb2.ConfigResponse(
            session_id=request.session_id,
            server_side_session_id=SERVER_SESSION_ID,
        )
        operation = request.operation.WhichOneof("op_type")
        if operation == "get":
            for key in request.operation.get.keys:
                response.pairs.add(key=key, value=self.config_values.get(key, "UTC"))
        elif operation == "get_with_default":
            for pair in request.operation.get_with_default.pairs:
                value = pair.value if pair.HasField("value") else "UTC"
                response.pairs.add(key=pair.key, value=value)
        elif operation == "get_option":
            for key in request.operation.get_option.keys:
                response.pairs.add(key=key)
        elif operation == "get_all":
            response.pairs.extend(
                base_pb2.KeyValue(key=key, value=value)
                for key, value in self.config_values.items()
            )
        elif operation == "is_modifiable":
            for key in request.operation.is_modifiable.keys:
                response.pairs.add(key=key, value="false")
        return response

    def AnalyzePlan(
        self, request: base_pb2.AnalyzePlanRequest, metadata: object = None
    ) -> base_pb2.AnalyzePlanResponse:
        self._capture("AnalyzePlan", request)
        raise CaptureStop()

    def ExecutePlan(
        self, request: base_pb2.ExecutePlanRequest, metadata: object = None
    ) -> AbortIterator:
        self._capture("ExecutePlan", request)
        return AbortIterator()

    def ReleaseExecute(
        self, request: base_pb2.ReleaseExecuteRequest, metadata: object = None
    ) -> base_pb2.ReleaseExecuteResponse:
        return base_pb2.ReleaseExecuteResponse(session_id=request.session_id)

    def Interrupt(
        self, request: base_pb2.InterruptRequest, metadata: object = None
    ) -> base_pb2.InterruptResponse:
        self._capture("Interrupt", request)
        return base_pb2.InterruptResponse(session_id=request.session_id)

    def ReleaseSession(
        self, request: base_pb2.ReleaseSessionRequest, metadata: object = None
    ) -> base_pb2.ReleaseSessionResponse:
        self._capture("ReleaseSession", request)
        return base_pb2.ReleaseSessionResponse(session_id=request.session_id)


def new_session(stub: RecordingStub) -> SparkSession:
    LogicalPlan._nextPlanId = 0
    session = SparkSession("sc://127.0.0.1:15002", userId="contract-fixture-user")
    session.client._session_id = CLIENT_SESSION_ID
    session.client._stub = stub
    return session


def encode_request(rpc: str, request: object, fixture_name: str) -> tuple[bytes, bytes]:
    normalized = type(request)()
    normalized.CopyFrom(request)
    if hasattr(normalized, "client_type"):
        normalized.ClearField("client_type")
    if fixture_name in OPERATION_IDS:
        if not hasattr(normalized, "operation_id"):
            raise AssertionError(
                f"fixture message has no operation_id field: {fixture_name}"
            )
        operation_id = normalized.operation_id
        if not operation_id:
            raise AssertionError(
                f"pinned client omitted operation_id for {fixture_name}"
            )
        try:
            parsed_operation_id = uuid.UUID(operation_id)
        except ValueError as error:
            raise AssertionError(
                f"pinned client sent an invalid operation_id for {fixture_name}: "
                f"{operation_id!r}"
            ) from error
        if parsed_operation_id.version != 4:
            raise AssertionError(
                f"expected a UUIDv4 operation_id for {fixture_name}, got "
                f"{operation_id!r}"
            )
        normalized.operation_id = OPERATION_IDS[fixture_name]
    if rpc not in {
        "AnalyzePlan",
        "Config",
        "ExecutePlan",
        "Interrupt",
        "ReattachExecute",
        "ReleaseExecute",
        "ReleaseSession",
    }:
        raise AssertionError(f"unexpected RPC for contract fixture: {rpc}")
    text_bytes = text_format.MessageToString(normalized, as_utf8=True).encode("utf-8")
    binary_bytes = normalized.SerializeToString(deterministic=True)
    from_text = type(normalized)()
    text_format.Parse(text_bytes.decode("utf-8"), from_text)
    from_binary = type(normalized)()
    from_binary.ParseFromString(binary_bytes)
    expected = normalized.SerializeToString(deterministic=True)
    if (
        from_text.SerializeToString(deterministic=True) != expected
        or from_binary.SerializeToString(deterministic=True) != expected
    ):
        raise AssertionError(
            f"text and binary fixture encodings differ for {fixture_name}"
        )
    return text_bytes, binary_bytes


def collect_fixture_requests() -> dict[str, tuple[str, object]]:
    fixtures: dict[str, tuple[str, object]] = {}

    stub = RecordingStub()
    session = new_session(stub)
    try:
        session.range(0, 10, 1, 1).filter("id > 5").count()
    except CaptureStop:
        pass
    fixtures["execute-range-filter-count"] = (
        "ExecutePlan",
        stub.requests["ExecutePlan"],
    )

    stub = RecordingStub()
    session = new_session(stub)
    try:
        session.range(10).select("id").limit(3).collect()
    except CaptureStop:
        pass
    fixtures["execute-range-select-limit"] = (
        "ExecutePlan",
        stub.requests["ExecutePlan"],
    )

    expression_calls = [
        (
            "execute-column-expressions",
            lambda spark: (
                spark.range(10)
                .filter(F.col("id") >= F.lit(5))
                .select((F.col("id") + F.lit(1)).alias("next"))
                .collect()
            ),
        ),
        ("execute-take", lambda spark: spark.range(10).take(3)),
        ("execute-head", lambda spark: spark.range(10).head(3)),
        (
            "execute-to-local-iterator",
            lambda spark: next(spark.range(10).toLocalIterator()),
        ),
    ]
    for name, call in expression_calls:
        stub = RecordingStub()
        session = new_session(stub)
        try:
            call(session)
        except CaptureStop:
            pass
        fixtures[name] = ("ExecutePlan", stub.requests["ExecutePlan"])

    stub = RecordingStub()
    session = new_session(stub)
    schema = StructType(
        [
            StructField("id", IntegerType(), nullable=False),
            StructField("label", StringType(), nullable=True),
        ]
    )
    try:
        local_df = session.createDataFrame([(1, "alpha"), (2, None)], schema=schema)
        local_df.collect()
    except CaptureStop:
        pass
    fixtures["config-create-dataframe-defaults"] = ("Config", stub.requests["Config"])
    fixtures["execute-local-relation"] = ("ExecutePlan", stub.requests["ExecutePlan"])

    stub = RecordingStub()
    session = new_session(stub)
    try:
        session.createDataFrame([], schema=schema).collect()
    except CaptureStop:
        pass
    fixtures["execute-empty-local-relation"] = (
        "ExecutePlan",
        stub.requests["ExecutePlan"],
    )

    stub = RecordingStub({"spark.sql.session.localRelationCacheThreshold": "1"})
    session = new_session(stub)
    session._cache_local_relation = lambda _relation: "contract-cache-artifact-id"
    try:
        cached_df = session.createDataFrame(
            [(1,)], schema=StructType([StructField("id", IntegerType())])
        )
        cached_df.collect()
    except CaptureStop:
        pass
    fixtures["execute-cached-local-relation-boundary"] = (
        "ExecutePlan",
        stub.requests["ExecutePlan"],
    )

    stub = RecordingStub()
    session = new_session(stub)
    try:
        _ = session.range(10).schema
    except CaptureStop:
        pass
    fixtures["analyze-range-schema"] = ("AnalyzePlan", stub.requests["AnalyzePlan"])

    stub = RecordingStub()
    session = new_session(stub)
    try:
        session.createDataFrame([(1, "alpha")], schema="id INT, label STRING")
    except CaptureStop:
        pass
    fixtures["analyze-ddl-schema"] = ("AnalyzePlan", stub.requests["AnalyzePlan"])

    config_calls = [
        ("config-get", lambda spark: spark.conf.get("spark.sql.session.timeZone")),
        (
            "config-get-with-default",
            lambda spark: spark.conf.get("sparkless.contract.missing", "fallback"),
        ),
        ("config-get-all", lambda spark: spark.conf.getAll),
        (
            "config-is-modifiable",
            lambda spark: spark.conf.isModifiable("spark.sql.ansi.enabled"),
        ),
        ("config-set", lambda spark: spark.conf.set("spark.sql.ansi.enabled", True)),
        ("config-unset", lambda spark: spark.conf.unset("spark.sql.ansi.enabled")),
    ]
    for name, call in config_calls:
        stub = RecordingStub()
        session = new_session(stub)
        call(session)
        fixtures[name] = ("Config", stub.requests["Config"])

    stub = RecordingStub()
    session = new_session(stub)
    # GetOption is a protobuf operation, but Spark 4.0.5's Python RuntimeConf
    # does not expose a method that emits it. Capture the low-level request
    # separately so it is not presented as a public RuntimeConfig call.
    option_operation = base_pb2.ConfigRequest.Operation(
        get_option=base_pb2.ConfigRequest.GetOption(keys=["sparkless.contract.missing"])
    )
    result = session.client.config(option_operation)
    if result.pairs != [("sparkless.contract.missing", None)]:
        raise AssertionError(
            f"GetOption absent-value behavior changed: {result.pairs!r}"
        )
    fixtures["config-get-option"] = ("Config", stub.requests["Config"])

    interrupt_calls = [
        ("interrupt-session", lambda client: client.interrupt_all()),
        ("interrupt-tag", lambda client: client.interrupt_tag("contract-tag")),
        (
            "interrupt-operation",
            lambda client: client.interrupt_operation(
                "77777777-7777-4777-8777-777777777777"
            ),
        ),
    ]
    for name, call in interrupt_calls:
        stub = RecordingStub()
        session = new_session(stub)
        call(session.client)
        fixtures[name] = ("Interrupt", stub.requests["Interrupt"])

    stub = RecordingStub()
    session = new_session(stub)
    session.client.release_session()
    fixtures["release-session"] = ("ReleaseSession", stub.requests["ReleaseSession"])

    stub = RecordingStub()
    session = new_session(stub)
    initial_request = base_pb2.ExecutePlanRequest(
        session_id=CLIENT_SESSION_ID,
        client_observed_server_side_session_id=SERVER_SESSION_ID,
        user_context=base_pb2.UserContext(user_id="contract-fixture-user"),
        operation_id=OPERATION_IDS["reattach-execute"],
    )
    iterator = ExecutePlanResponseReattachableIterator(
        initial_request,
        stub,
        session.client._retrying,
        session.client._builder.metadata(),
    )
    iterator._last_returned_response_id = "opaque-response-id"
    fixtures["reattach-execute"] = (
        "ReattachExecute",
        iterator._create_reattach_execute_request(),
    )
    fixtures["release-execute-all"] = (
        "ReleaseExecute",
        iterator._create_release_execute_request(None),
    )
    fixtures["release-execute-prefix"] = (
        "ReleaseExecute",
        iterator._create_release_execute_request("opaque-response-id"),
    )
    iterator._result_complete = True

    stub = RecordingStub()
    session = new_session(stub)
    try:
        session.sql("SELECT 1 AS result").collect()
    except CaptureStop:
        pass
    fixtures["execute-sql-command-excluded"] = (
        "ExecutePlan",
        stub.requests["ExecutePlan"],
    )

    return fixtures


def build_files() -> dict[Path, bytes]:
    if pyspark.__version__ != CLIENT_VERSION:
        raise RuntimeError(
            f"expected pyspark-client {CLIENT_VERSION}, found pyspark {pyspark.__version__}"
        )
    if sys.version_info[:2] != (3, 11):
        raise RuntimeError(
            f"fixture locks target Python 3.11, found {platform.python_version()}"
        )

    fixture_dir = (
        Path(__file__).parent / "fixtures" / f"pyspark-client-{CLIENT_VERSION}"
    )
    files: dict[Path, bytes] = {}
    manifest: dict[str, object] = {
        "client": "pyspark-client",
        "client_version": CLIENT_VERSION,
        "protocol_source": {
            "tag": "v4.0.5",
            "commit": "b472a3aa3c4b71b5a6ff068ef396f757bd662e59",
            "apache_license": "Apache-2.0",
        },
        "fixture_runtime": "CPython 3.11; patch version omitted from canonical requests",
        "harness_notes": [
            "Fake Config responses let normal public client APIs build plans; the local-relation threshold matches Spark 4.0.5's 64 MiB default except in the explicit one-byte branch fixture.",
            "The cached-local-relation fixture replaces only the artifact-upload call with a stable ID; it does not encode AddArtifacts or certify artifact support.",
            "GetOption is captured through the low-level Config request builder because the pinned Python RuntimeConf API does not expose it.",
        ],
        "normalization": [
            "fixed client session ID and user ID",
            "fixed ExecutePlan operation IDs",
            "empty client_type (runtime OS/Python metadata only)",
            "plan IDs reset to zero before each captured API call",
        ],
        "fixtures": {},
        "purpose": "Actual pinned-client request encoding; no server execution or conformance claim.",
    }

    for name, (rpc, request) in collect_fixture_requests().items():
        text_bytes, binary_bytes = encode_request(rpc, request, name)
        files[fixture_dir / f"{name}.textproto"] = text_bytes
        files[fixture_dir / f"{name}.pb"] = binary_bytes
        manifest["fixtures"][name] = {
            "rpc": rpc,
            "protobuf_type": type(request).DESCRIPTOR.full_name,
            "textproto_sha256": hashlib.sha256(text_bytes).hexdigest(),
            "protobuf_sha256": hashlib.sha256(binary_bytes).hexdigest(),
            "protobuf_bytes": len(binary_bytes),
        }
    files[fixture_dir / "manifest.json"] = (
        json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    return files


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--write", action="store_true", help="write the canonical fixtures"
    )
    mode.add_argument(
        "--check", action="store_true", help="fail if checked-in fixtures differ"
    )
    args = parser.parse_args()

    generated = build_files()
    fixture_dir = (
        Path(__file__).parent / "fixtures" / f"pyspark-client-{CLIENT_VERSION}"
    )
    expected_paths = set(generated)
    existing_paths = set(fixture_dir.iterdir()) if fixture_dir.exists() else set()
    unexpected_paths = existing_paths - expected_paths
    changed: list[Path] = []

    if args.write:
        for path in unexpected_paths:
            if path.is_dir() and not path.is_symlink():
                raise RuntimeError(
                    f"unexpected directory in generated fixture corpus: {path}"
                )
            path.unlink()

    for path, contents in generated.items():
        if args.write:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(contents)
        elif not path.exists() or path.read_bytes() != contents:
            changed.append(path)

    if args.check:
        changed.extend(unexpected_paths)

    if changed:
        for path in sorted(changed):
            print(
                f"fixture differs: {path.relative_to(Path(__file__).parent.parent.parent)}"
            )
        return 1
    mode_name = "wrote" if args.write else "verified"
    print(f"{mode_name} {len(generated) - 1} client request fixtures and manifest")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
