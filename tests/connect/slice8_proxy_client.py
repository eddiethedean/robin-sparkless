"""Exercise public session, auth, routing-affinity, and trailer behavior via a proxy."""

from __future__ import annotations

import hashlib
import json
import os
import sys
import time
import uuid

import grpc
from pyspark.sql.connect.proto import base_pb2
from pyspark.sql.connect.session import SparkSession
from slice8_proxy_url import url_with_token


def session_fingerprint(session: SparkSession) -> str:
    return hashlib.sha256(session._client._session_id.encode()).hexdigest()[:16]


def server_session_fingerprint(session: SparkSession) -> str:
    value = session._client._server_session_id
    if not value:
        raise AssertionError("proxy did not return a server-side session identity")
    return hashlib.sha256(value.encode()).hexdigest()[:16]


def authenticate_failure(url: str, token: str) -> None:
    session = SparkSession.builder.remote(url_with_token(url, token)).create()
    try:
        try:
            session.range(10).count()
        except Exception as error:
            message = str(error).upper()
            if (
                "UNAUTHENTICATED" not in message
                and "UNAUTHENTICATED" not in repr(error).upper()
            ):
                raise AssertionError(
                    f"invalid proxy credentials returned the wrong failure: {error}"
                ) from error
        else:
            raise AssertionError("proxy accepted an invalid bearer token")
    finally:
        try:
            session.stop()
        except Exception:
            # An authentication interceptor may reject the release RPC as well.
            pass


def observe_config_metadata(session: SparkSession) -> dict[str, object]:
    client = session._client
    request = base_pb2.ConfigRequest(
        session_id=client._session_id,
        user_context=base_pb2.UserContext(user_id=client._user_id or "slice8-proxy"),
    )
    if client._server_session_id is not None:
        request.client_observed_server_side_session_id = client._server_session_id
    request.operation.get.keys.append("spark.sql.session.timeZone")
    probe = str(uuid.uuid4())
    response, call = client._stub.Config.with_call(
        request,
        metadata=(("x-sparkless-slice8-probe", probe),),
        timeout=15,
    )
    if response.session_id != client._session_id:
        raise AssertionError("proxy changed the public client session identity")
    if response.server_side_session_id != client._server_session_id:
        raise AssertionError("proxy changed the server session identity")
    if not response.pairs or response.pairs[0].value != "UTC":
        raise AssertionError("proxy did not return the native session configuration")
    if call.code() is not grpc.StatusCode.OK:
        raise AssertionError(f"Config RPC returned {call.code()}")
    return {
        "request_probe_metadata_key": "x-sparkless-slice8-probe",
        "response_initial_metadata_keys": sorted(
            item.key for item in (call.initial_metadata() or ())
        ),
        "response_trailing_metadata_keys": sorted(
            item.key for item in (call.trailing_metadata() or ())
        ),
        "rpc_status": call.code().name,
    }


def reject_stale_server_identity(session: SparkSession) -> dict[str, object]:
    client = session._client
    stale_id = str(uuid.uuid4())
    while stale_id == client._server_session_id:
        stale_id = str(uuid.uuid4())
    request = base_pb2.ConfigRequest(
        session_id=client._session_id,
        user_context=base_pb2.UserContext(user_id=client._user_id or "slice8-proxy"),
        client_observed_server_side_session_id=stale_id,
    )
    request.operation.get.keys.append("spark.sql.session.timeZone")
    try:
        client._stub.Config.with_call(request, timeout=15)
    except grpc.RpcError as error:
        if error.code() is not grpc.StatusCode.FAILED_PRECONDITION:
            raise AssertionError(
                f"stale server identity returned {error.code()}, expected FAILED_PRECONDITION"
            ) from error
        if "StaleSessionIdentity" not in error.details():
            raise AssertionError(
                f"stale server identity returned the wrong diagnostic: {error.details()}"
            ) from error
        return {"rejected": True, "grpc_status": error.code().name}
    raise AssertionError("proxy accepted a stale server-side session identity")


def cancel_active_query(url: str) -> dict[str, object]:
    session = SparkSession.builder.remote(url).create()
    expected_rows = 10_000_000
    consumed = 0
    try:
        iterator = iter(session.range(expected_rows).select("id").toLocalIterator())
        first = next(iterator)
        if first.id != 0:
            raise AssertionError(
                f"cancellation stream began at row {first.id}, expected 0"
            )
        consumed = 1
        interrupted_ids = session._client.interrupt_all()
        if not interrupted_ids:
            raise AssertionError(
                "proxy did not report an active query when Interrupt(all) was sent"
            )
        deadline = time.monotonic() + 30
        while consumed < expected_rows:
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    "interrupted stream did not terminate within 30 seconds"
                )
            try:
                next(iterator)
                consumed += 1
            except StopIteration as error:
                raise AssertionError(
                    "interrupted stream ended without a visible cancellation error"
                ) from error
            except Exception as error:
                message = f"{error} {error!r}".upper()
                if "CANCELLED" not in message and "INTERRUPTED" not in message:
                    raise AssertionError(
                        f"interrupted stream returned the wrong error: {error}"
                    ) from error
                return {
                    "cancelled": True,
                    "interrupted_operation_count": len(interrupted_ids),
                    "rows_consumed_before_cancellation": consumed,
                    "error": type(error).__name__,
                }
        raise AssertionError("large query completed before cancellation was observed")
    finally:
        session.stop()


def authenticated_workload(url: str, token: str) -> dict[str, object]:
    authenticated_url = url_with_token(url, token)
    first = SparkSession.builder.remote(authenticated_url).create()
    second = SparkSession.builder.remote(authenticated_url).create()
    first_stopped = False
    try:
        first_client_id = first._client._session_id
        second_client_id = second._client._session_id
        if first_client_id == second_client_id:
            raise AssertionError("public clients reused a Connect session identity")

        values = first.range(10).filter("id > 5").count()
        if values != 4:
            raise AssertionError(f"proxy returned count {values}, expected 4")
        first_server_id = server_session_fingerprint(first)
        metadata = observe_config_metadata(first)
        streamed = first.range(20_000).select("id").toArrow()
        streamed_ids = streamed.column("id").to_pylist()
        if streamed_ids != list(range(20_000)):
            raise AssertionError(
                "same-session Arrow stream returned incorrect values or row count"
            )
        if str(streamed.schema.field("id").type) != "int64":
            raise AssertionError(
                "same-session Arrow stream returned the wrong physical schema"
            )
        if server_session_fingerprint(first) != first_server_id:
            raise AssertionError(
                "proxy changed backend session affinity during a session"
            )
        stale_identity = reject_stale_server_identity(first)

        second_count = second.range(7).count()
        if second_count != 7:
            raise AssertionError(f"second proxy session returned count {second_count}")
        second_server_id = server_session_fingerprint(second)
        if first_server_id == second_server_id:
            raise AssertionError(
                "independent public sessions shared one backend session identity"
            )

        first.stop()
        first_stopped = True
        if second.range(9).count() != 9:
            raise AssertionError("releasing one proxy session affected another session")
        if server_session_fingerprint(second) != second_server_id:
            raise AssertionError("proxy changed the second session's backend affinity")

        return {
            "independent_client_sessions": 2,
            "client_session_fingerprints": [
                hashlib.sha256(first_client_id.encode()).hexdigest()[:16],
                hashlib.sha256(second_client_id.encode()).hexdigest()[:16],
            ],
            "backend_session_fingerprints": [first_server_id, second_server_id],
            "same_session_affinity": True,
            "release_isolation": True,
            "stale_identity_rejected": stale_identity,
            "multi_batch_arrow_stream": {
                "rows": len(streamed_ids),
                "first_id": streamed_ids[0],
                "last_id": streamed_ids[-1],
                "physical_type": str(streamed.schema.field("id").type),
            },
            "config_rpc_metadata_and_trailers": metadata,
            "cancellation": cancel_active_query(authenticated_url),
        }
    finally:
        if not first_stopped:
            first.stop()
        second.stop()


def main() -> None:
    url = os.environ.get("SPARKLESS_PROXY_URL")
    token = os.environ.get("SPARKLESS_PROXY_TOKEN")
    mode = sys.argv[1] if len(sys.argv) == 2 else "workload"
    if not url or not token:
        raise RuntimeError("SPARKLESS_PROXY_URL and SPARKLESS_PROXY_TOKEN are required")
    if mode == "reject-invalid-token":
        authenticate_failure(url, "slice8-invalid-token-" + str(uuid.uuid4()))
        print(json.dumps({"invalid_token_rejected": True}), flush=True)
        return
    if mode != "workload":
        raise ValueError(f"unknown proxy client mode: {mode}")
    print(json.dumps(authenticated_workload(url, token), sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
