"""Exercise the public HTTP health and SQL contract of a running server."""

from __future__ import annotations

import argparse
import json
import time
import urllib.error
import urllib.request
import uuid


def request_json(base_url: str, path: str, body: dict | None = None) -> dict:
    request = urllib.request.Request(
        base_url.rstrip("/") + path,
        data=None if body is None else json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="GET" if body is None else "POST",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def verify_mutations(base_url: str) -> None:
    """Verify Phase 1 effects through public HTTP without an external adapter."""
    capabilities = request_json(base_url, "/capabilities")
    instance = capabilities.get("server_instance_id")
    if not instance or capabilities.get("mutation_protocol_version") != "1":
        raise RuntimeError("server does not advertise the Phase 1 mutation contract")

    prefix = "release_" + uuid.uuid4().hex
    writer, reader, table = prefix + "_writer", prefix + "_reader", prefix + "_items"

    def statement(session: str, sql: str, operation_id: str | None = None) -> dict:
        body = {"session": session, "kind": "sql", "code": sql}
        if operation_id is not None:
            body.update(server_instance_id=instance, operation_id=operation_id)
        return request_json(base_url, "/statements", body)

    def committed(result: dict, operation_id: str) -> dict:
        operation = result.get("operation", {})
        receipt = operation.get("receipt")
        if (
            result.get("status") != "ok"
            or operation.get("server_instance_id") != instance
            or operation.get("operation_id") != operation_id
            or operation.get("execution_state") != "succeeded"
            or operation.get("effect_outcome") != "committed"
            or not isinstance(receipt, dict)
            or not receipt
        ):
            raise RuntimeError(
                "mutation did not return a committed receipt: " + str(result)
            )
        return operation

    create_id, append_id = prefix + "_create", prefix + "_append"
    committed(
        statement(
            writer, f"CREATE TABLE {table} (id BIGINT, amount DECIMAL(12,3))", create_id
        ),
        create_id,
    )
    sql = f"INSERT INTO {table} VALUES (9007199254740993, 1.250)"
    original = committed(statement(writer, sql, append_id), append_id)
    replay = committed(statement(writer, sql, append_id), append_id)
    if replay != original:
        raise RuntimeError(
            "duplicate operation did not replay the same committed outcome"
        )

    for session, changed_sql in (
        (writer, f"INSERT INTO {table} VALUES (2, 2.000)"),
        (reader, sql),
    ):
        conflict = statement(session, changed_sql, append_id)
        if (
            conflict.get("status") != "error"
            or conflict.get("ename") != "OperationIdentityConflict"
        ):
            raise RuntimeError(
                "changed operation identity was not rejected: " + str(conflict)
            )

    close = request_json(base_url, "/close", {"session": writer})
    if close.get("status") != "ok":
        raise RuntimeError("writer session did not close")
    retained = request_json(
        base_url,
        "/operations/status",
        {"server_instance_id": instance, "operation_id": append_id},
    )
    if retained != original:
        raise RuntimeError(
            "session close did not preserve the committed operation status"
        )
    if committed(statement(writer, sql, append_id), append_id) != original:
        raise RuntimeError("closed-session replay did not retain the committed outcome")

    result = statement(reader, f"SELECT id, amount FROM {table}")
    payload = result.get("data", {}).get("application/json", {})
    if result.get("status") != "ok" or payload.get("data") != [
        [9007199254740993, "1.250"]
    ]:
        raise RuntimeError(
            "shared table lost exact values or applied a duplicate mutation"
        )
    fields = payload.get("schema", {}).get("fields", [])
    if [(f.get("name"), f.get("type")) for f in fields] != [
        ("id", "long"),
        ("amount", "decimal(12,3)"),
    ]:
        raise RuntimeError("shared table lost its exact logical schema: " + str(fields))
    drop_id = prefix + "_drop"
    committed(statement(reader, f"DROP TABLE {table}", drop_id), drop_id)
    for session in (writer, reader):
        request_json(base_url, "/close", {"session": session})


def verify_endpoint(base_url: str, startup_timeout: float = 60.0) -> None:
    endpoint = base_url.rstrip("/")
    deadline = time.monotonic() + startup_timeout
    while True:
        try:
            with urllib.request.urlopen(endpoint + "/health", timeout=5) as response:
                health = json.load(response)
            if health.get("status") == "ready":
                break
        except urllib.error.HTTPError as error:
            if error.code != 503:
                raise
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            pass

        if time.monotonic() >= deadline:
            raise RuntimeError("server did not become ready before the deadline")
        time.sleep(1)

    request = urllib.request.Request(
        endpoint + "/statements",
        data=json.dumps(
            {
                "session": "public-release-smoke",
                "kind": "sql",
                "code": "SELECT 1 AS result",
            }
        ).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        result = json.load(response)
    if result.get("status") != "ok":
        raise RuntimeError("server did not execute the SQL smoke query")
    values = result.get("data", {}).get("application/json", {}).get("data")
    if values != [[1]]:
        raise RuntimeError("server returned an unexpected SQL result")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("base_url", help="base HTTP URL of the running server")
    parser.add_argument("--startup-timeout", type=float, default=60.0)
    parser.add_argument("--mutations", action="store_true")
    args = parser.parse_args()
    try:
        verify_endpoint(args.base_url, args.startup_timeout)
        if args.mutations:
            verify_mutations(args.base_url)
    except (OSError, RuntimeError) as error:
        parser.error(str(error))
    print("server health and HTTP SQL smoke passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
