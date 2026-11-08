"""Demonstrate same-instance HTTP replay and unknown status after restart."""

import argparse
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from sparkless_server import SparklessServer


def request(base, path, body=None):
    req = Request(
        base + path,
        data=None if body is None else json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urlopen(req, timeout=10) as response:
        return json.load(response)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--server-bin")
    args = parser.parse_args()
    with TemporaryDirectory() as directory:
        with SparklessServer(
            executable_path=args.server_bin, connect_port=None
        ) as server:
            instance = request(server.http_url, "/capabilities")["server_instance_id"]
            create = {
                "session": "recovery",
                "kind": "sql",
                "code": "CREATE TABLE items (id BIGINT)",
                "server_instance_id": instance,
                "operation_id": "create-items",
            }
            assert request(server.http_url, "/statements", create)["status"] == "ok"
            original = {
                **create,
                "code": "INSERT INTO items VALUES (42)",
                "operation_id": "insert-42",
            }
            journal = Path(directory) / "request.json"
            journal.write_text(
                json.dumps(original)
            )  # Save before the first submission.
            assert request(server.http_url, "/statements", original)["status"] == "ok"
            # Model loss of the response: recover from the saved request, not a fresh ID.
            saved = json.loads(journal.read_text())
            identity = {
                key: saved[key] for key in ("server_instance_id", "operation_id")
            }
            status = request(server.http_url, "/operations/status", identity)
            assert status["effect_outcome"] == "committed", status
            assert status["execution_state"] == "succeeded", status
            replay = request(server.http_url, "/statements", saved)
            assert replay["operation"]["effect_outcome"] == "committed", replay
            query = {
                "session": "recovery",
                "kind": "sql",
                "code": "SELECT COUNT(*) AS n FROM items",
            }
            assert request(server.http_url, "/statements", query)["data"][
                "application/json"
            ]["data"] == [[1]]
            server.restart()
            assert server.server_instance_id != instance
            try:
                request(server.http_url, "/operations/status", identity)
            except HTTPError as error:
                assert error.code == 409
            else:
                raise AssertionError("old instance status must not be accepted")
    print("HTTP same-instance replay and restart boundary passed")


if __name__ == "__main__":
    main()
