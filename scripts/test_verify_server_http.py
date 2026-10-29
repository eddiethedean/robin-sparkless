"""Check release verification against real native HTTP and corrupted responses."""

import json
import os
import subprocess
import unittest
from pathlib import Path
from unittest.mock import patch

import verify_server_http as verifier

BINARY = Path(
    os.environ.get(
        "SPARKLESS_SERVER_BIN",
        str(Path(__file__).resolve().parents[1] / "target/release/sparkless-server"),
    )
)


@unittest.skipUnless(BINARY.is_file(), "build sparkless-server in release mode first")
class HttpReleaseVerificationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = subprocess.Popen(
            [str(BINARY), "control", "--host", "127.0.0.1", "--port", "0"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
        )
        cls.addClassCleanup(cls.stop_server)
        ready = json.loads(cls.server.stdout.readline())
        cls.url = ready["http_url"]
        verifier.verify_endpoint(cls.url)

    @classmethod
    def stop_server(cls):
        try:
            cls.server.stdin.write(
                '{"control_protocol_version":2,"command":"shutdown"}\n'
            )
            cls.server.stdin.flush()
            cls.server.wait(timeout=15)
        finally:
            if cls.server.poll() is None:
                cls.server.kill()
                cls.server.wait()
            cls.server.stdin.close()
            cls.server.stdout.close()

    def test_real_native_mutation_contract(self):
        verifier.verify_mutations(self.url)

    def assert_corrupted_response_rejected(self, corrupt, message):
        request = verifier.request_json

        def changed_response(url, path, body=None):
            result = request(url, path, body)
            corrupt(path, body, result)
            return result

        with patch.object(verifier, "request_json", side_effect=changed_response):
            with self.assertRaisesRegex(RuntimeError, message):
                verifier.verify_mutations(self.url)

    def test_missing_commit_receipt_fails(self):
        def corrupt(path, body, result):
            if "operation" in result:
                result["operation"]["receipt"] = None

        self.assert_corrupted_response_rejected(corrupt, "committed receipt")

    def test_duplicate_effect_fails(self):
        def corrupt(path, body, result):
            payload = result.get("data", {}).get("application/json", {})
            if body and body.get("code", "").startswith("SELECT id, amount"):
                payload["data"] *= 2

        self.assert_corrupted_response_rejected(corrupt, "duplicate mutation")

    def test_missing_schema_fails(self):
        def corrupt(path, body, result):
            if body and body.get("code", "").startswith("SELECT id, amount"):
                result["data"]["application/json"]["schema"] = {}

        self.assert_corrupted_response_rejected(corrupt, "logical schema")

    def test_status_loss_after_close_fails(self):
        def corrupt(path, body, result):
            if path == "/operations/status":
                result["receipt"] = None

        self.assert_corrupted_response_rejected(
            corrupt, "preserve the committed operation"
        )
