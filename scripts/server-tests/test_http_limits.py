from __future__ import annotations

import json
import os
import unittest
import urllib.error
import urllib.request

from server_test_utils import SparklessServer


class BoundedHttpContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        binary = os.environ.get("SPARKLESS_SERVER_BINARY")
        if not binary:
            raise unittest.SkipTest("SPARKLESS_SERVER_BINARY is not set")

        overrides = {
            "SPARKLESS_SERVER_MAX_SESSIONS": "16",
            "SPARKLESS_SERVER_MAX_REQUEST_BYTES": "512",
            "SPARKLESS_SERVER_MAX_SQL_BYTES": "256",
            "SPARKLESS_SERVER_MAX_RESPONSE_BYTES": "256",
            "SPARKLESS_SERVER_MAX_CELL_BYTES": "64",
        }
        original = {key: os.environ.get(key) for key in overrides}
        try:
            os.environ.update(overrides)
            cls.server = SparklessServer(executable=binary, startup_timeout=90)
            cls.server.start()
        finally:
            for key, value in original.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

    @classmethod
    def tearDownClass(cls) -> None:
        if hasattr(cls, "server"):
            cls.server.stop()

    def post(self, body: bytes) -> tuple[int, dict]:
        request = urllib.request.Request(
            self.server.http_url + "/statements",
            data=body,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return response.status, json.load(response)
        except urllib.error.HTTPError as error:
            return error.code, json.load(error)

    def statement(self, session: str, sql: str) -> tuple[int, dict]:
        if not hasattr(self, "_sessions"):
            self._sessions: set[str] = set()
        self._sessions.add(session)
        return self.post(
            json.dumps({"session": session, "kind": "sql", "code": sql}).encode()
        )

    def tearDown(self) -> None:
        for session in getattr(self, "_sessions", set()):
            request = urllib.request.Request(
                self.server.http_url + "/close",
                data=json.dumps({"session": session}).encode(),
                headers={"Content-Type": "application/json"},
            )
            try:
                with urllib.request.urlopen(request, timeout=10):
                    pass
            except urllib.error.HTTPError:
                pass

    def test_request_and_sql_limits_reject_without_truncation(self) -> None:
        huge_body = json.dumps(
            {"session": "release-limit-body", "kind": "sql", "code": "SELECT " + "x" * 700}
        ).encode()
        status, body = self.post(huge_body)
        self.assertEqual(status, 413)
        self.assertEqual(body["error"], "RequestTooLarge")

        status, result = self.statement(
            "release-limit-sql", "SELECT " + "1" * 300 + " AS n"
        )
        self.assertEqual(status, 413)
        self.assertEqual(result["error"], "SqlTooLarge")

    def test_cell_and_full_response_limits_are_named_errors(self) -> None:
        status, result = self.statement(
            "release-limit-cell", "SELECT '" + "a" * 70 + "' AS value"
        )
        self.assertEqual(status, 200)
        self.assertEqual(result["status"], "error")
        self.assertEqual(result["ename"], "ResultLimitExceeded")

        cell = "a" * 40
        query = ", ".join("'{}' AS c{}".format(cell, index) for index in range(4))
        status, result = self.statement("release-limit-envelope", "SELECT " + query)
        self.assertEqual(status, 200)
        self.assertEqual(result["status"], "error")
        self.assertEqual(result["ename"], "ResultLimitExceeded")

    def test_session_capacity_rejects_excess_session(self) -> None:
        original = os.environ.get("SPARKLESS_SERVER_MAX_SESSIONS")
        os.environ["SPARKLESS_SERVER_MAX_SESSIONS"] = "2"
        limited = SparklessServer(
            executable=os.environ["SPARKLESS_SERVER_BINARY"], startup_timeout=90
        )
        try:
            limited.start()
        finally:
            if original is None:
                os.environ.pop("SPARKLESS_SERVER_MAX_SESSIONS", None)
            else:
                os.environ["SPARKLESS_SERVER_MAX_SESSIONS"] = original
        try:
            with urllib.request.urlopen(
                urllib.request.Request(
                    limited.http_url + "/statements",
                    data=json.dumps(
                        {
                            "session": "release-session-one",
                            "kind": "sql",
                            "code": "SELECT 1 AS n",
                        }
                    ).encode(),
                    headers={"Content-Type": "application/json"},
                ),
                timeout=10,
            ) as response:
                self.assertEqual(json.load(response)["status"], "ok")
            status, result = self.post_to(limited, "release-session-two", "SELECT 1")
            self.assertEqual(status, 429)
            self.assertEqual(result["error"], "SessionCapacityExceeded")
        finally:
            limited.stop()

    def post_to(self, server: SparklessServer, session: str, sql: str) -> tuple[int, dict]:
        request = urllib.request.Request(
            server.http_url + "/statements",
            data=json.dumps({"session": session, "kind": "sql", "code": sql}).encode(),
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                return response.status, json.load(response)
        except urllib.error.HTTPError as error:
            return error.code, json.load(error)


if __name__ == "__main__":
    unittest.main()
