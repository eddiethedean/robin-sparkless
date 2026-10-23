from __future__ import annotations

import json
import os
import unittest
import urllib.error
import urllib.request

from server_test_utils import SparklessServer


class NativeHttpContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        binary = os.environ.get("SPARKLESS_SERVER_BINARY")
        if not binary:
            raise unittest.SkipTest("SPARKLESS_SERVER_BINARY is not set")
        cls.server = SparklessServer(executable=binary, startup_timeout=90)
        cls.server.start()

    @classmethod
    def tearDownClass(cls) -> None:
        if hasattr(cls, "server"):
            process = cls.server._process
            reader_threads = list(cls.server._reader_threads)
            cls.server.stop()
            if process is not None:
                for stream in (process.stdin, process.stdout, process.stderr):
                    if stream is not None and not stream.closed:
                        raise AssertionError("launcher left a child process pipe open")
            if any(thread.is_alive() for thread in reader_threads):
                raise AssertionError("launcher left a child reader thread running")

    def statement(self, session: str, sql: str, kind: str = "sql") -> tuple[int, dict]:
        request = urllib.request.Request(
            self.server.http_url + "/statements",
            data=json.dumps({"session": session, "kind": kind, "code": sql}).encode(),
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return response.status, json.load(response)
        except urllib.error.HTTPError as error:
            return error.code, json.load(error)

    def test_select_bigint_preserves_exact_value_and_schema(self) -> None:
        status, result = self.statement(
            "release-contract",
            "SELECT CAST(9007199254740993 AS BIGINT) AS n",
        )

        self.assertEqual(status, 200)
        payload = result["data"]["application/json"]
        self.assertEqual(payload["data"], [[9007199254740993]])
        self.assertEqual(payload["schema"]["fields"][0]["name"], "n")
        self.assertEqual(payload["schema"]["fields"][0]["type"], "long")

    def test_scalar_spark_semantics_and_smallint_promotion(self) -> None:
        status, modulo = self.statement("release-modulo", "SELECT -5 % 3 AS r")
        self.assertEqual(status, 200)
        self.assertEqual(modulo["data"]["application/json"]["data"], [[-2]])

        status, arithmetic = self.statement(
            "release-smallint",
            "SELECT CAST(1 AS SMALLINT) + CAST(2 AS SMALLINT) AS n",
        )
        self.assertEqual(status, 200)
        payload = arithmetic["data"]["application/json"]
        self.assertEqual(payload["data"], [[3]])
        self.assertEqual(payload["schema"]["fields"][0]["type"], "integer")

    def test_null_and_empty_results_keep_declared_schema(self) -> None:
        status, null_result = self.statement(
            "release-null",
            "SELECT CAST(NULL AS INT) AS n",
        )
        self.assertEqual(status, 200)
        null_payload = null_result["data"]["application/json"]
        self.assertEqual(null_payload["data"], [[None]])
        self.assertEqual(null_payload["schema"]["fields"][0]["type"], "integer")

        status, empty_result = self.statement(
            "release-empty",
            "SELECT CAST(1 AS INT) AS n WHERE 1 = 0",
        )
        self.assertEqual(status, 200)
        empty_payload = empty_result["data"]["application/json"]
        self.assertEqual(empty_payload["data"], [])
        self.assertEqual(empty_payload["schema"]["fields"][0]["type"], "integer")

    def test_high_precision_decimal_preserves_value_precision_and_scale(self) -> None:
        status, result = self.statement(
            "release-decimal",
            "SELECT CAST('123456789012345678901234567890123456.78' AS DECIMAL(38,2)) AS amount",
        )
        self.assertEqual(status, 200)
        payload = result["data"]["application/json"]
        self.assertEqual(
            payload["data"], [["123456789012345678901234567890123456.78"]]
        )
        field = payload["schema"]["fields"][0]
        self.assertEqual(field["type"], "decimal(38,2)")
        self.assertEqual(field["name"], "amount")

    def test_date_timestamp_and_duplicate_column_labels_keep_order(self) -> None:
        status, result = self.statement(
            "release-datetime",
            "SELECT DATE '2024-02-29' AS x, "
            "TIMESTAMP '2024-02-29 12:34:56.123456' AS x",
        )
        self.assertEqual(status, 200)
        payload = result["data"]["application/json"]
        self.assertEqual(
            payload["data"], [["2024-02-29", "2024-02-29T12:34:56.123456Z"]]
        )
        fields = payload["schema"]["fields"]
        self.assertEqual([field["name"] for field in fields], ["x", "x"])
        self.assertEqual([field["type"] for field in fields], ["date", "timestamp"])

    def test_writes_and_named_tables_fail_closed(self) -> None:
        for query in (
            "CREATE TABLE release_forbidden (id INT)",
            "SELECT * FROM release_forbidden",
        ):
            with self.subTest(query=query):
                status, result = self.statement("release-write", query)
                self.assertEqual(status, 200)
                self.assertEqual(result["status"], "error")
                self.assertEqual(result["ename"], "UnsupportedOperation")

    def test_session_close_is_idempotent_and_session_can_be_reopened(self) -> None:
        session = "release-close"
        status, result = self.statement(session, "SELECT 1 AS n")
        self.assertEqual(status, 200)
        self.assertEqual(result["status"], "ok")

        request = urllib.request.Request(
            self.server.http_url + "/close",
            data=json.dumps({"session": session}).encode(),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            self.assertEqual(response.status, 200)
            self.assertEqual(json.load(response)["status"], "ok")
        with urllib.request.urlopen(request, timeout=10) as response:
            self.assertEqual(response.status, 200)

        status, reopened = self.statement(session, "SELECT 2 AS n")
        self.assertEqual(status, 200)
        self.assertEqual(reopened["status"], "ok")
        self.assertEqual(reopened["data"]["application/json"]["data"], [[2]])

    def test_unsupported_operation_and_multiple_statements_fail_closed(self) -> None:
        status, unsupported = self.statement("release-unsupported", "SELECT 1", kind="python")
        self.assertEqual(status, 200)
        self.assertEqual(unsupported["status"], "error")
        self.assertEqual(unsupported["ename"], "UnsupportedOperation")

        status, multiple = self.statement("release-multiple", "SELECT 1; SELECT 2")
        self.assertEqual(status, 200)
        self.assertEqual(multiple["status"], "error")


class NativeAsyncLauncherTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        binary = os.environ.get("SPARKLESS_SERVER_BINARY")
        if not binary:
            self.skipTest("SPARKLESS_SERVER_BINARY is not set")
        self.server = SparklessServer(executable=binary, startup_timeout=90)
        await self.server.async_start()

    async def asyncTearDown(self) -> None:
        if hasattr(self, "server"):
            await self.server.async_stop()

    async def test_async_launcher_owns_and_reaps_server(self) -> None:
        with urllib.request.urlopen(self.server.http_url + "/health", timeout=10) as response:
            self.assertEqual(response.status, 200)
        self.assertTrue(self.server.is_running)
        await self.server.async_stop()
        self.assertFalse(self.server.is_running)


if __name__ == "__main__":
    unittest.main()
