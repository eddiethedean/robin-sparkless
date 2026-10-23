from __future__ import annotations

import os
import socket
import unittest
import urllib.request

from server_test_utils import SparklessServer


def executable_from_env() -> str:
    binary = os.environ.get("SPARKLESS_SERVER_BINARY")
    if not binary:
        raise unittest.SkipTest("SPARKLESS_SERVER_BINARY is not set")
    return binary


class SyncLauncherLifecycleTests(unittest.TestCase):
    def test_owned_children_nest_and_are_reaped_on_exception(self) -> None:
        binary = executable_from_env()
        outer = SparklessServer(executable=binary, startup_timeout=90)
        inner = SparklessServer(executable=binary, startup_timeout=90)
        processes = []
        with self.assertRaisesRegex(RuntimeError, "sentinel"):
            with outer:
                with inner:
                    processes.extend((outer._process, inner._process))
                    self.assertTrue(outer.is_running)
                    self.assertTrue(inner.is_running)
                    with urllib.request.urlopen(outer.http_url + "/health", timeout=10) as response:
                        self.assertEqual(response.status, 200)
                    with urllib.request.urlopen(inner.http_url + "/health", timeout=10) as response:
                        self.assertEqual(response.status, 200)
                    raise RuntimeError("sentinel")
        self.assertFalse(outer.is_running)
        self.assertFalse(inner.is_running)
        self.assertTrue(all(process.poll() is not None for process in processes))
        with self.assertRaisesRegex(RuntimeError, "not running and ready"):
            _ = outer.http_url

    def test_same_object_nested_context_is_rejected_and_outer_context_survives(self) -> None:
        server = SparklessServer(executable=executable_from_env(), startup_timeout=90)
        with server:
            with self.assertRaisesRegex(RuntimeError, "cannot be entered twice"):
                with server:
                    self.fail("nested entry unexpectedly succeeded")
            self.assertTrue(server.is_running)
        self.assertFalse(server.is_running)

    def test_restart_rechecks_readiness_and_uses_a_new_launch_identity(self) -> None:
        server = SparklessServer(executable=executable_from_env(), startup_timeout=90)
        with self.assertRaisesRegex(RuntimeError, "not running and ready"):
            _ = server.http_url
        server.start()
        first_launch = server._launch_id
        first_process = server._process
        server.stop()
        self.assertIsNotNone(first_process.poll())
        with self.assertRaisesRegex(RuntimeError, "not running and ready"):
            _ = server.http_url
        server.start()
        self.assertNotEqual(server._launch_id, first_launch)
        self.assertTrue(server.is_running)
        server.stop()

    def test_port_collision_fails_startup_without_exposing_an_endpoint(self) -> None:
        binary = executable_from_env()
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
            server = SparklessServer(
                executable=binary,
                port=port,
                startup_timeout=10,
            )
            with self.assertRaisesRegex(RuntimeError, "exited before readiness"):
                server.start()
        self.assertFalse(server.is_running)
        with self.assertRaisesRegex(RuntimeError, "not running and ready"):
            _ = server.http_url

    def test_missing_binary_fails_without_leaking_a_ready_endpoint(self) -> None:
        server = SparklessServer(executable="/definitely/missing/sparkless-server")
        with self.assertRaises(FileNotFoundError):
            server.start()
        with self.assertRaisesRegex(RuntimeError, "not running and ready"):
            _ = server.http_url


class AsyncLauncherContextTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_exception_reaps_owned_child_and_rejects_reentry(self) -> None:
        server = SparklessServer(executable=executable_from_env(), startup_timeout=90)
        process = None
        with self.assertRaisesRegex(RuntimeError, "sentinel"):
            async with server:
                process = server._process
                with self.assertRaisesRegex(RuntimeError, "cannot be entered twice"):
                    async with server:
                        self.fail("nested async entry unexpectedly succeeded")
                self.assertTrue(server.is_running)
                raise RuntimeError("sentinel")
        self.assertFalse(server.is_running)
        self.assertIsNotNone(process.returncode)
        with self.assertRaisesRegex(RuntimeError, "not running and ready"):
            _ = server.http_url


if __name__ == "__main__":
    unittest.main()
