from __future__ import annotations

import asyncio
import io
import json
import os
import socket
import unittest
import urllib.request
from unittest.mock import patch

from server_test_utils import SparklessServer


class _FakePipe(io.BytesIO):
    pass


class _FakeProcess:
    def __init__(self, stdout: bytes) -> None:
        self.stdin = _FakePipe()
        self.stdout = _FakePipe(stdout)
        self.stderr = _FakePipe()
        self.returncode = None
        self.pid = 12345

    def poll(self):
        return self.returncode

    def wait(self, timeout=None):
        self.returncode = 0
        return self.returncode

    def terminate(self):
        self.returncode = -15

    def kill(self):
        self.returncode = -9


class _AsyncFakeReader:
    def __init__(self, *, block: bool = False) -> None:
        self.block = block
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def readline(self) -> bytes:
        self.started.set()
        if self.block:
            await self.release.wait()
        return b""


class _AsyncFakeWriter:
    def write(self, value: bytes) -> None:
        pass

    async def drain(self) -> None:
        pass

    def close(self) -> None:
        pass


class _AsyncFakeProcess:
    def __init__(self) -> None:
        self.stdin = _AsyncFakeWriter()
        self.stdout = _AsyncFakeReader(block=True)
        self.stderr = _AsyncFakeReader()
        self.returncode = None

    async def wait(self) -> int:
        self.returncode = 0
        return self.returncode

    def terminate(self) -> None:
        self.returncode = -15

    def kill(self) -> None:
        self.returncode = -9


def executable_from_env() -> str:
    binary = os.environ.get("SPARKLESS_SERVER_BINARY")
    if not binary:
        raise unittest.SkipTest("SPARKLESS_SERVER_BINARY is not set")
    return binary


class SyncLauncherLifecycleTests(unittest.TestCase):
    def test_startup_timeout_reaps_child_and_closes_pipes(self) -> None:
        binary = executable_from_env()
        process = _FakeProcess(b"")
        server = SparklessServer(executable=binary, startup_timeout=0.02)
        with patch("sparkless.server.subprocess.Popen", return_value=process):
            with self.assertRaisesRegex(TimeoutError, "startup_timeout"):
                server.start()
        self.assertEqual(process.returncode, 0)
        self.assertIsNone(server._process)
        self.assertTrue(process.stdin.closed)
        self.assertTrue(process.stdout.closed)
        self.assertTrue(process.stderr.closed)
        with self.assertRaisesRegex(RuntimeError, "not running and ready"):
            _ = server.http_url

    def test_binary_version_mismatch_reaps_child_before_exposing_endpoint(self) -> None:
        binary = executable_from_env()

        def spawn(args, **kwargs):
            launch_id = args[args.index("--launch-id") + 1]
            record = {
                "type": "ready",
                "launch_id": launch_id,
                "protocol_version": 1,
                "agent_version": 1,
                "binary_version": "0.0.0",
                "capabilities": [
                    "http_agent_v1",
                    "read_only_sql",
                    "typed_scalar_results",
                ],
                "http_url": "http://127.0.0.1:1234",
            }
            process = _FakeProcess(json.dumps(record).encode() + b"\n")
            spawned.append(process)
            return process

        spawned = []
        server = SparklessServer(executable=binary, startup_timeout=1)
        with patch("sparkless.server.subprocess.Popen", side_effect=spawn):
            with self.assertRaisesRegex(RuntimeError, "binary version 0.0.0"):
                server.start()
        self.assertEqual(len(spawned), 1)
        self.assertEqual(spawned[0].returncode, 0)
        self.assertIsNone(server._process)
        with self.assertRaisesRegex(RuntimeError, "not running and ready"):
            _ = server.http_url

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
    async def test_async_startup_cancellation_reaps_child_and_clears_state(self) -> None:
        process = _AsyncFakeProcess()
        server = SparklessServer(executable=executable_from_env(), startup_timeout=30)

        async def spawn(*args, **kwargs):
            return process

        with patch("sparkless.server.asyncio.create_subprocess_exec", new=spawn):
            startup = asyncio.create_task(server.async_start())
            await asyncio.wait_for(process.stdout.started.wait(), timeout=1)
            startup.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await startup

        self.assertEqual(process.returncode, 0)
        self.assertIsNone(server._process)
        self.assertFalse(server.is_running)
        with self.assertRaisesRegex(RuntimeError, "not running and ready"):
            _ = server.http_url

    async def test_async_normal_exit_reaps_owned_child(self) -> None:
        server = SparklessServer(executable=executable_from_env(), startup_timeout=90)
        async with server:
            process = server._process
            self.assertTrue(server.is_running)
            self.assertTrue(server.http_url.startswith("http://"))
        self.assertIsNotNone(process.returncode)
        self.assertFalse(server.is_running)

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
