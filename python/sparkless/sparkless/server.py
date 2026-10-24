"""Owned native HTTP server lifecycle helpers.

The launcher starts only the packaged ``sparkless-server-bin`` executable or an
explicit executable path supplied by the caller. It never downloads, compiles,
or searches ``PATH`` for a server binary.
"""

from __future__ import annotations

import asyncio
import importlib
import ipaddress
import json
import math
import os
import queue
import subprocess
import threading
from collections import deque
from importlib.metadata import PackageNotFoundError, version as distribution_version
from typing import Any, Callable, Deque, Dict, Optional, Tuple, cast
from urllib.parse import urlsplit

from . import __version__

CONTROL_PROTOCOL_VERSION = 1
MAX_CONTROL_FRAME_BYTES = 16 * 1024
MAX_DIAGNOSTIC_BYTES = 64 * 1024


class SparklessServerError(RuntimeError):
    """Raised when the owned native service cannot satisfy its contract."""


class UnsupportedServerPlatformError(SparklessServerError):
    """Raised when no packaged server executable exists for this platform."""


def _resolve_executable(explicit: Optional[str]) -> str:
    if explicit is not None:
        path = os.path.abspath(os.fspath(explicit))
        if not os.path.isfile(path) or not os.access(path, os.X_OK):
            raise SparklessServerError(
                "server executable path must name an existing executable file"
            )
        return path

    try:
        binary_package = importlib.import_module("sparkless_server_bin")
    except ImportError as error:
        raise UnsupportedServerPlatformError(
            "the native server binary is not installed; install sparkless[server] "
            "for this platform or pass executable_path explicitly"
        ) from error
    try:
        binary_package_version = distribution_version("sparkless-server-bin")
    except PackageNotFoundError as error:
        raise UnsupportedServerPlatformError(
            "sparkless-server-bin is not installed as a package"
        ) from error
    if binary_package_version != __version__:
        raise SparklessServerError(
            "sparkless-server-bin version {} does not match sparkless {}".format(
                binary_package_version, __version__
            )
        )
    try:
        executable_path = cast(
            Callable[[], str], getattr(binary_package, "executable_path")
        )
        path = cast(str, os.fspath(executable_path()))
    except (FileNotFoundError, OSError, UnsupportedServerPlatformError) as error:
        raise UnsupportedServerPlatformError(str(error)) from error
    if not os.path.isfile(path) or not os.access(path, os.X_OK):
        raise UnsupportedServerPlatformError(
            "the installed server binary package has no executable for this platform"
        )
    return path


def _command(executable: str, host: str, port: int) -> Tuple[str, ...]:
    if not isinstance(host, str) or not host:
        raise ValueError("host must be a non-empty IP address")
    try:
        ipaddress.ip_address(host)
    except ValueError as error:
        raise ValueError("host must be an IP address") from error
    if type(port) is not int or not 0 <= port <= 65535:
        raise ValueError("port must be an integer in 0..=65535")
    return (executable, "control", "--host", host, "--port", str(port))


def _validate_timeouts(startup: float, shutdown: float, kill: float) -> None:
    for name, value in (
        ("startup_timeout", startup),
        ("shutdown_timeout", shutdown),
        ("kill_timeout", kill),
    ):
        if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
            raise ValueError("{} must be a positive number".format(name))


def _parse_record(frame: bytes) -> Dict[str, Any]:
    if len(frame) > MAX_CONTROL_FRAME_BYTES:
        raise SparklessServerError("server control record exceeds 16 KiB")
    try:
        value = json.loads(frame.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as error:
        raise SparklessServerError(
            "server emitted an invalid control record"
        ) from error
    if not isinstance(value, dict):
        raise SparklessServerError("server control record must be a JSON object")
    return value


def _validate_ready(record: Dict[str, Any]) -> str:
    if record.get("type") != "ready":
        raise SparklessServerError("server did not emit a readiness record")
    protocol_version = record.get("control_protocol_version")
    if (
        type(protocol_version) is not int
        or protocol_version != CONTROL_PROTOCOL_VERSION
    ):
        raise SparklessServerError("server control protocol version is unsupported")
    if record.get("server_version") != __version__:
        raise SparklessServerError(
            "server binary version does not match the installed sparkless package"
        )
    instance_id = record.get("server_instance_id")
    if not isinstance(instance_id, str) or not instance_id:
        raise SparklessServerError("server readiness record has no instance identity")
    capabilities = record.get("capabilities")
    if not isinstance(capabilities, dict) or capabilities.get("transport") != "http":
        raise SparklessServerError("server readiness record has invalid capabilities")
    if capabilities.get("server_instance_id") != instance_id:
        raise SparklessServerError(
            "server capability identity does not match readiness"
        )
    url = record.get("http_url")
    if not isinstance(url, str):
        raise SparklessServerError("server readiness record has no HTTP endpoint")
    parsed_url = urlsplit(url)
    try:
        endpoint_port = parsed_url.port
    except ValueError as error:
        raise SparklessServerError(
            "server readiness record has an invalid HTTP endpoint"
        ) from error
    if (
        parsed_url.scheme != "http"
        or not parsed_url.hostname
        or endpoint_port is None
        or not 1 <= endpoint_port <= 65535
        or parsed_url.username is not None
        or parsed_url.password is not None
        or parsed_url.path
        or parsed_url.query
        or parsed_url.fragment
    ):
        raise SparklessServerError("server readiness record has no HTTP endpoint")
    if capabilities.get("spark_connect_supported") is not False:
        raise SparklessServerError(
            "server advertised an unsupported Spark Connect endpoint"
        )
    agent_version = record.get("agent_protocol_version")
    if (
        not isinstance(agent_version, str)
        or not agent_version
        or capabilities.get("agent_protocol_version") != agent_version
    ):
        raise SparklessServerError("server did not identify its agent protocol version")
    if not isinstance(capabilities.get("mutation_protocol_version"), str):
        raise SparklessServerError(
            "server did not identify its mutation protocol version"
        )
    return url


class _ByteTail:
    def __init__(self, limit: int = MAX_DIAGNOSTIC_BYTES) -> None:
        self._limit = limit
        self._parts: Deque[bytes] = deque()
        self._size = 0
        self._lock = threading.Lock()

    def append(self, value: bytes) -> None:
        with self._lock:
            self._parts.append(value)
            self._size += len(value)
            while self._size > self._limit and self._parts:
                excess = self._size - self._limit
                first = self._parts[0]
                if len(first) <= excess:
                    self._parts.popleft()
                    self._size -= len(first)
                else:
                    self._parts[0] = first[excess:]
                    self._size -= excess

    def text(self) -> str:
        with self._lock:
            value = b"".join(self._parts)
        return value.decode("utf-8", "replace")


def _read_sync_frames(stream: Any, records: "queue.Queue[Any]") -> None:
    while True:
        try:
            frame = stream.readline(MAX_CONTROL_FRAME_BYTES + 1)
        except (OSError, ValueError) as error:
            records.put(error)
            return
        if not frame:
            records.put(None)
            return
        if len(frame) > MAX_CONTROL_FRAME_BYTES or not frame.endswith(b"\n"):
            while frame and not frame.endswith(b"\n"):
                frame = stream.readline(MAX_CONTROL_FRAME_BYTES + 1)
            records.put(
                SparklessServerError("server emitted an oversized control frame")
            )
            continue
        records.put(frame.rstrip(b"\r\n"))


def _drain_sync_stderr(stream: Any, tail: _ByteTail) -> None:
    while True:
        try:
            chunk = stream.read(4096)
        except OSError:
            return
        if not chunk:
            return
        tail.append(chunk)


async def _await_cleanup(task: "asyncio.Task[Any]") -> None:
    """Finish and reap cleanup even if the caller receives another cancellation."""
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            continue
        except BaseException:
            break
    if task.done():
        try:
            task.result()
        except BaseException:
            pass


class SparklessServer:
    """A synchronous owner for one ``sparkless-server`` child process."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 0,
        executable_path: Optional[str] = None,
        startup_timeout: float = 30.0,
        shutdown_timeout: float = 10.0,
        kill_timeout: float = 5.0,
        env: Optional[Dict[str, str]] = None,
    ) -> None:
        self.host = host
        self.port = port
        self._executable_path = executable_path
        self.startup_timeout = startup_timeout
        self.shutdown_timeout = shutdown_timeout
        self.kill_timeout = kill_timeout
        _validate_timeouts(startup_timeout, shutdown_timeout, kill_timeout)
        self.env = None if env is None else dict(env)
        self._process: Optional[subprocess.Popen] = None
        self._records: Optional["queue.Queue[Any]"] = None
        self._stderr_tail = _ByteTail()
        self._threads = []  # type: list[threading.Thread]
        self._owner_pid = os.getpid()
        self._http_url: Optional[str] = None
        self._instance_id: Optional[str] = None
        self._ready_record: Optional[Dict[str, Any]] = None
        self._entered = False

    def _check_owner(self) -> None:
        if os.getpid() != self._owner_pid:
            raise SparklessServerError(
                "server process ownership changed after fork; create a new launcher in the child"
            )

    @property
    def is_running(self) -> bool:
        self._check_owner()
        return self._process is not None and self._process.poll() is None

    @property
    def http_url(self) -> str:
        self._check_owner()
        if not self.is_running or self._http_url is None:
            raise SparklessServerError(
                "HTTP endpoint is available only while the server is ready"
            )
        return self._http_url

    @property
    def server_instance_id(self) -> str:
        self._check_owner()
        if not self.is_running or self._instance_id is None:
            raise SparklessServerError(
                "server identity is available only while the server is ready"
            )
        return self._instance_id

    @property
    def spark_connect_url(self) -> str:
        self._check_owner()
        if self.is_running:
            raise SparklessServerError(
                "Spark Connect is not supported by the HTTP server in this release"
            )
        raise SparklessServerError(
            "Spark Connect URL is available only while the server is running"
        )

    @property
    def diagnostics(self) -> str:
        return self._stderr_tail.text()

    def start(self) -> "SparklessServer":
        self._check_owner()
        if self.is_running and self._http_url is not None:
            return self
        if self._process is not None:
            self._stop_owned(force=False)
        executable = _resolve_executable(self._executable_path)
        command = _command(executable, self.host, self.port)
        child_env = None
        if self.env is not None:
            child_env = os.environ.copy()
            child_env.update(self.env)
        process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=child_env,
            bufsize=0,
        )
        self._process = process
        self._records = queue.Queue(maxsize=4)
        self._stderr_tail = _ByteTail()
        stdout_thread = threading.Thread(
            target=_read_sync_frames, args=(process.stdout, self._records), daemon=True
        )
        stderr_thread = threading.Thread(
            target=_drain_sync_stderr,
            args=(process.stderr, self._stderr_tail),
            daemon=True,
        )
        self._threads = [stdout_thread, stderr_thread]
        for thread in self._threads:
            thread.start()
        try:
            frame = self._records.get(timeout=self.startup_timeout)
            if frame is None:
                raise SparklessServerError("server exited before readiness")
            if isinstance(frame, BaseException):
                raise frame
            record = _parse_record(frame)
            url = _validate_ready(record)
        except BaseException:
            self._stop_owned(force=True)
            raise
        self._ready_record = record
        self._http_url = url
        self._instance_id = record["server_instance_id"]
        return self

    def stop(self) -> None:
        self._check_owner()
        self._stop_owned(force=False)

    def _stop_owned(self, force: bool) -> None:
        process = self._process
        if process is None:
            return
        if process.poll() is None and not force:
            try:
                if process.stdin is not None:
                    process.stdin.write(
                        b'{"control_protocol_version":1,"command":"shutdown"}\n'
                    )
                    process.stdin.flush()
                    process.stdin.close()
            except (BrokenPipeError, OSError):
                pass
            try:
                process.wait(timeout=self.shutdown_timeout)
            except subprocess.TimeoutExpired:
                process.terminate()
                try:
                    process.wait(timeout=self.kill_timeout)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
        elif process.poll() is None:
            process.kill()
            try:
                process.wait(timeout=self.kill_timeout)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        if process.stdout is not None:
            try:
                process.stdout.close()
            except OSError:
                pass
        if process.stderr is not None:
            try:
                process.stderr.close()
            except OSError:
                pass
        self._process = None
        self._records = None
        self._http_url = None
        self._instance_id = None
        self._ready_record = None

    def restart(self) -> "SparklessServer":
        self.stop()
        return self.start()

    def __enter__(self) -> "SparklessServer":
        if self._entered:
            raise SparklessServerError(
                "nested entry of the same server owner is not supported"
            )
        self._entered = True
        try:
            return self.start()
        except BaseException:
            self._entered = False
            raise

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        try:
            self.stop()
        finally:
            self._entered = False


class AsyncSparklessServer:
    """An asyncio owner for one ``sparkless-server`` child process."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 0,
        executable_path: Optional[str] = None,
        startup_timeout: float = 30.0,
        shutdown_timeout: float = 10.0,
        kill_timeout: float = 5.0,
        env: Optional[Dict[str, str]] = None,
    ) -> None:
        self.host = host
        self.port = port
        self._executable_path = executable_path
        self.startup_timeout = startup_timeout
        self.shutdown_timeout = shutdown_timeout
        self.kill_timeout = kill_timeout
        _validate_timeouts(startup_timeout, shutdown_timeout, kill_timeout)
        self.env = None if env is None else dict(env)
        self._process: Optional[asyncio.subprocess.Process] = None
        self._records: Optional["asyncio.Queue[Any]"] = None
        self._stderr_tail = _ByteTail()
        self._stdout_task: Optional["asyncio.Task[None]"] = None
        self._stderr_task: Optional["asyncio.Task[None]"] = None
        self._owner_pid = os.getpid()
        self._http_url: Optional[str] = None
        self._instance_id: Optional[str] = None
        self._ready_record: Optional[Dict[str, Any]] = None
        self._entered = False

    def _check_owner(self) -> None:
        if os.getpid() != self._owner_pid:
            raise SparklessServerError(
                "server process ownership changed after fork; create a new launcher in the child"
            )

    @property
    def is_running(self) -> bool:
        self._check_owner()
        return self._process is not None and self._process.returncode is None

    @property
    def http_url(self) -> str:
        self._check_owner()
        if not self.is_running or self._http_url is None:
            raise SparklessServerError(
                "HTTP endpoint is available only while the server is ready"
            )
        return self._http_url

    @property
    def server_instance_id(self) -> str:
        self._check_owner()
        if not self.is_running or self._instance_id is None:
            raise SparklessServerError(
                "server identity is available only while the server is ready"
            )
        return self._instance_id

    @property
    def spark_connect_url(self) -> str:
        self._check_owner()
        if self.is_running:
            raise SparklessServerError(
                "Spark Connect is not supported by the HTTP server in this release"
            )
        raise SparklessServerError(
            "Spark Connect URL is available only while the server is running"
        )

    @property
    def diagnostics(self) -> str:
        return self._stderr_tail.text()

    async def _read_stdout(self) -> None:
        process = self._process
        assert (
            process is not None
            and process.stdout is not None
            and self._records is not None
        )
        while True:
            try:
                frame = await process.stdout.readline()
            except (asyncio.LimitOverrunError, ValueError) as error:
                await self._records.put(error)
                return
            if not frame:
                await self._records.put(None)
                return
            if len(frame) > MAX_CONTROL_FRAME_BYTES or not frame.endswith(b"\n"):
                await self._records.put(
                    SparklessServerError("server emitted an oversized control frame")
                )
                return
            await self._records.put(frame.rstrip(b"\r\n"))

    async def _read_stderr(self) -> None:
        process = self._process
        assert process is not None and process.stderr is not None
        while True:
            chunk = await process.stderr.read(4096)
            if not chunk:
                return
            self._stderr_tail.append(chunk)

    async def start(self) -> "AsyncSparklessServer":
        self._check_owner()
        if self.is_running and self._http_url is not None:
            return self
        if self._process is not None:
            await self._stop_impl(force=False)
        executable = _resolve_executable(self._executable_path)
        command = _command(executable, self.host, self.port)
        spawn_task = asyncio.create_task(
            asyncio.create_subprocess_exec(
                *command,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=self._child_env(),
                limit=MAX_CONTROL_FRAME_BYTES + 1,
            )
        )
        try:
            self._process = await asyncio.shield(spawn_task)
        except asyncio.CancelledError:
            try:
                self._process = await asyncio.shield(spawn_task)
            except BaseException:
                raise
            cleanup = asyncio.create_task(self._stop_impl(force=True))
            await _await_cleanup(cleanup)
            raise
        self._records = asyncio.Queue(maxsize=4)
        self._stderr_tail = _ByteTail()
        self._stdout_task = asyncio.create_task(self._read_stdout())
        self._stderr_task = asyncio.create_task(self._read_stderr())
        try:
            frame = await asyncio.wait_for(self._records.get(), self.startup_timeout)
            if frame is None:
                raise SparklessServerError("server exited before readiness")
            if isinstance(frame, BaseException):
                raise frame
            record = _parse_record(frame)
            url = _validate_ready(record)
        except BaseException:
            cleanup = asyncio.create_task(self._stop_impl(force=True))
            await _await_cleanup(cleanup)
            raise
        self._ready_record = record
        self._http_url = url
        self._instance_id = record["server_instance_id"]
        return self

    def _child_env(self) -> Optional[Dict[str, str]]:
        if self.env is None:
            return None
        child_env = os.environ.copy()
        child_env.update(self.env)
        return child_env

    async def stop(self) -> None:
        self._check_owner()
        task = asyncio.create_task(self._stop_impl(force=False))
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            await _await_cleanup(task)
            raise

    async def _stop_impl(self, force: bool) -> None:
        process = self._process
        if process is None:
            return
        if process.returncode is None and not force:
            try:
                if process.stdin is not None:
                    process.stdin.write(
                        b'{"control_protocol_version":1,"command":"shutdown"}\n'
                    )
                    await asyncio.wait_for(process.stdin.drain(), 1.0)
                    process.stdin.close()
            except (BrokenPipeError, OSError, asyncio.TimeoutError):
                pass
            try:
                await asyncio.wait_for(process.wait(), self.shutdown_timeout)
            except asyncio.TimeoutError:
                process.terminate()
                try:
                    await asyncio.wait_for(process.wait(), self.kill_timeout)
                except asyncio.TimeoutError:
                    process.kill()
                    await process.wait()
        elif process.returncode is None:
            process.kill()
            try:
                await asyncio.wait_for(process.wait(), self.kill_timeout)
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()
        for task in (self._stdout_task, self._stderr_task):
            if task is not None and not task.done():
                task.cancel()
        for task in (self._stdout_task, self._stderr_task):
            if task is not None:
                try:
                    await task
                except BaseException:
                    pass
        self._process = None
        self._records = None
        self._stdout_task = None
        self._stderr_task = None
        self._http_url = None
        self._instance_id = None
        self._ready_record = None

    async def restart(self) -> "AsyncSparklessServer":
        await self.stop()
        return await self.start()

    async def __aenter__(self) -> "AsyncSparklessServer":
        if self._entered:
            raise SparklessServerError(
                "nested entry of the same server owner is not supported"
            )
        self._entered = True
        try:
            return await self.start()
        except BaseException:
            self._entered = False
            raise

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        try:
            await self.stop()
        finally:
            self._entered = False


__all__ = [
    "AsyncSparklessServer",
    "SparklessServer",
    "SparklessServerError",
    "UnsupportedServerPlatformError",
]
