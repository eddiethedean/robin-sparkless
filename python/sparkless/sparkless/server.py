"""Lifecycle helpers for an owned ``sparkless-server`` executable.

Importing this module never starts a process or downloads/builds a binary. Install
``sparkless[server]`` for a packaged executable, or pass ``executable=...``.
"""

from __future__ import annotations

import atexit
import asyncio
import contextlib
import importlib
import importlib.resources
import json
import math
import os
import queue
import re
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Union
from urllib.parse import urlsplit, urlunsplit

from . import __version__

_CONTROL_VERSION = 1
_AGENT_VERSION = 1
_MAX_CONTROL_BYTES = 16 * 1024
_MAX_DIAGNOSTIC_BYTES = 64 * 1024
_REQUIRED_CAPABILITIES = {
    "http_agent_v1",
    "read_only_sql",
    "typed_scalar_results",
}
_SECRET_RE = re.compile(
    r"(?i)\b([A-Za-z0-9_-]*(?:password|passwd|token|secret|authorization|api[_-]?key)"
    r"[A-Za-z0-9_-]*)(\s*[=:]\s*)"
    r"(?:[A-Za-z][A-Za-z0-9_-]*\s+)?[^\s,;]+"
)
_URL_PASSWORD_RE = re.compile(r"(?i)(://[^:/@\s]+:)[^@/\s]+(@)")


class SparklessServer:
    """Start and own a native Sparkless HTTP SQL child process.

    ``http_url`` is available only after the child's readiness record has been
    validated. Spark Connect is not part of the 4.16 HTTP release and
    ``spark_connect_url`` raises :class:`NotImplementedError`.
    """

    def __init__(
        self,
        executable: Optional[Union[str, os.PathLike[str]]] = None,
        *,
        bind: str = "127.0.0.1",
        port: int = 0,
        advertised_host: Optional[str] = None,
        startup_timeout: float = 30.0,
        shutdown_timeout: float = 10.0,
        kill_timeout: float = 5.0,
    ) -> None:
        if not 0 <= port <= 65535:
            raise ValueError("port must be between 0 and 65535")
        timeouts = (startup_timeout, shutdown_timeout, kill_timeout)
        if not all(math.isfinite(timeout) and 0 < timeout <= 24 * 60 * 60 for timeout in timeouts):
            raise ValueError("lifecycle timeouts must be finite, greater than zero, and at most 24 hours")
        self._executable = Path(executable) if executable is not None else None
        self._bind = bind
        self._port = port
        self._advertised_host = advertised_host
        self._startup_timeout = startup_timeout
        self._shutdown_timeout = shutdown_timeout
        self._kill_timeout = kill_timeout

        self._lifecycle_lock = threading.Lock()
        self._diagnostics_lock = threading.Lock()
        self._process: Any = None
        self._process_kind: Optional[str] = None
        self._owner_pid: Optional[int] = None
        self._launch_id: Optional[str] = None
        self._ready: Optional[Dict[str, Any]] = None
        self._http_url: Optional[str] = None
        self._stdout_records: "queue.Queue[Union[Dict[str, Any], BaseException]]" = queue.Queue(
            maxsize=8
        )
        self._reader_threads: List[threading.Thread] = []
        self._async_reader_tasks: List[asyncio.Task[Any]] = []
        self._async_loop: Optional[asyncio.AbstractEventLoop] = None
        self._diagnostics = bytearray()
        self._diagnostic_pending = bytearray()
        self._diagnostic_discarding_line = False
        self._resource_context: Any = None
        self._resource_path: Optional[Path] = None
        self._sync_entered = False
        self._async_entered = False
        atexit.register(self._atexit_stop)

    @property
    def pid(self) -> Optional[int]:
        process = self._process
        if process is None:
            return None
        return getattr(process, "pid", None)

    @property
    def is_running(self) -> bool:
        process = self._process
        if process is None or self._owner_pid != os.getpid():
            return False
        if self._process_kind == "sync":
            return process.poll() is None
        return process.returncode is None

    @property
    def http_url(self) -> str:
        self._require_ready()
        assert self._http_url is not None
        return self._http_url

    @property
    def spark_connect_url(self) -> str:
        self._require_ready()
        raise NotImplementedError(
            "Spark Connect is not supported by sparkless-server 4.16; only the HTTP SQL agent is available"
        )

    @property
    def diagnostics(self) -> str:
        with self._diagnostics_lock:
            return self._diagnostics.decode("utf-8", errors="replace")

    def start(self) -> "SparklessServer":
        with self._lifecycle_lock:
            self._check_process_owner()
            self._require_process_mode("sync")
            if self._ready is not None and self.is_running:
                return self
            if self._process is not None:
                self._stop_sync_locked()
            self._start_sync_locked()
            return self

    def stop(self) -> None:
        with self._lifecycle_lock:
            if self._process is None:
                self._clear_ready_state()
                return
            self._check_process_owner()
            self._require_process_mode("sync")
            self._stop_sync_locked()

    async def async_start(self) -> "SparklessServer":
        async with self._async_lifecycle_lock():
            self._check_process_owner()
            self._require_process_mode("async")
            if (
                self._process is not None
                and self._async_loop is not asyncio.get_running_loop()
            ):
                raise RuntimeError(
                    "an async SparklessServer must be managed on the event loop that started it"
                )
            if self._ready is not None and self.is_running:
                return self
            if self._process is not None:
                cleanup = asyncio.create_task(self._stop_async_locked())
                try:
                    await asyncio.shield(cleanup)
                except asyncio.CancelledError:
                    await asyncio.shield(cleanup)
                    raise
            try:
                await self._start_async_locked()
            except BaseException:
                cleanup = asyncio.create_task(self._stop_async_locked())
                try:
                    await asyncio.shield(cleanup)
                except asyncio.CancelledError:
                    await asyncio.shield(cleanup)
                raise
            return self

    async def async_stop(self) -> None:
        async with self._async_lifecycle_lock():
            if self._process is None:
                self._clear_ready_state()
                return
            self._check_process_owner()
            self._require_process_mode("async")
            if self._async_loop is not asyncio.get_running_loop():
                raise RuntimeError(
                    "an async SparklessServer must be stopped on the event loop that started it"
                )
            cleanup = asyncio.create_task(self._stop_async_locked())
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError:
                await asyncio.shield(cleanup)
                raise

    def __enter__(self) -> "SparklessServer":
        if self._sync_entered:
            raise RuntimeError("the same SparklessServer object cannot be entered twice")
        self.start()
        self._sync_entered = True
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self._sync_entered = False
        self.stop()

    async def __aenter__(self) -> "SparklessServer":
        if self._async_entered:
            raise RuntimeError("the same SparklessServer object cannot be entered twice")
        await self.async_start()
        self._async_entered = True
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self._async_entered = False
        await self.async_stop()

    def _require_ready(self) -> None:
        if self._owner_pid is not None and self._owner_pid != os.getpid():
            raise RuntimeError("this launcher was inherited across fork and cannot control its parent's child")
        if self._ready is None or not self.is_running:
            raise RuntimeError("SparklessServer is not running and ready; call start() or enter its context")

    def _check_process_owner(self) -> None:
        if self._owner_pid is not None and self._owner_pid != os.getpid():
            raise RuntimeError("this launcher was inherited across fork and cannot control its parent's child")

    def _require_process_mode(self, requested: str) -> None:
        if self._process is not None and self._process_kind != requested:
            raise RuntimeError(
                "SparklessServer was started with {}; use the matching lifecycle methods".format(
                    self._process_kind
                )
            )

    def _start_sync_locked(self) -> None:
        self._clear_diagnostics()
        executable = self._resolve_executable()
        launch_id = uuid.uuid4().hex
        args = self._command(executable, launch_id)
        try:
            process = subprocess.Popen(
                args,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=0,
            )
        except OSError as error:
            self._release_resource()
            raise RuntimeError("could not start the sparkless-server executable: {}".format(error)) from error
        self._process = process
        self._process_kind = "sync"
        self._owner_pid = os.getpid()
        self._launch_id = launch_id
        self._ready = None
        self._http_url = None
        self._stdout_records = queue.Queue(maxsize=8)
        self._reader_threads = [
            threading.Thread(target=self._drain_stdout, args=(process,), daemon=True),
            threading.Thread(target=self._drain_stderr, args=(process,), daemon=True),
        ]
        for thread in self._reader_threads:
            thread.start()
        try:
            record = self._wait_ready_sync(process)
            self._validate_ready_record(record, launch_id)
            self._ready = record
            self._http_url = self._client_url(record["http_url"])
        except BaseException:
            self._stop_sync_locked()
            raise

    async def _start_async_locked(self) -> None:
        self._clear_diagnostics()
        executable = self._resolve_executable()
        launch_id = uuid.uuid4().hex
        args = self._command(executable, launch_id)
        loop = asyncio.get_running_loop()
        spawn = asyncio.create_task(
            asyncio.create_subprocess_exec(
                *args,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                limit=_MAX_CONTROL_BYTES + 1,
            )
        )
        try:
            process = await asyncio.shield(spawn)
        except asyncio.CancelledError:
            process = await asyncio.shield(spawn)
            self._process = process
            self._process_kind = "async"
            self._owner_pid = os.getpid()
            self._launch_id = launch_id
            self._start_async_drainers(loop, process)
            raise
        self._process = process
        self._process_kind = "async"
        self._owner_pid = os.getpid()
        self._launch_id = launch_id
        self._ready = None
        self._http_url = None
        self._start_async_drainers(loop, process)
        record = await self._wait_ready_async(process)
        self._validate_ready_record(record, launch_id)
        self._ready = record
        self._http_url = self._client_url(record["http_url"])

    def _start_async_drainers(self, loop: asyncio.AbstractEventLoop, process: Any) -> None:
        self._async_loop = loop
        self._async_reader_tasks = [
            loop.create_task(self._drain_async_stderr(process)),
        ]

    def _wait_ready_sync(self, process: subprocess.Popen[bytes]) -> Dict[str, Any]:
        deadline = time.monotonic() + self._startup_timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("sparkless-server did not become ready before startup_timeout")
            try:
                record = self._stdout_records.get(timeout=min(remaining, 0.1))
            except queue.Empty:
                status = process.poll()
                if status is not None:
                    raise RuntimeError(
                        "sparkless-server exited before readiness (exit code {})".format(status)
                    )
                continue
            if isinstance(record, BaseException):
                raise RuntimeError("invalid sparkless-server control output: {}".format(record))
            if record.get("type") == "ready":
                return record
            raise RuntimeError("sparkless-server emitted an unexpected control record before readiness")

    async def _wait_ready_async(self, process: Any) -> Dict[str, Any]:
        assert process.stdout is not None
        deadline = asyncio.get_running_loop().time() + self._startup_timeout
        try:
            line = await asyncio.wait_for(
                process.stdout.readline(), timeout=self._startup_timeout
            )
        except asyncio.TimeoutError as error:
            raise TimeoutError("sparkless-server did not become ready before startup_timeout") from error
        except (asyncio.LimitOverrunError, ValueError) as error:
            raise RuntimeError("sparkless-server readiness record exceeds 16 KiB") from error
        if len(line) > _MAX_CONTROL_BYTES:
            raise RuntimeError("sparkless-server readiness record exceeds 16 KiB")
        if not line:
            status = process.returncode
            if status is None:
                raise RuntimeError(
                    "sparkless-server closed its control output before readiness"
                )
            raise RuntimeError(
                "sparkless-server exited before readiness (exit code {})".format(status)
            )
        try:
            record = json.loads(line.decode("utf-8"))
        except (UnicodeDecodeError, ValueError) as error:
            raise RuntimeError("sparkless-server emitted invalid readiness JSON") from error
        if asyncio.get_running_loop().time() > deadline:
            raise TimeoutError("sparkless-server did not become ready before startup_timeout")
        if not isinstance(record, dict) or record.get("type") != "ready":
            raise RuntimeError("sparkless-server emitted an unexpected control record before readiness")
        return record

    def _validate_ready_record(self, record: Dict[str, Any], launch_id: str) -> None:
        if record.get("launch_id") != launch_id:
            raise RuntimeError("readiness record belongs to a different server launch")
        if record.get("protocol_version") != _CONTROL_VERSION:
            raise RuntimeError("sparkless-server control protocol version is not supported")
        if record.get("agent_version") != _AGENT_VERSION:
            raise RuntimeError("sparkless-server HTTP agent version is not supported")
        if record.get("binary_version") != __version__:
            raise RuntimeError(
                "sparkless-server binary version {} does not match sparkless package {}".format(
                    record.get("binary_version"), __version__
                )
            )
        capabilities = record.get("capabilities")
        if not isinstance(capabilities, list) or not all(
            isinstance(capability, str) for capability in capabilities
        ):
            raise RuntimeError("sparkless-server reported invalid HTTP SQL capabilities")
        if not _REQUIRED_CAPABILITIES.issubset(capabilities):
            raise RuntimeError("sparkless-server did not report the required HTTP SQL capabilities")
        address = record.get("http_url")
        try:
            parsed = urlsplit(address) if isinstance(address, str) else None
            port = parsed.port if parsed is not None else None
        except ValueError as error:
            raise RuntimeError("sparkless-server readiness record has an invalid HTTP address") from error
        if (
            parsed is None
            or parsed.scheme != "http"
            or not parsed.hostname
            or port is None
            or port == 0
            or parsed.username is not None
            or parsed.password is not None
        ):
            raise RuntimeError("sparkless-server readiness record has an invalid HTTP address")

    def _client_url(self, actual_url: str) -> str:
        parsed = urlsplit(actual_url)
        host = self._advertised_host
        if host is None and parsed.hostname in ("0.0.0.0", "::"):
            host = "127.0.0.1"
        if host is None:
            return actual_url
        authority = "[{}]".format(host) if ":" in host and not host.startswith("[") else host
        if parsed.port is not None:
            authority = "{}:{}".format(authority, parsed.port)
        return urlunsplit((parsed.scheme, authority, parsed.path, parsed.query, parsed.fragment))

    def _command(self, executable: Path, launch_id: str) -> List[str]:
        return [
            str(executable),
            "serve",
            "--bind",
            self._bind,
            "--port",
            str(self._port),
            "--control",
            "--launch-id",
            launch_id,
        ]

    def _resolve_executable(self) -> Path:
        if self._executable is not None:
            executable = self._executable.expanduser().resolve()
            if not executable.is_file():
                raise FileNotFoundError("sparkless-server executable does not exist: {}".format(executable))
            if os.name != "nt" and not os.access(str(executable), os.X_OK):
                raise PermissionError("sparkless-server executable is not executable: {}".format(executable))
            return executable
        try:
            package = importlib.import_module("sparkless_server_bin")
        except ImportError as error:
            raise RuntimeError(
                "the optional server binary is not installed; install sparkless[server] or pass executable=..."
            ) from error
        package_version = getattr(package, "__version__", None)
        if package_version != __version__:
            raise RuntimeError(
                "sparkless-server-bin version {} does not match sparkless {}".format(
                    package_version, __version__
                )
            )
        resource_name = "sparkless-server.exe" if os.name == "nt" else "sparkless-server"
        try:
            self._resource_context = importlib.resources.path(
                "sparkless_server_bin", resource_name
            )
            resource_path = self._resource_context.__enter__()
        except (FileNotFoundError, ModuleNotFoundError, OSError) as error:
            self._release_resource()
            raise RuntimeError(
                "sparkless-server-bin does not contain a binary for this platform"
            ) from error
        self._resource_path = Path(resource_path)
        if not self._resource_path.is_file():
            self._release_resource()
            raise RuntimeError("sparkless-server binary resource is missing")
        if os.name != "nt" and not os.access(str(self._resource_path), os.X_OK):
            self._release_resource()
            raise PermissionError("packaged sparkless-server binary is not executable")
        return self._resource_path

    def _stop_sync_locked(self) -> None:
        process = self._process
        if process is None:
            self._clear_ready_state()
            return
        self._ready = None
        self._http_url = None
        try:
            if process.poll() is None:
                self._send_shutdown_sync(process)
                try:
                    process.wait(timeout=self._shutdown_timeout)
                except subprocess.TimeoutExpired:
                    process.terminate()
                    try:
                        process.wait(timeout=self._kill_timeout)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
        finally:
            self._finish_process(process)

    def _send_shutdown_sync(self, process: subprocess.Popen[bytes]) -> None:
        if process.stdin is None or self._launch_id is None:
            return
        record = json.dumps(
            {"type": "shutdown", "launch_id": self._launch_id},
            separators=(",", ":"),
        ).encode("utf-8") + b"\n"
        if len(record) > _MAX_CONTROL_BYTES:
            return
        try:
            process.stdin.write(record)
            process.stdin.flush()
        except (BrokenPipeError, OSError):
            pass

    async def _stop_async_locked(self) -> None:
        process = self._process
        if process is None:
            self._clear_ready_state()
            return
        self._ready = None
        self._http_url = None
        try:
            if process.returncode is None:
                await self._send_shutdown_async(process)
                try:
                    await asyncio.wait_for(process.wait(), timeout=self._shutdown_timeout)
                except asyncio.TimeoutError:
                    process.terminate()
                    try:
                        await asyncio.wait_for(process.wait(), timeout=self._kill_timeout)
                    except asyncio.TimeoutError:
                        process.kill()
                        await process.wait()
        finally:
            await self._finish_async_process(process)

    async def _send_shutdown_async(self, process: Any) -> None:
        if process.stdin is None or self._launch_id is None:
            return
        record = json.dumps(
            {"type": "shutdown", "launch_id": self._launch_id},
            separators=(",", ":"),
        ).encode("utf-8") + b"\n"
        if len(record) > _MAX_CONTROL_BYTES:
            return
        try:
            process.stdin.write(record)
            await asyncio.wait_for(process.stdin.drain(), timeout=self._shutdown_timeout)
        except (BrokenPipeError, ConnectionError, OSError, asyncio.TimeoutError):
            pass

    def _finish_process(self, process: Any) -> None:
        for thread in self._reader_threads:
            thread.join(timeout=1)
        for stream in (process.stdin, process.stdout, process.stderr):
            if stream is not None:
                with contextlib.suppress(OSError):
                    stream.close()
        for thread in self._reader_threads:
            thread.join(timeout=1)
        self._reader_threads = []
        self._process = None
        self._process_kind = None
        self._owner_pid = None
        self._launch_id = None
        self._ready = None
        self._http_url = None
        self._release_resource()

    async def _finish_async_process(self, process: Any) -> None:
        for task in self._async_reader_tasks:
            if not task.done():
                task.cancel()
        if self._async_reader_tasks:
            await asyncio.gather(*self._async_reader_tasks, return_exceptions=True)
        self._async_reader_tasks = []
        if process.stdin is not None:
            process.stdin.close()
        self._process = None
        self._process_kind = None
        self._owner_pid = None
        self._launch_id = None
        self._ready = None
        self._http_url = None
        self._release_resource()

    def _clear_ready_state(self) -> None:
        self._ready = None
        self._http_url = None
        self._launch_id = None
        self._owner_pid = None
        self._process = None
        self._process_kind = None
        self._release_resource()

    def _release_resource(self) -> None:
        context = self._resource_context
        self._resource_context = None
        self._resource_path = None
        if context is not None:
            with contextlib.suppress(Exception):
                context.__exit__(None, None, None)

    def _drain_stdout(self, process: subprocess.Popen[bytes]) -> None:
        assert process.stdout is not None
        while True:
            line = process.stdout.readline(_MAX_CONTROL_BYTES + 1)
            if not line:
                return
            if len(line) > _MAX_CONTROL_BYTES:
                self._queue_stdout_record(RuntimeError("control record exceeds 16 KiB"))
                return
            try:
                import json

                record = json.loads(line.decode("utf-8"))
                if not isinstance(record, dict):
                    raise ValueError("control record must be a JSON object")
                self._queue_stdout_record(record)
            except (UnicodeDecodeError, ValueError) as error:
                self._queue_stdout_record(error)
                return

    def _queue_stdout_record(self, record: Union[Dict[str, Any], BaseException]) -> None:
        try:
            self._stdout_records.put_nowait(record)
        except queue.Full:
            # The control stream is capped and carries only readiness and stop.
            pass

    def _drain_stderr(self, process: subprocess.Popen[bytes]) -> None:
        assert process.stderr is not None
        while True:
            chunk = process.stderr.read(4096)
            if not chunk:
                self._append_diagnostics(b"", final=True)
                return
            self._append_diagnostics(chunk)

    async def _drain_async_stderr(self, process: Any) -> None:
        assert process.stderr is not None
        while True:
            chunk = await process.stderr.read(4096)
            if not chunk:
                self._append_diagnostics(b"", final=True)
                return
            self._append_diagnostics(chunk)

    def _append_diagnostics(self, chunk: bytes, *, final: bool = False) -> None:
        with self._diagnostics_lock:
            if self._diagnostic_discarding_line:
                newline = chunk.find(b"\n")
                if newline < 0:
                    return
                chunk = chunk[newline + 1 :]
                self._diagnostic_discarding_line = False
            self._diagnostic_pending.extend(chunk)
            complete_lines = []
            while True:
                newline = self._diagnostic_pending.find(b"\n")
                if newline < 0:
                    break
                complete_lines.append(bytes(self._diagnostic_pending[: newline + 1]))
                del self._diagnostic_pending[: newline + 1]
            if len(self._diagnostic_pending) > 8192:
                complete_lines.append(b"[diagnostic line truncated and redacted]\n")
                self._diagnostic_pending.clear()
                self._diagnostic_discarding_line = not final
            elif final and self._diagnostic_pending:
                complete_lines.append(bytes(self._diagnostic_pending))
                self._diagnostic_pending.clear()
            for line in complete_lines:
                self._store_sanitized_diagnostic(line)

    def _store_sanitized_diagnostic(self, chunk: bytes) -> None:
        text = chunk.decode("utf-8", errors="replace")
        text = _SECRET_RE.sub(r"\1\2[redacted]", text)
        text = _URL_PASSWORD_RE.sub(r"\1[redacted]\2", text)
        text = "".join(character for character in text if character in "\n\r\t" or ord(character) >= 32)
        sanitized = text.encode("utf-8", errors="replace")
        self._diagnostics.extend(sanitized)
        if len(self._diagnostics) > _MAX_DIAGNOSTIC_BYTES:
            del self._diagnostics[: len(self._diagnostics) - _MAX_DIAGNOSTIC_BYTES]

    def _clear_diagnostics(self) -> None:
        with self._diagnostics_lock:
            self._diagnostics.clear()
            self._diagnostic_pending.clear()
            self._diagnostic_discarding_line = False

    @contextlib.asynccontextmanager
    async def _async_lifecycle_lock(self) -> Any:
        loop = asyncio.get_running_loop()
        acquire = loop.run_in_executor(None, self._lifecycle_lock.acquire)
        try:
            await asyncio.shield(acquire)
        except asyncio.CancelledError:
            await asyncio.shield(acquire)
            self._lifecycle_lock.release()
            raise
        try:
            yield
        finally:
            self._lifecycle_lock.release()

    def _atexit_stop(self) -> None:
        if self._process_kind == "sync" and self._owner_pid == os.getpid():
            with contextlib.suppress(Exception):
                self.stop()

