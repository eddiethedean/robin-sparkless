"""Shared process, control-protocol, and readiness primitives for the launcher."""

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
from typing import Any, Callable, Deque, Dict, Mapping, Optional, Tuple, cast
from urllib.parse import urlsplit

CONTROL_PROTOCOL_VERSION = 2
CAPABILITIES_PROTOCOL_VERSION = 1
SPARK_CONNECT_PROTOCOL_VERSION = "4.0.5"
MAX_CONTROL_FRAME_BYTES = 16 * 1024
MAX_DIAGNOSTIC_BYTES = 64 * 1024


class SparklessServerError(RuntimeError):
    """Raised when the owned native service cannot satisfy its contract."""


class UnsupportedServerPlatformError(SparklessServerError):
    """Raised when no packaged server executable exists for this platform."""


class UnsupportedSparkConnectError(SparklessServerError):
    """Raised when Spark Connect is disabled or unavailable in the server binary."""


def _product_package(product: str) -> Tuple[str, str]:
    if product == "base":
        return "sparkless_server", "sparkless-server"
    if product == "duckdb":
        return "sparkless_server_duckdb", "sparkless-server-duckdb"
    raise ValueError("product must be 'base' or 'duckdb'")


def _installed_product_version(product: str) -> Optional[str]:
    _, distribution = _product_package(product)
    try:
        return distribution_version(distribution)
    except PackageNotFoundError:
        return None


def _resolve_executable(explicit: Optional[str], product: str = "base") -> str:
    if explicit is not None:
        path = os.path.abspath(os.fspath(explicit))
        if not os.path.isfile(path) or not os.access(path, os.X_OK):
            raise SparklessServerError(
                "server executable path must name an existing executable file"
            )
        return path

    module_name, distribution = _product_package(product)
    install_hint = (
        "sparkless-server" if product == "base" else "sparkless-server-duckdb"
    )
    try:
        binary_package = importlib.import_module(module_name)
    except ImportError as error:
        raise UnsupportedServerPlatformError(
            "the {} server binary is not installed; install {} "
            "or pass executable_path explicitly".format(product, install_hint)
        ) from error
    binary_package_version = _installed_product_version(product)
    if binary_package_version is None:
        raise UnsupportedServerPlatformError(
            "{} is not installed as a package".format(install_hint)
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


def _command_with_connect(
    executable: str,
    host: str,
    port: int,
    connect_host: str,
    connect_port: Optional[int],
) -> Tuple[str, ...]:
    command = list(_command(executable, host, port))
    if connect_port is None:
        command.append("--disable-connect")
        return tuple(command)
    if not isinstance(connect_host, str) or not connect_host:
        raise ValueError("connect_host must be a non-empty IP address")
    try:
        ipaddress.ip_address(connect_host)
    except ValueError as error:
        raise ValueError("connect_host must be an IP address") from error
    if type(connect_port) is not int or not 0 <= connect_port <= 65535:
        raise ValueError("connect_port must be an integer in 0..=65535 or None")
    command.extend(
        ("--connect-host", connect_host, "--connect-port", str(connect_port))
    )
    return tuple(command)


def _binary_lacks_spark_connect(diagnostics: str) -> bool:
    return (
        "--connect-host requires the server connect feature" in diagnostics
        or "--connect-port requires the server connect feature" in diagnostics
    )


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


def _validate_ready(
    record: Dict[str, Any],
    require_spark_connect: bool = True,
    expected_product: str = "base",
    expected_backend: str = "polars",
) -> Tuple[str, Optional[str]]:
    if record.get("type") != "ready":
        raise SparklessServerError("server did not emit a readiness record")
    protocol_version = record.get("control_protocol_version")
    if (
        type(protocol_version) is not int
        or protocol_version != CONTROL_PROTOCOL_VERSION
    ):
        raise SparklessServerError("server control protocol version is unsupported")
    capabilities_protocol_version = record.get("capabilities_protocol_version")
    if (
        type(capabilities_protocol_version) is not int
        or capabilities_protocol_version != CAPABILITIES_PROTOCOL_VERSION
    ):
        raise SparklessServerError(
            "server capabilities protocol version is unsupported"
        )
    server_version = record.get("server_version")
    if not isinstance(server_version, str) or not server_version:
        raise SparklessServerError("server readiness record has no server version")
    expected_server_version = _installed_product_version(expected_product)
    reported_product = record.get("server_product")
    if reported_product != expected_product:
        raise SparklessServerError(
            "selected {} launcher received a '{}' server binary".format(
                expected_product, reported_product or "legacy/unknown"
            )
        )
    if expected_backend not in {"polars", "duckdb"}:
        raise SparklessServerError(
            "selected execution backend must be 'polars' or 'duckdb'"
        )
    if record.get("execution_backend") != expected_backend:
        raise SparklessServerError(
            "server execution backend is {}, expected {}".format(
                record.get("execution_backend") or "unknown", expected_backend
            )
        )
    if (
        expected_server_version is not None
        and server_version != expected_server_version
    ):
        raise SparklessServerError(
            "server binary version {} does not match installed {} {}".format(
                server_version,
                _product_package(expected_product)[1],
                expected_server_version,
            )
        )
    instance_id = record.get("server_instance_id")
    if not isinstance(instance_id, str) or not instance_id:
        raise SparklessServerError("server readiness record has no instance identity")
    capabilities = record.get("capabilities")
    if not isinstance(capabilities, dict) or capabilities.get("transport") != "http":
        raise SparklessServerError("server readiness record has invalid capabilities")
    nested_capabilities_version = capabilities.get("capabilities_protocol_version")
    if (
        type(nested_capabilities_version) is not int
        or nested_capabilities_version != CAPABILITIES_PROTOCOL_VERSION
    ):
        raise SparklessServerError("server readiness record has invalid capabilities")
    if capabilities.get("server_instance_id") != instance_id:
        raise SparklessServerError(
            "server capability identity does not match readiness"
        )
    if capabilities.get("server_product") != expected_product:
        raise SparklessServerError(
            "server product differs between readiness and capabilities"
        )
    if capabilities.get("execution_backend") != expected_backend:
        raise SparklessServerError(
            "server capabilities do not identify the selected execution backend"
        )
    sources = capabilities.get("data_sources")
    if not isinstance(sources, list) or any(
        not isinstance(item, str) for item in sources
    ):
        raise SparklessServerError(
            "server readiness record has invalid data-source capabilities"
        )
    owner_modes = capabilities.get("duckdb_owner_modes")
    if not isinstance(owner_modes, list) or any(
        not isinstance(item, str) for item in owner_modes
    ):
        raise SparklessServerError(
            "server readiness record has invalid DuckDB owner capabilities"
        )
    if expected_product == "duckdb" and "duckdb" not in sources:
        raise SparklessServerError(
            "DuckDB server product does not advertise DuckDB sources"
        )
    if expected_product == "base" and ("duckdb" in sources or owner_modes):
        raise SparklessServerError(
            "base server product must not advertise DuckDB sources"
        )
    if expected_product == "duckdb" and not {
        "standalone-read-only",
        "shared-quack-owner",
    }.issubset(owner_modes):
        raise SparklessServerError(
            "DuckDB server product has incomplete source-owner capabilities"
        )
    url = _validate_endpoint(record.get("http_url"), "http", "HTTP")
    if capabilities.get("http_url") != url:
        raise SparklessServerError(
            "HTTP endpoint differs between readiness and capabilities"
        )
    transports = capabilities.get("transports")
    if not isinstance(transports, list) or any(
        not isinstance(item, str) for item in transports
    ):
        raise SparklessServerError("server readiness record has invalid transports")
    connect_supported = capabilities.get("spark_connect_supported")
    if type(connect_supported) is not bool:
        raise SparklessServerError("server did not identify Spark Connect support")
    connect_url_value = record.get("spark_connect_url")
    capability_connect_url = capabilities.get("spark_connect_url")
    if connect_supported:
        if set(transports) != {"http", "spark_connect"} or len(transports) != 2:
            raise SparklessServerError(
                "server reported an unexpected Spark Connect transport set"
            )
        connect_url = _validate_endpoint(connect_url_value, "sc", "Spark Connect")
        if capability_connect_url != connect_url:
            raise SparklessServerError(
                "Spark Connect endpoint differs between readiness and capabilities"
            )
        if (
            capabilities.get("spark_connect_protocol_version")
            != SPARK_CONNECT_PROTOCOL_VERSION
        ):
            raise SparklessServerError(
                "server Spark Connect protocol version is unsupported"
            )
    else:
        connect_url = None
        if transports != ["http"]:
            raise SparklessServerError(
                "server reported an unexpected HTTP-only transport set"
            )
        if capabilities.get("spark_connect_protocol_version") is not None:
            raise SparklessServerError(
                "server reported a Spark Connect protocol without support"
            )
        if connect_url_value is not None or capability_connect_url is not None:
            raise SparklessServerError(
                "server advertised a Spark Connect endpoint without support"
            )
    if require_spark_connect and connect_url is None:
        raise UnsupportedSparkConnectError(
            "server binary does not provide a ready Spark Connect endpoint"
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
    return url, connect_url


def _validate_endpoint(value: Any, scheme: str, label: str) -> str:
    if not isinstance(value, str):
        raise SparklessServerError(
            "server readiness record has no {} endpoint".format(label)
        )
    parsed_url = urlsplit(value)
    try:
        endpoint_port = parsed_url.port
    except ValueError as error:
        raise SparklessServerError(
            "server readiness record has an invalid {} endpoint".format(label)
        ) from error
    if (
        parsed_url.scheme != scheme
        or not parsed_url.hostname
        or endpoint_port is None
        or not 1 <= endpoint_port <= 65535
        or parsed_url.username is not None
        or parsed_url.password is not None
        or parsed_url.path
        or parsed_url.query
        or parsed_url.fragment
    ):
        raise SparklessServerError(
            "server readiness record has an invalid {} endpoint".format(label)
        )
    return value


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


async def _await_cleanup(task: "asyncio.Task[Any]") -> Any:
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
            return task.result()
        except BaseException:
            pass


__all__ = [
    "Any",
    "CAPABILITIES_PROTOCOL_VERSION",
    "CONTROL_PROTOCOL_VERSION",
    "Callable",
    "Deque",
    "Dict",
    "MAX_CONTROL_FRAME_BYTES",
    "MAX_DIAGNOSTIC_BYTES",
    "Mapping",
    "Optional",
    "PackageNotFoundError",
    "SPARK_CONNECT_PROTOCOL_VERSION",
    "SparklessServerError",
    "Tuple",
    "UnsupportedServerPlatformError",
    "UnsupportedSparkConnectError",
    "_ByteTail",
    "_await_cleanup",
    "_binary_lacks_spark_connect",
    "_command",
    "_command_with_connect",
    "_drain_sync_stderr",
    "_installed_product_version",
    "_parse_record",
    "_product_package",
    "_read_sync_frames",
    "_resolve_executable",
    "_validate_endpoint",
    "_validate_ready",
    "_validate_timeouts",
    "asyncio",
    "cast",
    "deque",
    "distribution_version",
    "importlib",
    "ipaddress",
    "json",
    "math",
    "os",
    "queue",
    "subprocess",
    "threading",
    "urlsplit",
]
