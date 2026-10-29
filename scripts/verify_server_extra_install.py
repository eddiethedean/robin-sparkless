"""Smoke-test a freshly installed sparkless[server] consumer environment."""

from __future__ import annotations

import argparse
import asyncio
import importlib.metadata
import os
import socket
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlsplit

from verify_server_http import verify_endpoint, verify_mutations


def verify(
    expected_version: str,
    protocol: str = "current",
    mutations: bool = False,
    connect_client: bool = False,
    client_python: str | None = None,
) -> None:
    if mutations and protocol == "legacy-4.16":
        raise RuntimeError("mutation checks require the current server protocol")
    if protocol in ("legacy-4.16", "legacy-4.17"):
        _verify_legacy_server_extra(expected_version, mutations)
        return
    import sparkless
    from sparkless.server import (
        AsyncSparklessServer,
        SparklessServer,
        UnsupportedSparkConnectError,
    )
    from sparkless_server_bin import executable_path

    for distribution in ("sparkless", "sparkless-server-bin"):
        actual = importlib.metadata.version(distribution)
        if actual != expected_version:
            raise RuntimeError(
                "{} version {} does not match {}".format(
                    distribution, actual, expected_version
                )
            )
    if sparkless.__version__ != expected_version:
        raise RuntimeError("sparkless.__version__ does not match installed metadata")

    binary = Path(executable_path())
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise RuntimeError("server extra does not provide an executable binary")

    def require_no_pyspark_import() -> None:
        if "pyspark" in sys.modules:
            raise RuntimeError("starting the native launcher imported PySpark")

    def exercise_client(connect_url: str) -> None:
        environment = os.environ.copy()
        environment["SPARKLESS_CONNECT_URL"] = connect_url
        subprocess.run(
            [
                client_python or sys.executable,
                str(
                    Path(__file__).resolve().parents[1]
                    / "tests/connect/smoke_public_client.py"
                ),
            ],
            check=True,
            env=environment,
            timeout=240,
        )

    def verify_connect_socket(connect_url: str) -> None:
        parsed = urlsplit(connect_url)
        try:
            port = parsed.port
        except ValueError as error:
            raise RuntimeError(
                "launcher returned an invalid Spark Connect URL"
            ) from error
        if parsed.scheme != "sc" or not parsed.hostname or port is None:
            raise RuntimeError("launcher returned an invalid Spark Connect URL")
        with socket.create_connection((parsed.hostname, port), timeout=5):
            pass

    def require_http_only_record(launcher: object) -> None:
        record = getattr(launcher, "_ready_record", None)
        capabilities = record.get("capabilities") if isinstance(record, dict) else None
        if (
            not isinstance(record, dict)
            or not isinstance(capabilities, dict)
            or record.get("spark_connect_url") is not None
            or capabilities.get("spark_connect_supported") is not False
            or capabilities.get("spark_connect_url") is not None
            or capabilities.get("transports") != ["http"]
        ):
            raise RuntimeError("HTTP-only launcher readiness advertised Spark Connect")

    server = SparklessServer(startup_timeout=30, shutdown_timeout=15, kill_timeout=5)
    server.start()
    first_identity = server.server_instance_id
    try:
        if protocol == "current" and not server.server_instance_id:
            raise RuntimeError("owned server did not report an instance identity")
        require_no_pyspark_import()
        verify_endpoint(server.http_url)
        connect_url = server.spark_connect_url
        verify_connect_socket(connect_url)
        if mutations:
            verify_mutations(server.http_url)
        if connect_client:
            exercise_client(connect_url)
    except BaseException:
        if server.diagnostics:
            print(server.diagnostics[-4096:], file=sys.stderr)
        raise
    finally:
        server.stop()

    server.start()
    try:
        if protocol == "current" and not server.server_instance_id:
            raise RuntimeError("restarted server did not report an instance identity")
        if protocol == "current" and server.server_instance_id == first_identity:
            raise RuntimeError("server restart reused the previous instance identity")
        verify_connect_socket(server.spark_connect_url)
        verify_endpoint(server.http_url)
    finally:
        server.stop()

    http_only = SparklessServer(
        startup_timeout=30,
        shutdown_timeout=15,
        kill_timeout=5,
        connect_port=None,
    )
    http_only.start()
    try:
        verify_endpoint(http_only.http_url)
        require_http_only_record(http_only)
        try:
            _ = http_only.spark_connect_url
        except UnsupportedSparkConnectError:
            pass
        else:
            raise RuntimeError("HTTP-only launcher exposed a Spark Connect URL")
    finally:
        http_only.stop()

    async def verify_async_launchers() -> None:
        async_server = AsyncSparklessServer(
            startup_timeout=30, shutdown_timeout=15, kill_timeout=5
        )
        await async_server.start()
        try:
            require_no_pyspark_import()
            verify_endpoint(async_server.http_url)
            verify_connect_socket(async_server.spark_connect_url)
        finally:
            await async_server.stop()

        async_http_only = AsyncSparklessServer(
            startup_timeout=30,
            shutdown_timeout=15,
            kill_timeout=5,
            connect_port=None,
        )
        await async_http_only.start()
        try:
            verify_endpoint(async_http_only.http_url)
            require_http_only_record(async_http_only)
            try:
                _ = async_http_only.spark_connect_url
            except UnsupportedSparkConnectError:
                pass
            else:
                raise RuntimeError("async HTTP-only launcher exposed a Connect URL")
        finally:
            await async_http_only.stop()

    asyncio.run(verify_async_launchers())


def _verify_legacy_server_extra(expected_version: str, mutations: bool) -> None:
    """Keep the guarded 4.16/4.17 recovery paths compatible with their HTTP binaries."""
    import sparkless
    from sparkless.server import SparklessServer
    from sparkless_server_bin import executable_path

    for distribution in ("sparkless", "sparkless-server-bin"):
        actual = importlib.metadata.version(distribution)
        if actual != expected_version:
            raise RuntimeError(
                "{} version {} does not match {}".format(
                    distribution, actual, expected_version
                )
            )
    if sparkless.__version__ != expected_version:
        raise RuntimeError("sparkless.__version__ does not match installed metadata")
    binary = Path(executable_path())
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise RuntimeError("server extra does not provide an executable binary")

    server = SparklessServer(startup_timeout=30, shutdown_timeout=15, kill_timeout=5)
    server.start()
    try:
        verify_endpoint(server.http_url)
        if mutations:
            verify_mutations(server.http_url)
    finally:
        server.stop()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("version", help="expected coordinated release version")
    parser.add_argument(
        "--mutations", action="store_true", help="verify Phase 1 HTTP mutation outcomes"
    )
    parser.add_argument(
        "--connect-client",
        action="store_true",
        help="run the pinned public client through the installed launcher",
    )
    parser.add_argument(
        "--client-python", help="Python executable with pyspark-client installed"
    )
    parser.add_argument(
        "--protocol",
        choices=("current", "legacy-4.16", "legacy-4.17"),
        default="current",
        help="control protocol expected from the packaged server binary",
    )
    args = parser.parse_args()
    try:
        if args.connect_client and args.protocol != "current":
            raise RuntimeError(
                "the public Connect client requires the current control protocol"
            )
        verify(
            args.version,
            args.protocol,
            args.mutations,
            args.connect_client,
            args.client_python,
        )
    except (OSError, RuntimeError, importlib.metadata.PackageNotFoundError) as error:
        print("ERROR: {}".format(error), file=sys.stderr)
        return 1
    print("fresh sparkless[server] install launched and queried its packaged binary")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
