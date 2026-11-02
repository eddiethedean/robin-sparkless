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
    product: str = "base",
    api_extra: bool = False,
    legacy_forwarder: bool = False,
) -> None:
    if mutations and protocol == "legacy-4.16":
        raise RuntimeError("mutation checks require the current server protocol")
    if protocol in ("legacy-4.16", "legacy-4.17"):
        _verify_legacy_server_extra(expected_version, mutations)
        return
    if product not in ("base", "duckdb"):
        raise ValueError("product must be 'base' or 'duckdb'")
    package_name = "sparkless-server" if product == "base" else "sparkless-server-duckdb"
    module_name = "sparkless_server" if product == "base" else "sparkless_server_duckdb"
    product_module = __import__(module_name, fromlist=["SparklessServer"])
    AsyncSparklessServer = product_module.AsyncSparklessServer
    SparklessServer = product_module.SparklessServer
    UnsupportedSparkConnectError = product_module.UnsupportedSparkConnectError
    executable_path = product_module.executable_path

    for distribution, expected in (
        (package_name, expected_version),
        ("sparkless-connect", _engine_version()),
    ):
        actual = importlib.metadata.version(distribution)
        if actual != expected:
            raise RuntimeError(
                "{} version {} does not match {}".format(distribution, actual, expected)
            )
    if api_extra:
        import sparkless

        if sparkless.__version__ != expected_version:
            raise RuntimeError("sparkless.__version__ does not match installed metadata")
        if importlib.metadata.version("sparkless-native") != _engine_version():
            raise RuntimeError("sparkless API extra did not install its native engine")
        if legacy_forwarder:
            _verify_legacy_forwarder(expected_version)
    else:
        installed = {item.metadata["Name"].lower().replace("_", "-") for item in importlib.metadata.distributions()}
        for forbidden in ("sparkless", "sparkless-native"):
            if forbidden in installed:
                raise RuntimeError("server-only install unexpectedly includes {}".format(forbidden))
        if "sparkless_native" in sys.modules:
            raise RuntimeError("importing a server-only product loaded the local native engine")

    binary = Path(executable_path())
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise RuntimeError("server extra does not provide an executable binary")
    if product == "base" and hasattr(product_module, "quack_extension_path"):
        raise RuntimeError("base product unexpectedly exposes a Quack extension accessor")
    if product == "duckdb":
        extension = Path(product_module.quack_extension_path())
        if not extension.is_file() or extension.stat().st_size == 0:
            raise RuntimeError("DuckDB product does not provide its matching Quack extension")

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

    def require_product_record(launcher: object) -> None:
        record = getattr(launcher, "_ready_record", None)
        capabilities = record.get("capabilities") if isinstance(record, dict) else None
        expected_identity = "base" if product == "base" else "duckdb"
        sources = capabilities.get("data_sources") if isinstance(capabilities, dict) else None
        if (
            not isinstance(record, dict)
            or record.get("server_product") != expected_identity
            or not isinstance(capabilities, dict)
            or capabilities.get("server_product") != expected_identity
            or capabilities.get("execution_backend") != "polars"
            or not isinstance(sources, list)
            or ((product == "duckdb") != ("duckdb" in sources))
        ):
            raise RuntimeError("selected launcher readiness reported the wrong product capabilities")

    server = SparklessServer(startup_timeout=30, shutdown_timeout=15, kill_timeout=5)
    server.start()
    first_identity = server.server_instance_id
    try:
        require_product_record(server)
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
        require_product_record(server)
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
            require_product_record(async_server)
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


def _installed_distributions() -> set[str]:
    return {
        item.metadata["Name"].lower().replace("_", "-")
        for item in importlib.metadata.distributions()
        if item.metadata.get("Name")
    }


def verify_client_only(expected_version: str) -> None:
    """Check that the remote launcher/client install carries no local engine or server."""
    actual = importlib.metadata.version("sparkless-connect")
    if actual != expected_version:
        raise RuntimeError("sparkless-connect version does not match the candidate")
    installed = _installed_distributions()
    forbidden = {
        "sparkless",
        "sparkless-native",
        "sparkless-server",
        "sparkless-server-duckdb",
        "sparkless-server-bin",
    }
    present = sorted(forbidden & installed)
    if present:
        raise RuntimeError("client-only install unexpectedly includes {}".format(present))
    from sparkless_connect.server import SparklessServer

    if SparklessServer(product="duckdb").product != "duckdb":
        raise RuntimeError("common launcher cannot select a requested product")
    if "pyspark" in sys.modules:
        raise RuntimeError("importing the remote launcher eagerly imported PySpark")
    if "sparkless_native" in sys.modules:
        raise RuntimeError("importing the remote launcher loaded the local native engine")


def verify_local_only(expected_version: str) -> None:
    """Check that the local API wheel does not pull in server distributions."""
    if importlib.metadata.version("sparkless") != expected_version:
        raise RuntimeError("sparkless version does not match the candidate")
    if importlib.metadata.version("sparkless-native") != _engine_version():
        raise RuntimeError("sparkless-native version does not match the candidate")
    installed = _installed_distributions()
    forbidden = {
        "sparkless-connect",
        "sparkless-server",
        "sparkless-server-duckdb",
        "sparkless-server-bin",
    }
    present = sorted(forbidden & installed)
    if present:
        raise RuntimeError("local-only install unexpectedly includes {}".format(present))
    import sparkless  # noqa: F401

    try:
        from sparkless.server import SparklessServer  # noqa: F401
    except ImportError as error:
        if "sparkless-connect" not in str(error):
            raise RuntimeError("local server compatibility import lacks an install hint") from error
    else:
        raise RuntimeError("local-only install unexpectedly exposes server helpers")


def verify_coinstallation(expected_version: str) -> None:
    """Start both server products from one clean server-only environment."""
    for distribution in (
        "sparkless-connect",
        "sparkless-server",
        "sparkless-server-duckdb",
    ):
        if importlib.metadata.version(distribution) != expected_version:
            raise RuntimeError("{} version does not match the candidate".format(distribution))
    installed = _installed_distributions()
    forbidden = {"sparkless", "sparkless-native", "sparkless-server-bin"}
    present = sorted(forbidden & installed)
    if present:
        raise RuntimeError("server coinstall unexpectedly includes {}".format(present))

    import sparkless_server
    import sparkless_server_duckdb
    from sparkless_connect import (
        SparklessServer as CommonSparklessServer,
        SparklessServerError,
    )

    if sparkless_server.SparklessServerError is not SparklessServerError:
        raise RuntimeError("base product exported a different server exception class")
    if sparkless_server_duckdb.SparklessServerError is not SparklessServerError:
        raise RuntimeError("DuckDB product exported a different server exception class")
    if sparkless_server.SparklessConnect.server_class is not sparkless_server.SparklessServer:
        raise RuntimeError("base SparklessConnect helper is not product-bound")
    if sparkless_server_duckdb.SparklessConnect.server_class is not sparkless_server_duckdb.SparklessServer:
        raise RuntimeError("DuckDB SparklessConnect helper is not product-bound")
    if hasattr(sparkless_server, "quack_extension_path"):
        raise RuntimeError("base product unexpectedly exposes the Quack extension")
    if not Path(sparkless_server_duckdb.quack_extension_path()).is_file():
        raise RuntimeError("DuckDB product has no packaged Quack extension")

    servers = [
        CommonSparklessServer(startup_timeout=30, shutdown_timeout=15, kill_timeout=5),
        CommonSparklessServer(
            product="duckdb", startup_timeout=30, shutdown_timeout=15, kill_timeout=5
        ),
    ]
    expected_products = ("base", "duckdb")
    try:
        for server, product in zip(servers, expected_products, strict=True):
            server.start()
            record = server._ready_record
            capabilities = record.get("capabilities") if isinstance(record, dict) else None
            if (
                not isinstance(record, dict)
                or record.get("server_product") != product
                or record.get("execution_backend") != "polars"
                or not isinstance(capabilities, dict)
                or capabilities.get("server_product") != product
                or capabilities.get("execution_backend") != "polars"
                or not isinstance(capabilities.get("data_sources"), list)
                or (product == "duckdb") != ("duckdb" in capabilities["data_sources"])
            ):
                raise RuntimeError("coinstalled launcher selected the wrong product")
            verify_endpoint(server.http_url)
        if servers[0].server_instance_id == servers[1].server_instance_id:
            raise RuntimeError("coinstalled server products reused an instance identity")
    except BaseException:
        for server in reversed(servers):
            if server.is_running:
                if server.diagnostics:
                    print(server.diagnostics[-4096:], file=sys.stderr)
                server.stop()
        raise
    else:
        for server in reversed(servers):
            server.stop()
    if "pyspark" in sys.modules or "sparkless_native" in sys.modules:
        raise RuntimeError("starting server products imported PySpark or the local engine")


def _verify_legacy_forwarder(expected_version: str) -> None:
    import sparkless_server
    from sparkless.server import SparklessServer as LegacySparklessServer
    from sparkless.server import SparklessServerError
    from sparkless_server_bin import executable_path, quack_extension_path

    if importlib.metadata.version("sparkless-server-bin") != expected_version:
        raise RuntimeError("sparkless-server-bin version does not match the candidate")
    if Path(executable_path()).resolve() != Path(sparkless_server.executable_path()).resolve():
        raise RuntimeError("legacy executable accessor does not forward to the base product")
    if LegacySparklessServer().product != "base":
        raise RuntimeError("legacy sparkless.server helper no longer defaults to base")
    if SparklessServerError is not sparkless_server.SparklessServerError:
        raise RuntimeError("legacy facade exported a different server exception class")
    try:
        quack_extension_path()
    except FileNotFoundError as error:
        if "sparkless-server-duckdb" not in str(error):
            raise RuntimeError("legacy Quack accessor lacks a DuckDB install hint") from error
    else:
        raise RuntimeError("legacy base install unexpectedly supplied Quack")


def _engine_version() -> str:
    """The API pins its independently versioned native/server components exactly."""
    import tomllib

    root = Path(__file__).resolve().parents[1]
    return tomllib.loads((root / "Cargo.toml").read_text())["package"]["version"]


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
    parser.add_argument("--product", choices=("base", "duckdb"), default="base")
    parser.add_argument(
        "--profile",
        choices=("server", "coinstall", "client-only", "local-only"),
        default="server",
        help="install profile to inspect; server runs the selected product smoke",
    )
    parser.add_argument(
        "--api-extra",
        action="store_true",
        help="also require the local API and native engine installed by sparkless extras",
    )
    parser.add_argument(
        "--legacy-forwarder",
        action="store_true",
        help="check legacy server-bin and sparkless.server forwarding (requires --api-extra)",
    )
    parser.add_argument(
        "--protocol",
        choices=("current", "legacy-4.16", "legacy-4.17"),
        default="current",
        help="control protocol expected from the packaged server binary",
    )
    args = parser.parse_args()
    try:
        if args.legacy_forwarder and not args.api_extra:
            raise RuntimeError("--legacy-forwarder requires --api-extra")
        if args.profile != "server" and (
            args.connect_client or args.api_extra or args.legacy_forwarder
        ):
            raise RuntimeError("client/local/coinstall profiles cannot use server-only options")
        if args.profile == "client-only":
            verify_client_only(args.version)
        elif args.profile == "local-only":
            verify_local_only(args.version)
        elif args.profile == "coinstall":
            verify_coinstallation(args.version)
        elif args.connect_client and args.protocol != "current":
            raise RuntimeError(
                "the public Connect client requires the current control protocol"
            )
        else:
            verify(
                args.version,
                args.protocol,
                args.mutations,
                args.connect_client,
                args.client_python,
                args.product,
                args.api_extra,
                args.legacy_forwarder,
            )
    except (OSError, RuntimeError, importlib.metadata.PackageNotFoundError) as error:
        print("ERROR: {}".format(error), file=sys.stderr)
        return 1
    print("{} install profile verified".format(args.profile))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
