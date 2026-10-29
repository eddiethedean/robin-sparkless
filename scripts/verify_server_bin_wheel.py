"""Build-independent smoke test for an installed sparkless-server-bin wheel."""

from __future__ import annotations

import argparse
import importlib
import importlib.metadata
import json
import os
import selectors
import socket
import subprocess
import sys
import tempfile
import urllib.request
from pathlib import Path
from urllib.parse import urlsplit

from verify_server_http import verify_endpoint, verify_mutations

import tomllib


PACKAGE_NAME = "sparkless-server-bin"
CONTROL_PROTOCOL_VERSION = 2
CAPABILITIES_PROTOCOL_VERSION = 1
SPARK_CONNECT_PROTOCOL_VERSION = "4.0.5"
MAX_CONTROL_FRAME_BYTES = 16 * 1024


def _wheel_parts(path: Path) -> tuple[str, str, str]:
    if not path.is_file() or path.suffix != ".whl":
        raise ValueError("wheel must name an existing .whl file")
    parts = path.name[:-4].split("-")
    if len(parts) != 5:
        raise ValueError("wheel filename does not follow the five-field wheel format")
    distribution, version, _python, _abi, platform = parts
    if distribution.replace("_", "-").lower() != PACKAGE_NAME:
        raise ValueError("wheel is not the sparkless-server-bin distribution")
    return version, platform, distribution


def _read_frame(
    selector: selectors.BaseSelector, process: subprocess.Popen[str]
) -> dict:
    ready = selector.select(timeout=10)
    if not ready:
        raise RuntimeError("server control record timed out")
    assert process.stdout is not None
    frame = process.stdout.readline(MAX_CONTROL_FRAME_BYTES + 1)
    if not frame or len(frame.encode("utf-8")) > MAX_CONTROL_FRAME_BYTES:
        raise RuntimeError("server emitted an empty or oversized control record")
    try:
        value = json.loads(frame)
    except json.JSONDecodeError as error:
        raise RuntimeError("server emitted invalid control JSON") from error
    if not isinstance(value, dict):
        raise RuntimeError("server control record is not a JSON object")
    return value


def _smoke_installed_binary(
    expected_version: str,
    *,
    connect: bool = False,
    connect_client: bool = False,
    client_python: str | None = None,
) -> None:
    installed_version = importlib.metadata.version(PACKAGE_NAME)
    if installed_version != expected_version:
        raise RuntimeError(
            "installed wheel version {} does not match {}".format(
                installed_version, expected_version
            )
        )
    package = importlib.import_module("sparkless_server_bin")
    executable = Path(package.executable_path())
    verify_binary(
        executable,
        expected_version,
        connect=connect,
        connect_client=connect_client,
        client_python=client_python,
    )


def _verify_instance(
    executable: Path,
    expected_version: str,
    *,
    http: bool,
    connect: bool,
    connect_client: bool = False,
    client_python: str | None = None,
    mutations: bool = True,
) -> None:
    if not executable.is_file() or not os.access(executable, os.X_OK):
        raise RuntimeError("installed wheel does not contain an executable server")

    command = [str(executable), "control"]
    if http:
        command.extend(("--host", "127.0.0.1", "--port", "0"))
    else:
        command.append("--disable-http")
    if connect:
        command.extend(("--connect-host", "127.0.0.1", "--connect-port", "0"))
    else:
        command.append("--disable-connect")
    process = subprocess.Popen(
        command,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )
    selector = selectors.DefaultSelector()
    assert process.stdout is not None
    selector.register(process.stdout, selectors.EVENT_READ)
    try:
        ready = _read_frame(selector, process)
        if ready.get("type") != "ready":
            raise RuntimeError("installed server did not become ready")
        control_version = ready.get("control_protocol_version")
        if (
            type(control_version) is not int
            or control_version != CONTROL_PROTOCOL_VERSION
        ):
            raise RuntimeError("installed server control protocol is incompatible")
        ready_capabilities_version = ready.get("capabilities_protocol_version")
        if (
            type(ready_capabilities_version) is not int
            or ready_capabilities_version != CAPABILITIES_PROTOCOL_VERSION
        ):
            raise RuntimeError("installed server capabilities protocol is incompatible")
        if ready.get("server_version") != expected_version:
            raise RuntimeError("installed binary and wheel versions do not match")
        instance_id = ready.get("server_instance_id")
        capabilities = ready.get("capabilities")
        if not isinstance(instance_id, str) or not instance_id:
            raise RuntimeError("installed server did not identify its instance")
        if not isinstance(capabilities, dict):
            raise RuntimeError("installed server did not report capabilities")
        if capabilities.get("server_instance_id") != instance_id:
            raise RuntimeError("ready and capability instance identities differ")
        nested_capabilities_version = capabilities.get("capabilities_protocol_version")
        if (
            type(nested_capabilities_version) is not int
            or nested_capabilities_version != CAPABILITIES_PROTOCOL_VERSION
        ):
            raise RuntimeError("installed server capability framing is incompatible")
        if capabilities.get("transport") != ("http" if http else "spark_connect"):
            raise RuntimeError(
                "installed server reported an unexpected primary transport"
            )
        transports = capabilities.get("transports")
        if not isinstance(transports, list) or any(
            not isinstance(transport, str) for transport in transports
        ):
            raise RuntimeError("installed server reported invalid transports")
        expected_transports = ["http"] if http else []
        if connect:
            expected_transports.append("spark_connect")
        if transports != expected_transports:
            raise RuntimeError(
                "installed server advertised an unexpected transport set"
            )
        endpoint = ready.get("http_url")
        if http:
            if not isinstance(endpoint, str) or not endpoint.startswith(
                "http://127.0.0.1:"
            ):
                raise RuntimeError(
                    "installed server did not bind the requested loopback endpoint"
                )
            if capabilities.get("http_url") != endpoint or "http" not in transports:
                raise RuntimeError(
                    "HTTP endpoint differs between readiness and capabilities"
                )
            with urllib.request.urlopen(endpoint + "/health", timeout=5) as response:
                if response.status != 200:
                    raise RuntimeError("installed server health check failed")
            verify_endpoint(endpoint)
            if mutations:
                verify_mutations(endpoint)
        elif (
            endpoint is not None
            or capabilities.get("http_url") is not None
            or "http" in transports
        ):
            raise RuntimeError("Connect-only server advertised an HTTP endpoint")

        connect_url_value = ready.get("spark_connect_url")
        capability_connect_url = capabilities.get("spark_connect_url")
        supported = capabilities.get("spark_connect_supported")
        connect_url = None
        if connect:
            if not isinstance(connect_url_value, str):
                raise RuntimeError(
                    "installed server did not report a Spark Connect endpoint"
                )
            parsed_connect = urlsplit(connect_url_value)
            try:
                connect_port = parsed_connect.port
            except ValueError as error:
                raise RuntimeError(
                    "installed server reported an invalid Spark Connect URL"
                ) from error
            if (
                parsed_connect.scheme != "sc"
                or parsed_connect.hostname != "127.0.0.1"
                or connect_port is None
                or not 1 <= connect_port <= 65535
                or parsed_connect.path
                or parsed_connect.query
                or parsed_connect.fragment
            ):
                raise RuntimeError(
                    "installed server reported an invalid Spark Connect URL"
                )
            connect_url = connect_url_value
            if (
                supported is not True
                or capability_connect_url != connect_url
                or capabilities.get("spark_connect_protocol_version")
                != SPARK_CONNECT_PROTOCOL_VERSION
                or "spark_connect" not in transports
            ):
                raise RuntimeError(
                    "installed server advertised inconsistent Connect capabilities"
                )
            with socket.create_connection(
                (parsed_connect.hostname, connect_port), timeout=5
            ):
                pass
            if connect_client:
                root = Path(__file__).resolve().parents[1]
                environment = os.environ.copy()
                environment["SPARKLESS_CONNECT_URL"] = connect_url
                subprocess.run(
                    [
                        client_python or sys.executable,
                        str(root / "tests/connect/smoke_public_client.py"),
                    ],
                    check=True,
                    env=environment,
                    timeout=240,
                )
        elif (
            supported is not False
            or connect_url_value is not None
            or capability_connect_url is not None
            or capabilities.get("spark_connect_protocol_version") is not None
        ):
            raise RuntimeError("HTTP-only server advertised Spark Connect support")

        assert process.stdin is not None
        process.stdin.write(
            json.dumps(
                {
                    "control_protocol_version": CONTROL_PROTOCOL_VERSION,
                    "command": "shutdown",
                }
            )
            + "\n"
        )
        process.stdin.flush()
        stopped = _read_frame(selector, process)
        stopped_version = stopped.get("control_protocol_version")
        if (
            stopped.get("type") != "stopped"
            or type(stopped_version) is not int
            or stopped_version != CONTROL_PROTOCOL_VERSION
        ):
            raise RuntimeError("installed server did not acknowledge graceful shutdown")
        return_code = process.wait(timeout=10)
        if return_code != 0:
            raise RuntimeError(
                "installed server exited with status {}".format(return_code)
            )
        if endpoint is not None:
            parsed_http = urlsplit(endpoint)
            assert parsed_http.hostname is not None and parsed_http.port is not None
            try:
                socket.create_connection(
                    (parsed_http.hostname, parsed_http.port), timeout=0.2
                )
            except OSError:
                pass
            else:
                raise RuntimeError("HTTP listener remained open after shutdown")
        if connect_url is not None:
            parsed_connect = urlsplit(connect_url)
            assert (
                parsed_connect.hostname is not None and parsed_connect.port is not None
            )
            try:
                socket.create_connection(
                    (parsed_connect.hostname, parsed_connect.port), timeout=0.2
                )
            except OSError:
                pass
            else:
                raise RuntimeError(
                    "Spark Connect listener remained open after shutdown"
                )
    finally:
        selector.close()
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        if process.stderr is not None:
            stderr = process.stderr.read()
            if process.returncode not in (0, None):
                raise RuntimeError("installed server stderr: {}".format(stderr[-4096:]))


def _verify_connect_bind_failure(executable: Path, *, http: bool) -> None:
    with socket.socket() as occupied:
        occupied.bind(("127.0.0.1", 0))
        occupied.listen(1)
        host, port = occupied.getsockname()
        command = [str(executable), "control"]
        if http:
            command.extend(("--host", "127.0.0.1", "--port", "0"))
        else:
            command.append("--disable-http")
        command.extend(
            [
                "--connect-host",
                host,
                "--connect-port",
                str(port),
            ]
        )
        process = subprocess.Popen(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            stdout, _stderr = process.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.communicate()
            raise RuntimeError(
                "server did not fail readiness on a Connect port collision"
            )
        if process.returncode == 0 or stdout:
            raise RuntimeError(
                "server published readiness despite a Connect port collision"
            )


def verify_binary(
    executable: Path,
    expected_version: str,
    *,
    connect: bool = False,
    connect_client: bool = False,
    client_python: str | None = None,
    connect_only: bool = False,
) -> None:
    if connect_only and not connect:
        raise ValueError("Connect-only mode requires --connect")
    if connect_client and not connect:
        raise ValueError("--connect-client requires --connect")
    _verify_instance(
        executable,
        expected_version,
        http=not connect_only,
        connect=connect,
        connect_client=connect_client,
        client_python=client_python,
    )
    if connect:
        _verify_connect_bind_failure(executable, http=not connect_only)
        if not connect_only:
            _verify_instance(
                executable,
                expected_version,
                http=True,
                connect=False,
                mutations=False,
            )


def verify_wheel(
    wheel: Path,
    expected_platform: str,
    package_project: Path,
    *,
    connect: bool = False,
    connect_client: bool = False,
    client_python: str | None = None,
) -> None:
    wheel = wheel.resolve()
    expected_version = tomllib.loads(package_project.read_text())["project"]["version"]
    wheel_version, platform, _distribution = _wheel_parts(wheel)
    if wheel_version != expected_version:
        raise ValueError(
            "wheel version {} does not match package metadata {}".format(
                wheel_version, expected_version
            )
        )
    if platform != expected_platform:
        raise ValueError(
            "wheel platform {} does not match expected {}".format(
                platform, expected_platform
            )
        )

    with tempfile.TemporaryDirectory(prefix="sparkless-server-bin-") as temporary:
        venv_path = Path(temporary) / "venv"
        subprocess.run([sys.executable, "-m", "venv", str(venv_path)], check=True)
        python = venv_path / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
        subprocess.run(
            [str(python), "-m", "pip", "install", "--no-deps", str(wheel)],
            check=True,
        )
        smoke_command = [
            str(python),
            str(Path(__file__).resolve()),
            "--smoke-installed",
            expected_version,
        ]
        if connect:
            smoke_command.append("--connect")
        if connect_client:
            smoke_command.append("--connect-client")
        if client_python:
            smoke_command.extend(("--client-python", client_python))
        subprocess.run(smoke_command, check=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wheel", type=Path)
    parser.add_argument("--platform")
    parser.add_argument(
        "--project",
        type=Path,
        default=Path(__file__).resolve().parents[1]
        / "python/server-bin/pyproject.toml",
    )
    parser.add_argument("--smoke-installed", metavar="VERSION")
    parser.add_argument(
        "--connect",
        action="store_true",
        help="verify Connect and HTTP-only feature modes",
    )
    parser.add_argument(
        "--connect-only",
        action="store_true",
        help="verify a Connect-only native feature build (use with --binary)",
    )
    parser.add_argument(
        "--connect-client",
        action="store_true",
        help="run the public PySpark Connect client (requires --connect)",
    )
    parser.add_argument(
        "--client-python", help="Python executable with pyspark-client installed"
    )
    parser.add_argument(
        "--binary", type=Path, help="verify this exact native executable"
    )
    parser.add_argument("--version", help="expected native binary version")
    args = parser.parse_args()
    try:
        if args.connect_client and not (args.connect or args.connect_only):
            parser.error("--connect-client requires --connect or --connect-only")
        if args.binary is not None:
            if not args.version or args.wheel or args.smoke_installed:
                parser.error(
                    "--binary requires --version and cannot be combined with wheel modes"
                )
            verify_binary(
                args.binary.resolve(),
                args.version,
                connect=args.connect or args.connect_only,
                connect_client=args.connect_client,
                client_python=args.client_python,
                connect_only=args.connect_only,
            )
        elif args.smoke_installed is not None:
            if args.connect_only:
                parser.error("--connect-only is supported only with --binary")
            _smoke_installed_binary(
                args.smoke_installed,
                connect=args.connect,
                connect_client=args.connect_client,
                client_python=args.client_python,
            )
        else:
            if args.wheel is None or args.platform is None:
                parser.error("--wheel and --platform are required")
            if args.connect_only:
                parser.error("--connect-only is supported only with --binary")
            verify_wheel(
                args.wheel,
                args.platform,
                args.project,
                connect=args.connect,
                connect_client=args.connect_client,
                client_python=args.client_python,
            )
    except (OSError, ValueError, subprocess.SubprocessError, RuntimeError) as error:
        print("ERROR: {}".format(error), file=sys.stderr)
        return 1
    print(
        "server binary readiness, HTTP, optional Connect, port isolation, and shutdown checks passed"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
