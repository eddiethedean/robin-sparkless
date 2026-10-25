"""Build-independent smoke test for an installed sparkless-server-bin wheel."""

from __future__ import annotations

import argparse
import importlib
import importlib.metadata
import json
import os
import selectors
import subprocess
import sys
import tempfile
import urllib.request
import uuid
from pathlib import Path

try:
    import tomllib
except ImportError:  # Python 3.8–3.10 developer environments.
    import tomli as tomllib


PACKAGE_NAME = "sparkless-server-bin"
CONTROL_PROTOCOL_VERSION = 1
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


def _smoke_installed_binary(expected_version: str, protocol: str = "current") -> None:
    installed_version = importlib.metadata.version(PACKAGE_NAME)
    if installed_version != expected_version:
        raise RuntimeError(
            "installed wheel version {} does not match {}".format(
                installed_version, expected_version
            )
        )
    package = importlib.import_module("sparkless_server_bin")
    executable = Path(package.executable_path())
    if not executable.is_file() or not os.access(executable, os.X_OK):
        raise RuntimeError("installed wheel does not contain an executable server")

    launch_id = str(uuid.uuid4())
    if protocol == "legacy-4.16":
        command = [
            str(executable),
            "serve",
            "--bind",
            "127.0.0.1",
            "--port",
            "0",
            "--control",
            "--launch-id",
            launch_id,
        ]
    else:
        command = [
            str(executable),
            "control",
            "--host",
            "127.0.0.1",
            "--port",
            "0",
        ]
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
        if protocol == "legacy-4.16":
            if ready.get("protocol_version") != CONTROL_PROTOCOL_VERSION:
                raise RuntimeError("installed server control protocol is incompatible")
            if ready.get("agent_version") != 1:
                raise RuntimeError("installed server HTTP agent protocol is incompatible")
            if ready.get("binary_version") != expected_version:
                raise RuntimeError("installed binary and wheel versions do not match")
            if ready.get("launch_id") != launch_id:
                raise RuntimeError("installed server readiness belongs to another launch")
            capabilities = ready.get("capabilities")
            if not isinstance(capabilities, list) or not {
                "http_agent_v1",
                "read_only_sql",
                "typed_scalar_results",
            }.issubset(capabilities):
                raise RuntimeError("installed server did not report required capabilities")
        else:
            if ready.get("control_protocol_version") != CONTROL_PROTOCOL_VERSION:
                raise RuntimeError("installed server control protocol is incompatible")
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
        endpoint = ready.get("http_url")
        if not isinstance(endpoint, str) or not endpoint.startswith(
            "http://127.0.0.1:"
        ):
            raise RuntimeError(
                "installed server did not bind the requested loopback endpoint"
            )
        with urllib.request.urlopen(endpoint + "/health", timeout=5) as response:
            if response.status != 200:
                raise RuntimeError("installed server health check failed")

        assert process.stdin is not None
        if protocol == "legacy-4.16":
            shutdown_request = {"type": "shutdown", "launch_id": launch_id}
        else:
            shutdown_request = {
                "control_protocol_version": CONTROL_PROTOCOL_VERSION,
                "command": "shutdown",
            }
        process.stdin.write(json.dumps(shutdown_request) + "\n")
        process.stdin.flush()
        stopped = _read_frame(selector, process)
        if stopped.get("type") != "stopped":
            raise RuntimeError("installed server did not acknowledge graceful shutdown")
        if protocol == "legacy-4.16" and (
            stopped.get("protocol_version") != CONTROL_PROTOCOL_VERSION
            or stopped.get("launch_id") != launch_id
        ):
            raise RuntimeError("installed server shutdown acknowledgement is invalid")
        return_code = process.wait(timeout=10)
        if return_code != 0:
            raise RuntimeError(
                "installed server exited with status {}".format(return_code)
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


def verify_wheel(
    wheel: Path, expected_platform: str, package_project: Path, protocol: str
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
        subprocess.run(
            [
                str(python),
                str(Path(__file__).resolve()),
                "--smoke-installed",
                expected_version,
                "--protocol",
                protocol,
            ],
            check=True,
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wheel", type=Path)
    parser.add_argument("--platform")
    parser.add_argument(
        "--protocol",
        choices=("current", "legacy-4.16"),
        default="current",
        help="control protocol expected from the packaged server binary",
    )
    parser.add_argument(
        "--project",
        type=Path,
        default=Path(__file__).resolve().parents[1]
        / "python/server-bin/pyproject.toml",
    )
    parser.add_argument("--smoke-installed", metavar="VERSION")
    args = parser.parse_args()
    try:
        if args.smoke_installed is not None:
            _smoke_installed_binary(args.smoke_installed, args.protocol)
        else:
            if args.wheel is None or args.platform is None:
                parser.error("--wheel and --platform are required")
            verify_wheel(args.wheel, args.platform, args.project, args.protocol)
    except (OSError, ValueError, subprocess.SubprocessError, RuntimeError) as error:
        print("ERROR: {}".format(error), file=sys.stderr)
        return 1
    print("server binary wheel platform, version, startup, health, and shutdown passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
