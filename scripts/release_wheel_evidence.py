"""Write digest-bound evidence for an installed Sparkless release wheel."""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import re
import tempfile
from typing import Any


def canonical_machine(value: str) -> str:
    normalized = value.lower().replace("-", "_")
    if normalized in {"amd64", "x86_64", "x64"}:
        return "x86_64"
    if normalized in {"arm64", "aarch64"}:
        return "aarch64"
    return normalized


def wheel_sha256(wheel: Path) -> str:
    digest = hashlib.sha256()
    with wheel.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def build_evidence(
    *, wheel: Path, expected_sha256: str, suite: str, target: str
) -> dict[str, Any]:
    if not wheel.is_file() or wheel.suffix != ".whl":
        raise ValueError("artifact path does not name a local wheel: {}".format(wheel))
    match = re.match(r"^sparkless-([^-]+)-cp311-abi3-", wheel.name)
    if not match:
        raise ValueError("artifact is not a Sparkless cp311-abi3 wheel: {}".format(wheel.name))

    digest = wheel_sha256(wheel)
    if digest != expected_sha256.lower():
        raise ValueError("wheel changed after its pre-install digest was recorded")

    import sparkless
    import sparkless._native as native

    installed_version = importlib.metadata.version("sparkless")
    if installed_version != match.group(1):
        raise ValueError(
            "installed version {} does not match wheel version {}".format(
                installed_version, match.group(1)
            )
        )
    if not getattr(native, "__file__", None):
        raise ValueError("the Sparkless native extension did not load")
    package_path = str(Path(sparkless.__file__).resolve())
    if "site-packages" not in package_path and "dist-packages" not in package_path:
        raise ValueError("Sparkless did not import from the installed environment")

    actual_machine = canonical_machine(platform.machine())
    expected_machine = {
        "manylinux-x86_64": "x86_64",
        "manylinux-aarch64": "aarch64",
        "musllinux-x86_64": "x86_64",
        "musllinux-aarch64": "aarch64",
        "macos-x86_64": "x86_64",
        "macos-aarch64": "aarch64",
        "windows-x86_64": "x86_64",
        "windows-arm64": "aarch64",
    }.get(target)
    if expected_machine is not None and actual_machine != expected_machine:
        raise ValueError(
            "target {} requires native {}, found {}".format(
                target, expected_machine, actual_machine
            )
        )
    source_sha = os.environ.get("GITHUB_SHA", "local")
    if not source_sha:
        raise ValueError("GITHUB_SHA is required for wheel evidence")
    return {
        "schema_version": 1,
        "mode": os.environ.get("RELEASE_MODE", "release_candidate"),
        "source_sha": source_sha,
        "workflow": os.environ.get("GITHUB_WORKFLOW", "local"),
        "run_id": os.environ.get("GITHUB_RUN_ID", "local"),
        "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT", "1"),
        "target": target,
        "platform": platform.platform(),
        "platform_tag": wheel.name.split("-")[-1].removesuffix(".whl"),
        "machine": actual_machine,
        "python": platform.python_version(),
        "python_architecture": actual_machine,
        "build": {
            "profile": "release",
            "features": ["delta"],
            "rust_toolchain": "1.93.1",
        },
        "artifact": wheel.name,
        "sha256": digest,
        "preinstall_sha256": expected_sha256.lower(),
        "suite": suite,
        "result": "success",
    }


def write_evidence(evidence: dict[str, Any], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=output.parent, delete=False
    ) as stream:
        temporary = Path(stream.name)
        json.dump(evidence, stream, sort_keys=True, indent=2)
        stream.write("\n")
    # Linux runtime witnesses write into a host-mounted directory from a
    # container running as root. Keep the evidence readable by the runner's
    # artifact uploader after the atomic rename.
    temporary.chmod(0o644)
    temporary.replace(output)
