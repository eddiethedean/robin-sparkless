"""Record or validate digest-bound evidence for release server artifacts."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
from pathlib import Path
import sys
from typing import Any
import zipfile


SERVER_WHEELS = {
    "server-bin-linux-x86_64": "manylinux_2_28_x86_64",
    "server-bin-macos-aarch64": "macosx_14_0_arm64",
}
SERVER_BUILD = {"profile": "release", "features": [], "rust_toolchain": "1.93.1"}
CONTAINER_BUILD = {"profile": "release", "features": [], "rust_toolchain": "1.93"}
CONTAINER_CHECKS = ["non-root", "sql-http-mutations", "graceful-shutdown"]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def embedded_server_binary(wheel: Path) -> tuple[str, str]:
    with zipfile.ZipFile(wheel) as archive:
        candidates = [
            name
            for name in archive.namelist()
            if name.startswith("sparkless_server_bin/sparkless-server")
            and not name.endswith("/")
        ]
        if len(candidates) != 1:
            raise ValueError("server wheel must contain exactly one executable")
        name = candidates[0]
        digest = hashlib.sha256(archive.read(name)).hexdigest()
    return Path(name).name, digest


def _base_record(kind: str, target: str, suite: str) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "kind": kind,
        "mode": "manual_validation",
        "source_sha": "",
        "workflow": "",
        "run_id": "",
        "run_attempt": "",
        "target": target,
        "suite": suite,
        "result": "success",
    }


def _release_context() -> dict[str, str]:
    context = {
        "source_sha": os.environ.get("GITHUB_SHA", ""),
        "workflow": os.environ.get("GITHUB_WORKFLOW", ""),
        "run_id": os.environ.get("GITHUB_RUN_ID", ""),
        "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT", ""),
        "mode": os.environ.get("RELEASE_MODE", ""),
    }
    if not re.fullmatch(r"[0-9a-f]{40,64}", context["source_sha"]):
        raise ValueError("GITHUB_SHA must identify the tested source commit")
    if (
        not context["workflow"]
        or not context["run_id"].isdigit()
        or not context["run_attempt"].isdigit()
    ):
        raise ValueError("GitHub workflow run provenance is required")
    if context["mode"] not in {
        "manual_validation",
        "manual_rust_publish",
        "release_candidate",
        "stable_tag",
    }:
        raise ValueError("RELEASE_MODE must identify a supported release run")
    return context


def _write(record: dict[str, Any], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(record, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )


def record_server_wheel(wheel: Path, target: str, output: Path) -> None:
    expected_platform = SERVER_WHEELS.get(target)
    if expected_platform is None:
        raise ValueError("unknown server wheel target: {}".format(target))
    if not wheel.is_file() or wheel.suffix != ".whl":
        raise ValueError("server artifact must be a local wheel")
    platform_tag = wheel.name[:-4].split("-")[-1]
    if platform_tag != expected_platform:
        raise ValueError("server wheel platform tag does not match its target")
    binary_name, binary_sha = embedded_server_binary(wheel)
    record = _base_record(
        "server-bin-wheel", target, "version-http-mutations-graceful-shutdown"
    )
    record.update(_release_context())
    record.update(
        {
            "platform": platform.platform(),
            "machine": platform.machine(),
            "python": platform.python_version(),
            "build": SERVER_BUILD,
            "artifact": wheel.name,
            "sha256": sha256_file(wheel),
            "embedded_binary": {"filename": binary_name, "sha256": binary_sha},
        }
    )
    _write(record, output)


def record_container(archive: Path, image: str, image_id: str, output: Path) -> None:
    if not archive.is_file() or not re.fullmatch(r"sha256:[0-9a-f]{64}", image_id):
        raise ValueError("container archive and immutable image ID are required")
    record = _base_record(
        "server-container", "linux-native", "nonroot-http-mutations-shutdown"
    )
    record.update(_release_context())
    record.update(
        {
            "platform": platform.platform(),
            "machine": platform.machine(),
            "build": CONTAINER_BUILD,
            "image": image,
            "image_id": image_id,
            "artifact": archive.name,
            "sha256": sha256_file(archive),
            "checks": CONTAINER_CHECKS,
        }
    )
    _write(record, output)


def _validate_common(
    record: dict[str, Any],
    *,
    source_sha: str,
    run_id: str,
    run_attempt: str,
    workflow: str,
    mode: str,
) -> list[str]:
    errors: list[str] = []
    if record.get("schema_version") != 1:
        errors.append("unsupported evidence schema")
    for key, expected in (
        ("source_sha", source_sha),
        ("run_id", run_id),
        ("run_attempt", run_attempt),
        ("workflow", workflow),
        ("mode", mode),
        ("result", "success"),
    ):
        if record.get(key) != expected:
            errors.append("unexpected {}={!r}".format(key, record.get(key)))
    return errors


def verify_server_wheels(
    evidence_dir: Path,
    wheel_dir: Path,
    *,
    source_sha: str,
    run_id: str,
    run_attempt: str,
    workflow: str,
    mode: str,
) -> list[str]:
    errors: list[str] = []
    files = sorted(evidence_dir.glob("*.json"))
    observed: dict[str, dict[str, Any]] = {}
    wheels = {path.name: path for path in wheel_dir.glob("*.whl")}
    for path in files:
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            errors.append("{} is malformed: {}".format(path.name, error))
            continue
        target = record.get("target")
        if not isinstance(target, str) or target in observed:
            errors.append("{} has an invalid or duplicate target".format(path.name))
            continue
        observed[target] = record
        errors.extend(
            "{}: {}".format(path.name, issue)
            for issue in _validate_common(
                record,
                source_sha=source_sha,
                run_id=run_id,
                run_attempt=run_attempt,
                workflow=workflow,
                mode=mode,
            )
        )
        if (
            record.get("kind") != "server-bin-wheel"
            or record.get("suite") != "version-http-mutations-graceful-shutdown"
        ):
            errors.append(
                "{} does not record the server wheel runtime suite".format(path.name)
            )
        if record.get("build") != SERVER_BUILD:
            errors.append(
                "{} has unexpected server binary build provenance".format(path.name)
            )
        wheel_name = record.get("artifact")
        wheel = wheels.get(wheel_name) if isinstance(wheel_name, str) else None
        if wheel is None:
            errors.append(
                "{} refers to a wheel absent from the candidate distribution".format(
                    path.name
                )
            )
            continue
        if sha256_file(wheel) != record.get("sha256"):
            errors.append(
                "{} wheel digest does not match the candidate bytes".format(path.name)
            )
        try:
            binary_name, binary_sha = embedded_server_binary(wheel)
        except (OSError, ValueError, zipfile.BadZipFile) as error:
            errors.append("{} server wheel is malformed: {}".format(path.name, error))
        else:
            if record.get("embedded_binary") != {
                "filename": binary_name,
                "sha256": binary_sha,
            }:
                errors.append(
                    "{} embedded server binary digest does not match the wheel".format(
                        path.name
                    )
                )
        if target in SERVER_WHEELS and not wheel.name.endswith(
            SERVER_WHEELS[target] + ".whl"
        ):
            errors.append(
                "{} wheel platform tag does not match its target".format(path.name)
            )
    if set(observed) != set(SERVER_WHEELS):
        errors.append("server wheel evidence inventory is incomplete or unexpected")
    return errors


def verify_container(
    evidence_path: Path,
    archive: Path,
    *,
    source_sha: str,
    run_id: str,
    run_attempt: str,
    workflow: str,
    mode: str,
) -> list[str]:
    try:
        record = json.loads(evidence_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        return ["container evidence is malformed: {}".format(error)]
    errors = _validate_common(
        record,
        source_sha=source_sha,
        run_id=run_id,
        run_attempt=run_attempt,
        workflow=workflow,
        mode=mode,
    )
    if (
        record.get("kind") != "server-container"
        or record.get("suite") != "nonroot-http-mutations-shutdown"
    ):
        errors.append("container evidence does not record the expected smoke contract")
    if record.get("checks") != CONTAINER_CHECKS:
        errors.append("container evidence is missing a required runtime check")
    if record.get("build") != CONTAINER_BUILD:
        errors.append("container evidence has unexpected build provenance")
    if record.get("artifact") != archive.name or record.get("sha256") != sha256_file(
        archive
    ):
        errors.append("container archive digest does not match the tested image bytes")
    if not isinstance(record.get("image_id"), str) or not re.fullmatch(
        r"sha256:[0-9a-f]{64}", record["image_id"]
    ):
        errors.append("container evidence is missing the immutable image ID")
    if record.get("image") != "sparkless-server:candidate-{}".format(source_sha):
        errors.append(
            "container evidence does not identify this source candidate image"
        )
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    record_wheel = commands.add_parser("record-server-wheel")
    record_wheel.add_argument("--wheel", type=Path, required=True)
    record_wheel.add_argument("--target", required=True)
    record_wheel.add_argument("--output", type=Path, required=True)
    record_image = commands.add_parser("record-container")
    record_image.add_argument("--archive", type=Path, required=True)
    record_image.add_argument("--image", required=True)
    record_image.add_argument("--image-id", required=True)
    record_image.add_argument("--output", type=Path, required=True)
    for name in ("verify-server-wheels", "verify-container"):
        command = commands.add_parser(name)
        command.add_argument("--source-sha", required=True)
        command.add_argument("--run-id", required=True)
        command.add_argument("--run-attempt", required=True)
        command.add_argument("--workflow", required=True)
        command.add_argument(
            "--mode", choices=("release_candidate", "stable_tag"), required=True
        )
        if name == "verify-server-wheels":
            command.add_argument("--evidence-dir", type=Path, required=True)
            command.add_argument("--wheel-dir", type=Path, required=True)
        else:
            command.add_argument("--evidence", type=Path, required=True)
            command.add_argument("--archive", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command == "record-server-wheel":
            record_server_wheel(args.wheel, args.target, args.output)
        elif args.command == "record-container":
            record_container(args.archive, args.image, args.image_id, args.output)
        elif args.command == "verify-server-wheels":
            errors = verify_server_wheels(
                args.evidence_dir,
                args.wheel_dir,
                source_sha=args.source_sha,
                run_id=args.run_id,
                run_attempt=args.run_attempt,
                workflow=args.workflow,
                mode=args.mode,
            )
            if errors:
                raise ValueError("; ".join(errors))
        else:
            errors = verify_container(
                args.evidence,
                args.archive,
                source_sha=args.source_sha,
                run_id=args.run_id,
                run_attempt=args.run_attempt,
                workflow=args.workflow,
                mode=args.mode,
            )
            if errors:
                raise ValueError("; ".join(errors))
    except (OSError, ValueError, zipfile.BadZipFile) as error:
        print("Release artifact evidence rejected: {}".format(error), file=sys.stderr)
        return 1
    print("Verified release artifact evidence for {}".format(args.command))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
