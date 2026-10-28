"""Require every planned suite to have tested the exact candidate wheel bytes."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys
from typing import Any


PYTHON_311_TO_314 = {"3.11", "3.12", "3.13", "3.14"}
EXPECTED: set[tuple[str, str, str]] = set()
for target in ("manylinux-x86_64", "macos-aarch64", "windows-x86_64"):
    EXPECTED.update((target, "broad-python", version) for version in PYTHON_311_TO_314)
EXPECTED.add(("manylinux-x86_64", "serial-delta", "3.11"))
for target in (
    "manylinux-x86_64",
    "manylinux-aarch64",
    "musllinux-x86_64",
    "musllinux-aarch64",
    "macos-x86_64",
    "windows-arm64",
):
    EXPECTED.update(
        (target, "platform-runtime", version) for version in ("3.11", "3.14")
    )

EXPECTED_MACHINE = {
    "manylinux-x86_64": "x86_64",
    "manylinux-aarch64": "aarch64",
    "musllinux-x86_64": "x86_64",
    "musllinux-aarch64": "aarch64",
    "macos-x86_64": "x86_64",
    "macos-aarch64": "aarch64",
    "windows-x86_64": "x86_64",
    "windows-arm64": "aarch64",
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def verify(
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
    evidence_files = sorted(evidence_dir.rglob("*.json"))
    if not evidence_files:
        return ["no wheel test evidence JSON files were found"]

    wheels = {path.name: path for path in wheel_dir.glob("*.whl")}
    observed: dict[tuple[str, str, str], dict[str, Any]] = {}
    for path in evidence_files:
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
            key = (
                record["target"],
                record["suite"],
                record["python"].rsplit(".", 1)[0],
            )
        except (
            OSError,
            json.JSONDecodeError,
            KeyError,
            TypeError,
            AttributeError,
        ) as error:
            errors.append("{} is malformed: {}".format(path.name, error))
            continue
        if key in observed:
            errors.append("duplicate wheel evidence for {}".format(key))
        observed[key] = record

        if record.get("schema_version") != 1:
            errors.append("{} uses an unsupported evidence schema".format(path.name))
        for field, expected in (
            ("source_sha", source_sha),
            ("run_id", run_id),
            ("run_attempt", run_attempt),
            ("workflow", workflow),
            ("mode", mode),
            ("result", "success"),
        ):
            if record.get(field) != expected:
                errors.append(
                    "{} has unexpected {}={!r}".format(
                        path.name, field, record.get(field)
                    )
                )
        if record.get("machine") != EXPECTED_MACHINE.get(record.get("target")):
            errors.append(
                "{} did not run on the target's native architecture".format(path.name)
            )
        if record.get("python_architecture") != EXPECTED_MACHINE.get(
            record.get("target")
        ):
            errors.append(
                "{} used a Python interpreter with the wrong architecture".format(
                    path.name
                )
            )
        build = record.get("build")
        if build != {
            "profile": "release",
            "features": ["delta"],
            "rust_toolchain": "1.93.1",
        }:
            errors.append("{} has unexpected wheel build provenance".format(path.name))
        artifact = record.get("artifact")
        wheel = wheels.get(artifact) if isinstance(artifact, str) else None
        if wheel is None:
            errors.append(
                "{} refers to a wheel absent from the candidate inventory".format(
                    path.name
                )
            )
        elif (
            not isinstance(record.get("sha256"), str)
            or _sha256(wheel) != record["sha256"]
        ):
            errors.append(
                "{} digest does not match the candidate wheel bytes".format(path.name)
            )
        if record.get("preinstall_sha256") != record.get("sha256"):
            errors.append(
                "{} was not checked against its pre-install digest".format(path.name)
            )
        if isinstance(artifact, str) and "cp311-abi3-" not in artifact:
            errors.append("{} does not name a cp311-abi3 artifact".format(path.name))

    missing = sorted(EXPECTED - set(observed))
    unexpected = sorted(set(observed) - EXPECTED)
    if missing:
        errors.append("missing required wheel suite evidence: {}".format(missing))
    if unexpected:
        errors.append("unexpected wheel suite evidence: {}".format(unexpected))

    for target in EXPECTED_MACHINE:
        digests = {
            record.get("sha256")
            for (record_target, _suite, _python), record in observed.items()
            if record_target == target
        }
        if len(digests) != 1:
            errors.append(
                "{} evidence does not refer to one identical wheel digest".format(
                    target
                )
            )
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--wheel-dir", type=Path, required=True)
    parser.add_argument("--source-sha", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--run-attempt", required=True)
    parser.add_argument("--workflow", required=True)
    parser.add_argument(
        "--mode", choices=("release_candidate", "stable_tag"), required=True
    )
    args = parser.parse_args()
    errors = verify(
        args.evidence_dir,
        args.wheel_dir,
        source_sha=args.source_sha,
        run_id=args.run_id,
        run_attempt=args.run_attempt,
        workflow=args.workflow,
        mode=args.mode,
    )
    if errors:
        for error in errors:
            print("Release wheel evidence rejected: {}".format(error), file=sys.stderr)
        return 1
    print(
        "Verified {} exact-wheel suite results across {} wheel targets".format(
            len(EXPECTED), len(EXPECTED_MACHINE)
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
