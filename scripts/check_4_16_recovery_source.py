"""Guard the one-time 4.16.0 recovery workflow to its validated source run."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


SOURCE_RUN_ID = 40042661738
SOURCE_SHA = "8b20ea8ce3326a38c361ec859278b32ef790f53b"
SERVER_BUILD_JOBS = (
    "Build and validate sparkless-server-bin (ubuntu-22.04)",
    "Build and validate sparkless-server-bin (windows-latest)",
    "Build and validate sparkless-server-bin (macos-15-intel)",
    "Build and validate sparkless-server-bin (macos-15)",
)
SERVER_ARTIFACTS = (
    "sparkless-server-bin-ubuntu-22.04-manylinux_2_35_x86_64",
    "sparkless-server-bin-windows-latest-native",
    "sparkless-server-bin-macos-15-intel-macosx_15_0_x86_64",
    "sparkless-server-bin-macos-15-macosx_15_0_arm64",
)
REQUIRED_JOBS = (
    "Release metadata",
    "Build test binaries",
    "Deny",
    "Rust tests",
    "Format",
    "Python (mypy)",
    "Audit",
    "Clippy",
    "Verify required server release evidence",
)


def _read_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as source:
        value = json.load(source)
    if not isinstance(value, dict):
        raise ValueError("{} does not contain a JSON object".format(path))
    return value


def validate_source_run(
    run: dict[str, Any], jobs_response: dict[str, Any], artifacts_response: dict[str, Any]
) -> None:
    if run.get("id") != SOURCE_RUN_ID:
        raise ValueError("recovery is pinned to source run {}".format(SOURCE_RUN_ID))
    if run.get("head_sha") != SOURCE_SHA:
        raise ValueError("source run commit does not match the validated 4.16.0 tag")
    if run.get("head_branch") != "v4.16.0" or run.get("event") != "push":
        raise ValueError("source run was not triggered by pushing the v4.16.0 tag")

    jobs = jobs_response.get("jobs", [])
    by_name: dict[str, list[dict[str, Any]]] = {}
    for job in jobs:
        by_name.setdefault(job.get("name", ""), []).append(job)
    for name in REQUIRED_JOBS + SERVER_BUILD_JOBS:
        records = by_name.get(name, [])
        if len(records) != 1 or records[0].get("conclusion") != "success":
            raise ValueError("required source job did not pass exactly once: {}".format(name))

    python_jobs = [
        job for job in jobs if job.get("name", "").startswith("Python tests - ")
    ]
    if len(python_jobs) != 15 or any(
        job.get("conclusion") != "success" for job in python_jobs
    ):
        raise ValueError("all 15 source Python test jobs must have passed")

    artifacts = artifacts_response.get("artifacts", [])
    by_artifact = {item.get("name"): item for item in artifacts}
    for name in SERVER_ARTIFACTS:
        artifact = by_artifact.get(name)
        if artifact is None or artifact.get("expired") is not False:
            raise ValueError("validated server wheel artifact is unavailable: {}".format(name))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, required=True)
    parser.add_argument("--jobs", type=Path, required=True)
    parser.add_argument("--artifacts", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        validate_source_run(
            _read_json(args.run), _read_json(args.jobs), _read_json(args.artifacts)
        )
    except (OSError, ValueError) as error:
        parser.error(str(error))
    print("validated source run {} at {}".format(SOURCE_RUN_ID, SOURCE_SHA))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
