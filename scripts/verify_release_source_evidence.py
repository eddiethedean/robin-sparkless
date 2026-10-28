"""Record and verify digest-bound evidence for the isolated sdist wheel build."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import sys
from typing import Any


BUILD = {"profile": "release", "features": ["delta"], "rust_toolchain": "1.93.1"}
SUITE = "isolated-sdist-maturin-wheel"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _release_context() -> dict[str, str]:
    result = {
        "source_sha": os.environ.get("GITHUB_SHA", ""),
        "workflow": os.environ.get("GITHUB_WORKFLOW", ""),
        "run_id": os.environ.get("GITHUB_RUN_ID", ""),
        "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT", ""),
        "mode": os.environ.get("RELEASE_MODE", ""),
    }
    if not re.fullmatch(r"[0-9a-f]{40,64}", result["source_sha"]):
        raise ValueError("GITHUB_SHA must identify the tested source commit")
    if (
        not result["workflow"]
        or not result["run_id"].isdigit()
        or not result["run_attempt"].isdigit()
    ):
        raise ValueError("GitHub workflow run provenance is required")
    if result["mode"] not in {"release_candidate", "stable_tag"}:
        raise ValueError("RELEASE_MODE must identify a candidate or stable tag")
    return result


def record(sdist: Path, wheel: Path, output: Path) -> None:
    if not sdist.is_file() or not sdist.name.endswith(".tar.gz"):
        raise ValueError("source distribution must be a local .tar.gz archive")
    if not wheel.is_file() or wheel.suffix != ".whl":
        raise ValueError("isolated source build must produce a local wheel")
    record: dict[str, Any] = {
        "schema_version": 1,
        "kind": "sdist-source-wheel",
        **_release_context(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "python": platform.python_version(),
        "python_architecture": platform.machine(),
        "build": BUILD,
        "sdist": {"artifact": sdist.name, "sha256": sha256_file(sdist)},
        "derived_wheel": {"artifact": wheel.name, "sha256": sha256_file(wheel)},
        "suite": SUITE,
        "result": "success",
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(record, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )


def _read_record(path: Path) -> dict[str, Any]:
    try:
        record = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(
            "source-build evidence is malformed: {}".format(error)
        ) from error
    if not isinstance(record, dict):
        raise ValueError("source-build evidence must be a JSON object")
    return record


def verify(
    evidence_path: Path,
    sdist_dir: Path,
    wheel_dir: Path,
    *,
    source_sha: str,
    run_id: str,
    run_attempt: str,
    workflow: str,
    mode: str,
) -> list[str]:
    record_data = _read_record(evidence_path)
    errors: list[str] = []
    expected_fields = {
        "schema_version": 1,
        "kind": "sdist-source-wheel",
        "source_sha": source_sha,
        "run_id": run_id,
        "run_attempt": run_attempt,
        "workflow": workflow,
        "mode": mode,
        "build": BUILD,
        "suite": SUITE,
        "result": "success",
    }
    for key, expected in expected_fields.items():
        if record_data.get(key) != expected:
            errors.append("unexpected {}={!r}".format(key, record_data.get(key)))

    sdists = sorted(path for path in sdist_dir.glob("*.tar.gz") if path.is_file())
    wheels = sorted(path for path in wheel_dir.glob("*.whl") if path.is_file())
    if len(sdists) != 1:
        errors.append("candidate must contain exactly one source distribution")
    if len(wheels) != 1:
        errors.append("source witness must contain exactly one derived wheel")
    if len(sdists) == 1:
        expected_sdist = {"artifact": sdists[0].name, "sha256": sha256_file(sdists[0])}
        if record_data.get("sdist") != expected_sdist:
            errors.append(
                "source distribution digest does not match the candidate bytes"
            )
    if len(wheels) == 1:
        expected_wheel = {"artifact": wheels[0].name, "sha256": sha256_file(wheels[0])}
        if record_data.get("derived_wheel") != expected_wheel:
            errors.append(
                "derived wheel digest does not match the tested source-build bytes"
            )
    return errors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    record_parser = commands.add_parser("record")
    record_parser.add_argument("--sdist", type=Path, required=True)
    record_parser.add_argument("--wheel", type=Path, required=True)
    record_parser.add_argument("--output", type=Path, required=True)
    verify_parser = commands.add_parser("verify")
    verify_parser.add_argument("--evidence", type=Path, required=True)
    verify_parser.add_argument("--sdist-dir", type=Path, required=True)
    verify_parser.add_argument("--wheel-dir", type=Path, required=True)
    verify_parser.add_argument("--source-sha", required=True)
    verify_parser.add_argument("--run-id", required=True)
    verify_parser.add_argument("--run-attempt", required=True)
    verify_parser.add_argument("--workflow", required=True)
    verify_parser.add_argument(
        "--mode", choices=("release_candidate", "stable_tag"), required=True
    )
    args = parser.parse_args(argv)
    try:
        if args.command == "record":
            record(args.sdist, args.wheel, args.output)
        else:
            errors = verify(
                args.evidence,
                args.sdist_dir,
                args.wheel_dir,
                source_sha=args.source_sha,
                run_id=args.run_id,
                run_attempt=args.run_attempt,
                workflow=args.workflow,
                mode=args.mode,
            )
            if errors:
                raise ValueError("; ".join(errors))
    except (OSError, ValueError) as error:
        print("Source-build evidence rejected: {}".format(error), file=sys.stderr)
        return 1
    print("Verified isolated source-build evidence")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
