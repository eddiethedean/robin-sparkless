"""Classify a Release workflow run and enforce its checked-in job manifest."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import sys
from typing import Any


MANIFEST_PATH = Path(__file__).with_name("release_job_manifest.json")
STABLE_TAG = re.compile(r"^refs/tags/v(\d+\.\d+\.\d+)$")


def _input_bool(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"", "false", "0", "no"}:
        return False
    if normalized in {"true", "1", "yes"}:
        return True
    raise ValueError("publish_rust must be true or false")


def classify_mode(event: str, ref: str, publish_rust: str, recovery_run_id: str) -> str:
    """Return a supported mode; reject ambiguous inputs before credentials run."""
    wants_rust = _input_bool(publish_rust)

    if event == "push":
        if recovery_run_id or wants_rust:
            raise ValueError("push events cannot use manual publication inputs")
        if ref.startswith("refs/heads/codex/release-"):
            return "release_candidate"
        if STABLE_TAG.fullmatch(ref):
            return "stable_tag"
        raise ValueError("unsupported Release push ref: {}".format(ref))

    if event != "workflow_dispatch":
        raise ValueError("unsupported Release event: {}".format(event))

    if recovery_run_id:
        if wants_rust:
            raise ValueError("PyPI recovery cannot be combined with Rust publication")
        if ref != "refs/heads/main":
            raise ValueError("PyPI recovery is restricted to workflow_dispatch on main")
        if not recovery_run_id.isdecimal():
            raise ValueError("PyPI recovery run ID must be numeric")
        return "pypi_recovery"

    if wants_rust:
        if ref != "refs/heads/main":
            raise ValueError("Rust publication is restricted to workflow_dispatch on main")
        return "manual_rust_publish"

    if not (ref.startswith("refs/heads/") or STABLE_TAG.fullmatch(ref)):
        raise ValueError("manual validation requires a branch or stable tag ref")
    return "manual_validation"


def _required_jobs(manifest: dict[str, Any], mode: str, ref: str) -> tuple[set[str], set[str]]:
    """Return (required, explicitly-optional) job IDs for one run mode."""
    shared = set(manifest["shared_release_jobs"])
    candidate = set(manifest["candidate_jobs"])
    manual = set(manifest["manual_release_jobs"])
    optional: set[str] = set()

    if mode in {"release_candidate", "stable_tag"}:
        required = shared | candidate
        if not _is_417_ref(ref):
            specific = manifest["release_line_specific"]["4.17"]
            v17 = set(specific["must_skip_otherwise"])
            required -= v17
            optional |= v17
        return required, optional | manual

    if mode in {"manual_validation", "manual_rust_publish"}:
        return shared | manual, candidate

    if mode == "pypi_recovery":
        return set(), shared | candidate | manual

    raise ValueError("unsupported mode in acceptance gate: {}".format(mode))


def _is_417_ref(ref: str) -> bool:
    return ref.startswith("refs/heads/codex/release-4.17") or ref.startswith(
        "refs/tags/v4.17."
    )


def validate_gate(mode: str, ref: str, needs: dict[str, Any], manifest: dict[str, Any]) -> list[str]:
    """Fail missing jobs, unlisted skips, failures, and unexpected dependencies."""
    errors: list[str] = []
    expected = (
        set(manifest["shared_release_jobs"])
        | set(manifest["candidate_jobs"])
        | set(manifest["manual_release_jobs"])
    )
    if set(needs) != expected | {"release-mode"}:
        missing = sorted((expected | {"release-mode"}) - set(needs))
        unexpected = sorted(set(needs) - expected - {"release-mode"})
        if missing:
            errors.append("gate is missing manifest job result(s): {}".format(", ".join(missing)))
        if unexpected:
            errors.append("gate has unlisted dependency job(s): {}".format(", ".join(unexpected)))

    mode_job = needs.get("release-mode", {})
    if mode_job.get("result") != "success":
        errors.append("release mode classifier did not succeed")

    required, optional = _required_jobs(manifest, mode, ref)
    for job_id in sorted(required):
        status = (needs.get(job_id) or {}).get("result")
        if status != "success":
            errors.append("required job {} concluded {}".format(job_id, status or "missing"))

    for job_id in sorted(optional):
        status = (needs.get(job_id) or {}).get("result")
        if status != "skipped":
            errors.append(
                "job {} may be skipped only in this mode; concluded {}".format(
                    job_id, status or "missing"
                )
            )

    return errors


def _manifest() -> dict[str, Any]:
    data = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    if data.get("schema_version") != 1:
        raise ValueError("unsupported release job manifest schema")
    return data


def _classify(args: argparse.Namespace) -> int:
    try:
        mode = classify_mode(
            os.environ.get("GITHUB_EVENT_NAME", ""),
            os.environ.get("GITHUB_REF", ""),
            os.environ.get("INPUT_PUBLISH_RUST", ""),
            os.environ.get("INPUT_PYPI_RECOVERY_RUN_ID", ""),
        )
    except ValueError as error:
        print("Release mode rejected: {}".format(error), file=sys.stderr)
        return 1

    output = Path(args.output)
    with output.open("a", encoding="utf-8") as stream:
        stream.write("mode={}\n".format(mode))
        stream.write("source_sha={}\n".format(os.environ.get("GITHUB_SHA", "")))
    print("Release mode: {} for {}".format(mode, os.environ.get("GITHUB_SHA", "unknown SHA")))
    return 0


def _gate(args: argparse.Namespace) -> int:
    try:
        manifest = _manifest()
        mode = classify_mode(
            os.environ.get("GITHUB_EVENT_NAME", ""),
            os.environ.get("GITHUB_REF", ""),
            os.environ.get("INPUT_PUBLISH_RUST", ""),
            os.environ.get("INPUT_PYPI_RECOVERY_RUN_ID", ""),
        )
        if mode != args.mode:
            raise ValueError("mode classifier output disagrees with workflow context")
        needs = json.loads(Path(args.needs_json).read_text(encoding="utf-8"))
        if not isinstance(needs, dict):
            raise ValueError("needs input must be a JSON object")
        errors = validate_gate(mode, os.environ.get("GITHUB_REF", ""), needs, manifest)
    except (OSError, json.JSONDecodeError, ValueError, KeyError) as error:
        print("Release gate rejected: {}".format(error), file=sys.stderr)
        return 1

    if errors:
        for error in errors:
            print("Release gate rejected: {}".format(error), file=sys.stderr)
        return 1

    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as stream:
            stream.write(
                "Accepted release mode `{}` for source SHA `{}`.\n".format(
                    mode, os.environ.get("GITHUB_SHA", "unknown")
                )
            )
    print("Accepted release mode {} for {}".format(mode, os.environ.get("GITHUB_SHA", "unknown SHA")))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    classify_parser = subparsers.add_parser("classify")
    classify_parser.add_argument("--output", default=os.environ.get("GITHUB_OUTPUT", "/dev/null"))
    gate_parser = subparsers.add_parser("gate")
    gate_parser.add_argument("--mode", required=True)
    gate_parser.add_argument("--needs-json", required=True)
    args = parser.parse_args(argv)
    return _classify(args) if args.command == "classify" else _gate(args)


if __name__ == "__main__":
    raise SystemExit(main())
