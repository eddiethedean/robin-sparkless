"""Enforce the checked-in required job inventory for CI workflow runs."""

from __future__ import annotations

import json
import os
from pathlib import Path
import sys
from typing import Any


MANIFEST_PATH = Path(__file__).with_name("release_job_manifest.json")


def validate_ci_gate(event: str, needs: dict[str, Any], manifest: dict[str, Any]) -> list[str]:
    required = set(manifest["ci_required_jobs"])
    manual = set(manifest["ci_manual_jobs"])
    expected = required | manual
    errors: list[str] = []

    if set(needs) != expected:
        missing = sorted(expected - set(needs))
        unexpected = sorted(set(needs) - expected)
        if missing:
            errors.append("CI gate is missing manifest job result(s): {}".format(", ".join(missing)))
        if unexpected:
            errors.append("CI gate has unlisted dependency job(s): {}".format(", ".join(unexpected)))

    for job_id in sorted(required):
        status = (needs.get(job_id) or {}).get("result")
        if status != "success":
            errors.append("required CI job {} concluded {}".format(job_id, status or "missing"))

    for job_id in sorted(manual):
        status = (needs.get(job_id) or {}).get("result")
        expected_status = "success" if event == "workflow_dispatch" else "skipped"
        if status != expected_status:
            errors.append(
                "manual CI job {} must conclude {}; got {}".format(
                    job_id, expected_status, status or "missing"
                )
            )
    return errors


def main() -> int:
    try:
        manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
        if manifest.get("schema_version") != 1:
            raise ValueError("unsupported job manifest schema")
        needs = json.loads(os.environ["NEEDS_JSON"])
        if not isinstance(needs, dict):
            raise ValueError("needs input must be a JSON object")
        errors = validate_ci_gate(os.environ["CI_EVENT_NAME"], needs, manifest)
    except (KeyError, OSError, json.JSONDecodeError, ValueError) as error:
        print("CI gate rejected: {}".format(error), file=sys.stderr)
        return 1

    if errors:
        for error in errors:
            print("CI gate rejected: {}".format(error), file=sys.stderr)
        return 1

    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as stream:
            stream.write("Accepted CI job inventory for `{}`.\n".format(os.environ["CI_EVENT_NAME"]))
    print("Accepted CI job inventory for {}".format(os.environ["CI_EVENT_NAME"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
