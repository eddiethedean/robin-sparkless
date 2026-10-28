"""Validate a completed release run before reusing its PyPI artifacts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import sys
from typing import Any


RELEASE_WORKFLOW = ".github/workflows/release.yml"
VERIFIED_CANDIDATE_JOB = "Verify complete candidate PyPI inventory (V31)"
PUBLISHED_CRATES_JOB = "Build docs and publish to crates.io"
SOURCE_EVIDENCE_ARTIFACT = "release-sdist-source-witness-v1"
SOURCE_EVIDENCE_MARKER = "Verify isolated source build from the exact sdist (evidence schema v1)"
REQUIRED_ARTIFACTS = {
    "verified-server-bin-candidate",
    "verified-sparkless-candidate",
}


def validate_recovery_source(
    run: dict[str, Any],
    jobs: dict[str, Any],
    artifacts: dict[str, Any],
    repository: str,
    workflow_id: int,
    source_workflow: str | None = None,
) -> str:
    """Return the stable version only for an eligible completed tag run."""
    return validate_recovery_source_details(
        run, jobs, artifacts, repository, workflow_id, source_workflow
    )["version"]


def validate_recovery_source_details(
    run: dict[str, Any],
    jobs: dict[str, Any],
    artifacts: dict[str, Any],
    repository: str,
    workflow_id: int,
    source_workflow: str | None = None,
) -> dict[str, str]:
    """Validate the original run and select its declared evidence compatibility policy."""
    if run.get("event") != "push":
        raise ValueError("source run must be triggered by a tag push")
    if run.get("status") != "completed":
        raise ValueError("source release run must be completed")
    if run.get("workflow_id") != workflow_id:
        raise ValueError("source run must use the release workflow")
    if run.get("path") != RELEASE_WORKFLOW:
        raise ValueError("source run came from a different workflow file")
    head_repository = run.get("head_repository") or {}
    if str(head_repository.get("full_name", "")).lower() != repository.lower():
        raise ValueError("source run must belong to this repository")

    tag = run.get("head_branch", "")
    match = re.fullmatch(r"v([0-9]+\.[0-9]+\.[0-9]+)", tag)
    if match is None:
        raise ValueError("source run must target a stable version tag")

    if not any(
        job.get("name") == VERIFIED_CANDIDATE_JOB
        and job.get("conclusion") == "success"
        for job in jobs.get("jobs", [])
    ):
        raise ValueError("source run did not pass the complete V31 inventory gate")
    if not any(
        job.get("name") == PUBLISHED_CRATES_JOB
        and job.get("conclusion") == "success"
        for job in jobs.get("jobs", [])
    ):
        raise ValueError("source run did not successfully publish the Rust crates")

    live_artifacts = [
        artifact
        for artifact in artifacts.get("artifacts", [])
        if artifact.get("expired") is False
    ]
    available_artifacts = {artifact.get("name") for artifact in live_artifacts}
    missing_artifacts = REQUIRED_ARTIFACTS - available_artifacts
    if missing_artifacts:
        raise ValueError(
            "source run is missing unexpired verified artifact(s): {}".format(
                ", ".join(sorted(missing_artifacts))
            )
        )
    uses_source_manifest = (
        source_workflow is not None and SOURCE_EVIDENCE_MARKER in source_workflow
    )
    has_source_manifest_artifact = SOURCE_EVIDENCE_ARTIFACT in available_artifacts
    if uses_source_manifest and not has_source_manifest_artifact:
        raise ValueError("source workflow requires the unexpired schema v1 source evidence artifact")
    if not uses_source_manifest and has_source_manifest_artifact:
        raise ValueError("source evidence artifact is present without its workflow schema marker")
    policy = "manifest_v1" if uses_source_manifest else "legacy_inventory_v0"

    source_sha = run.get("head_sha", "")
    if not re.fullmatch(r"[0-9a-f]{40,64}", source_sha):
        raise ValueError("source run is missing its immutable head SHA")
    run_id = run.get("id")
    run_attempt = run.get("run_attempt")
    if not isinstance(run_id, int) or not isinstance(run_attempt, int):
        raise ValueError("source run is missing its run ID or attempt")
    workflow_name = run.get("name", "")
    if not isinstance(workflow_name, str) or not workflow_name:
        raise ValueError("source run is missing its workflow name")
    return {
        "version": match.group(1),
        "source_sha": source_sha,
        "source_run_id": str(run_id),
        "source_run_attempt": str(run_attempt),
        "source_workflow": workflow_name,
        "source_evidence_policy": policy,
    }


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("{} must contain a JSON object".format(path))
    return value


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-json", type=Path, required=True)
    parser.add_argument("--jobs-json", type=Path, required=True)
    parser.add_argument("--artifacts-json", type=Path, required=True)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--workflow-id", type=int, required=True)
    parser.add_argument("--source-workflow", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        details = validate_recovery_source_details(
            _read_json(args.run_json),
            _read_json(args.jobs_json),
            _read_json(args.artifacts_json),
            args.repository,
            args.workflow_id,
            args.source_workflow.read_text(encoding="utf-8"),
        )
    except (OSError, json.JSONDecodeError, ValueError) as error:
        print("PyPI recovery source rejected: {}".format(error), file=sys.stderr)
        return 1
    for name, value in details.items():
        print("{}={}".format(name, value))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
