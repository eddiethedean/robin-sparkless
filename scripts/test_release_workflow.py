"""Check publication boundaries and execute the workflows' actual gate commands."""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import yaml

import release_context


ROOT = Path(__file__).resolve().parents[1]


def workflow(name: str) -> dict:
    return yaml.safe_load((ROOT / ".github/workflows" / name).read_text())


def run_script(script: str, **environment: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["bash", "-e", "-o", "pipefail", "-c", script],
        env={**os.environ, **environment},
        text=True,
        capture_output=True,
        timeout=10,
    )


class ReleaseContextTests(unittest.TestCase):
    def test_publication_targets(self):
        cases = [
            ("push", "refs/heads/codex/release-4.18", False, (False, False)),
            ("push", "refs/tags/v4.18.0", False, (True, True)),
            ("workflow_dispatch", "refs/heads/main", False, (False, False)),
            ("workflow_dispatch", "refs/heads/main", True, (False, True)),
            ("workflow_dispatch", "refs/heads/topic", False, (False, False)),
            ("workflow_dispatch", "refs/tags/v4.18.0", False, (False, False)),
        ]
        for event, ref, rust, expected in cases:
            with self.subTest(event=event, ref=ref, rust=rust):
                self.assertEqual(
                    release_context.publication_targets(event, ref, rust), expected
                )

    def test_unsupported_publication_is_rejected(self):
        cases = [
            ("push", "refs/tags/v4.18.0rc1", False),
            ("push", "refs/tags/v4.18", False),
            ("push", "refs/heads/main", False),
            ("push", "refs/tags/v4.18.0", True),
            ("workflow_dispatch", "refs/heads/topic", True),
            ("workflow_dispatch", "refs/tags/v4.18.0", True),
            ("pull_request", "refs/tags/v4.18.0", False),
        ]
        for case in cases:
            with self.subTest(context=case), self.assertRaises(ValueError):
                release_context.publication_targets(*case)

    def test_metadata_failure_emits_no_publication_outputs(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "output"
            environment = {
                "GITHUB_EVENT_NAME": "push",
                "GITHUB_REF": "refs/tags/v4.18.0",
                "GITHUB_OUTPUT": str(output),
            }
            with (
                patch.dict(os.environ, environment),
                patch.object(
                    release_context, "check_release", return_value=["version mismatch"]
                ),
                self.assertRaises(SystemExit),
            ):
                release_context.main()
            self.assertFalse(output.exists())


class ReleaseWorkflowTests(unittest.TestCase):
    def test_every_validation_job_is_required_before_publishing(self):
        jobs = workflow("release.yml")["jobs"]
        gate = jobs["release-validation-gate"]
        publishers = {name for name in jobs if name.startswith("publish")}
        self.assertEqual(
            set(gate["needs"]), set(jobs) - publishers - {"release-validation-gate"}
        )
        self.assertEqual(gate["if"], "always()")
        for name in publishers:
            publisher = jobs[name]
            with self.subTest(publisher=name):
                self.assertIn("release-validation-gate", publisher["needs"])
                # Default success() must block publishers when a prerequisite fails.
                self.assertNotIn("always()", publisher["if"])
                self.assertIn("outputs.publish_", publisher["if"])

    def test_release_gate_rejects_failed_cancelled_and_skipped_jobs(self):
        gate = workflow("release.yml")["jobs"]["release-validation-gate"]
        script = gate["steps"][0]["run"]
        statuses = ["success"] * len(gate["needs"])
        self.assertEqual(run_script(script, RESULTS=",".join(statuses)).returncode, 0)
        for index, job in enumerate(gate["needs"]):
            for status in ("failure", "cancelled", "skipped"):
                with self.subTest(job=job, status=status):
                    changed = statuses.copy()
                    changed[index] = status
                    self.assertNotEqual(
                        run_script(script, RESULTS=",".join(changed)).returncode, 0
                    )

    @unittest.skipUnless(shutil.which("jq"), "jq is required for CI gate tests")
    def test_ci_gate_allows_only_intentional_manual_skips(self):
        gate = workflow("ci.yml")["jobs"]["ci-acceptance"]
        script = gate["steps"][0]["run"]
        for event in ("push", "pull_request", "workflow_dispatch"):
            results = {name: {"result": "success"} for name in gate["needs"]}
            if event != "workflow_dispatch":
                for name in ("pyspark-smoke", "python-full"):
                    results[name]["result"] = "skipped"
            environment = {"CI_EVENT_NAME": event, "NEEDS_JSON": json.dumps(results)}
            self.assertEqual(run_script(script, **environment).returncode, 0)
            for name in gate["needs"]:
                for status in ("failure", "cancelled", "skipped"):
                    if status == results[name]["result"]:
                        continue
                    with self.subTest(event=event, job=name, status=status):
                        changed = {**results, name: {"result": status}}
                        self.assertNotEqual(
                            run_script(
                                script,
                                CI_EVENT_NAME=event,
                                NEEDS_JSON=json.dumps(changed),
                            ).returncode,
                            0,
                        )

    @unittest.skipUnless(shutil.which("jq"), "jq is required for CI reuse tests")
    def test_publication_requires_ci_success_for_the_same_commit(self):
        metadata = workflow("release.yml")["jobs"]["release-metadata"]
        step = metadata["steps"][-1]
        self.assertEqual(step["if"], "steps.context.outputs.publish_rust == 'true'")
        # Emulate gh's successful-run response and execute the actual jq filter.
        script = 'gh() { printf "%s" "$CI_RUNS"; }\n' + step["run"]
        cases = [
            ([], False),
            ([{"event": "push", "headSha": "other"}], False),
            ([{"event": "pull_request", "headSha": "candidate"}], False),
            ([{"event": "push", "headSha": "candidate"}], True),
            ([{"event": "workflow_dispatch", "headSha": "candidate"}], True),
        ]
        with tempfile.TemporaryDirectory() as directory:
            for runs, accepted in cases:
                with self.subTest(runs=runs):
                    result = run_script(
                        script,
                        CI_RUNS=json.dumps(runs),
                        GITHUB_SHA="candidate",
                        RUNNER_TEMP=directory,
                    )
                    self.assertEqual(result.returncode == 0, accepted, result.stderr)


if __name__ == "__main__":
    unittest.main()
