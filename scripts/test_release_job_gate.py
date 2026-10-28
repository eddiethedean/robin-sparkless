"""Dry-run tests for the release workflow's mutually exclusive job modes."""

from __future__ import annotations

import json
from pathlib import Path
import unittest

from verify_release_job_gate import _required_jobs, classify_mode, validate_gate


MANIFEST = json.loads(Path(__file__).with_name("release_job_manifest.json").read_text())


class ReleaseModeTests(unittest.TestCase):
    def test_candidate_tag_manual_and_recovery_classification(self) -> None:
        cases = (
            ("push", "refs/heads/codex/release-4.17", "false", "", "release_candidate"),
            ("push", "refs/tags/v4.17.0", "false", "", "stable_tag"),
            ("workflow_dispatch", "refs/heads/main", "false", "", "manual_validation"),
            ("workflow_dispatch", "refs/heads/main", "true", "", "manual_rust_publish"),
            ("workflow_dispatch", "refs/heads/main", "false", "123456", "pypi_recovery"),
        )
        for event, ref, publish_rust, recovery_id, expected in cases:
            with self.subTest(mode=expected):
                self.assertEqual(
                    classify_mode(event, ref, publish_rust, recovery_id), expected
                )

    def test_conflicting_publication_inputs_are_rejected(self) -> None:
        cases = (
            ("push", "refs/tags/v4.17.0", "true", ""),
            ("workflow_dispatch", "refs/heads/topic", "true", ""),
            ("workflow_dispatch", "refs/heads/main", "true", "123456"),
            ("workflow_dispatch", "refs/heads/main", "false", "abc"),
            ("workflow_dispatch", "refs/tags/v4.17", "false", ""),
        )
        for event, ref, publish_rust, recovery_id in cases:
            with self.subTest(ref=ref, recovery_id=recovery_id):
                with self.assertRaises(ValueError):
                    classify_mode(event, ref, publish_rust, recovery_id)


class ReleaseGateTests(unittest.TestCase):
    def _needs(self, mode: str, ref: str) -> dict[str, dict[str, str]]:
        required, optional = _required_jobs(MANIFEST, mode, ref)
        needs = {"release-mode": {"result": "success"}}
        needs.update({job: {"result": "success"} for job in required})
        needs.update({job: {"result": "skipped"} for job in optional})
        return needs

    def test_candidate_requires_exact_artifact_and_witness_matrix(self) -> None:
        needs = self._needs("release_candidate", "refs/heads/codex/release-4.17")
        self.assertEqual(
            validate_gate("release_candidate", "refs/heads/codex/release-4.17", needs, MANIFEST),
            [],
        )
        needs["python-wheel-linux-runtime"]["result"] = "failure"
        self.assertTrue(
            any("python-wheel-linux-runtime" in issue for issue in validate_gate(
                "release_candidate", "refs/heads/codex/release-4.17", needs, MANIFEST
            ))
        )

    def test_manual_validation_uses_only_manual_python_cells(self) -> None:
        ref = "refs/heads/main"
        needs = self._needs("manual_validation", ref)
        self.assertEqual(validate_gate("manual_validation", ref, needs, MANIFEST), [])
        needs["python-tests"]["result"] = "skipped"
        self.assertTrue(validate_gate("manual_validation", ref, needs, MANIFEST))

    def test_manifest_rejects_missing_and_unexpected_needs(self) -> None:
        ref = "refs/heads/codex/release-4.17"
        needs = self._needs("release_candidate", ref)
        del needs["phase1-v17-resource-witness"]
        needs["made-up-job"] = {"result": "success"}
        issues = validate_gate("release_candidate", ref, needs, MANIFEST)
        self.assertTrue(any("missing manifest job" in issue for issue in issues))
        self.assertTrue(any("unlisted dependency" in issue for issue in issues))


if __name__ == "__main__":
    unittest.main()
