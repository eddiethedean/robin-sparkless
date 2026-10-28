import unittest

from verify_pypi_recovery_source import (
    SOURCE_EVIDENCE_ARTIFACT,
    SOURCE_EVIDENCE_MARKER,
    validate_recovery_source,
    validate_recovery_source_details,
)


class PypiRecoverySourceTest(unittest.TestCase):
    def setUp(self):
        self.run = {
            "event": "push",
            "status": "completed",
            "workflow_id": 123,
            "path": ".github/workflows/release.yml",
            "head_repository": {"full_name": "example/project"},
            "head_branch": "v4.16.1",
            "head_sha": "c" * 40,
            "id": 9876,
            "run_attempt": 1,
            "name": "Release",
        }
        self.jobs = {
            "jobs": [
                {
                    "name": "Verify complete candidate PyPI inventory (V31)",
                    "conclusion": "success",
                },
                {
                    "name": "Build docs and publish to crates.io",
                    "conclusion": "success",
                },
            ]
        }
        self.artifacts = {
            "artifacts": [
                {"name": "verified-server-bin-candidate", "expired": False},
                {"name": "verified-sparkless-candidate", "expired": False},
            ]
        }

    def validate(self):
        return validate_recovery_source(
            self.run, self.jobs, self.artifacts, "example/project", 123
        )

    def test_accepts_only_a_completed_verified_stable_tag_run(self):
        self.assertEqual(self.validate(), "4.16.1")

    def test_rejects_non_tag_push(self):
        self.run["head_branch"] = "codex/release-4.17"
        with self.assertRaisesRegex(ValueError, "stable version tag"):
            self.validate()

    def test_rejects_non_push_or_incomplete_runs(self):
        self.run["event"] = "workflow_dispatch"
        with self.assertRaisesRegex(ValueError, "tag push"):
            self.validate()
        self.run["event"] = "push"
        self.run["status"] = "in_progress"
        with self.assertRaisesRegex(ValueError, "must be completed"):
            self.validate()

    def test_rejects_other_workflows_and_repositories(self):
        self.run["workflow_id"] = 999
        with self.assertRaisesRegex(ValueError, "release workflow"):
            self.validate()
        self.run["workflow_id"] = 123
        self.run["path"] = ".github/workflows/other.yml"
        with self.assertRaisesRegex(ValueError, "different workflow"):
            self.validate()
        self.run["path"] = ".github/workflows/release.yml"
        self.run["head_repository"]["full_name"] = "other/project"
        with self.assertRaisesRegex(ValueError, "this repository"):
            self.validate()

    def test_requires_successful_v31_inventory(self):
        self.jobs["jobs"][0]["conclusion"] = "failure"
        with self.assertRaisesRegex(ValueError, "V31 inventory gate"):
            self.validate()

    def test_requires_rust_release_to_have_succeeded(self):
        self.jobs["jobs"][1]["conclusion"] = "failure"
        with self.assertRaisesRegex(ValueError, "publish the Rust crates"):
            self.validate()

    def test_requires_both_unexpired_verified_artifacts(self):
        self.artifacts["artifacts"][0]["expired"] = True
        with self.assertRaisesRegex(ValueError, "missing unexpired"):
            self.validate()

    def test_rejects_prerelease_tags(self):
        self.run["head_branch"] = "v4.17.0-rc.1"
        with self.assertRaisesRegex(ValueError, "stable version tag"):
            self.validate()

    def test_legacy_workflow_uses_explicit_compatibility_policy(self):
        result = validate_recovery_source_details(
            self.run,
            self.jobs,
            self.artifacts,
            "example/project",
            123,
            "name: Release\n# historical workflow without source evidence v1\n",
        )
        self.assertEqual(result["source_evidence_policy"], "legacy_inventory_v0")
        self.assertEqual(result["source_sha"], "c" * 40)
        self.assertEqual(result["source_run_attempt"], "1")

    def test_new_workflow_requires_the_versioned_source_evidence_artifact(self):
        workflow = "name: Release\n- " + SOURCE_EVIDENCE_MARKER + "\n"
        with self.assertRaisesRegex(ValueError, "requires the unexpired schema v1"):
            validate_recovery_source_details(
                self.run, self.jobs, self.artifacts, "example/project", 123, workflow
            )

        self.artifacts["artifacts"].append(
            {"name": SOURCE_EVIDENCE_ARTIFACT, "expired": False}
        )
        result = validate_recovery_source_details(
            self.run, self.jobs, self.artifacts, "example/project", 123, workflow
        )
        self.assertEqual(result["source_evidence_policy"], "manifest_v1")

    def test_evidence_artifact_cannot_be_claimed_by_a_legacy_workflow(self):
        self.artifacts["artifacts"].append(
            {"name": SOURCE_EVIDENCE_ARTIFACT, "expired": False}
        )
        with self.assertRaisesRegex(ValueError, "without its workflow schema marker"):
            validate_recovery_source_details(
                self.run,
                self.jobs,
                self.artifacts,
                "example/project",
                123,
                "name: Release\n",
            )


if __name__ == "__main__":
    unittest.main()
