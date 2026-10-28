import tomllib
import unittest
from copy import deepcopy
from datetime import date
from pathlib import Path

from verify_rust_consumer_graph import (
    audit_decision,
    consumer_manifest,
    verify_consumer_features,
)


class DownstreamConsumerManifestTest(unittest.TestCase):
    def test_workspace_patches_cannot_leak_into_consumer_audit(self):
        source = """[workspace]
members = [".", "python"]
[package]
name = "consumer"
version = "4.17.0"
[patch.crates-io]
zstd = { path = "vendor/fixed" }
[dev-dependencies]
tempfile = "3"
"""
        manifest = tomllib.loads(consumer_manifest(source))
        self.assertNotIn("patch", manifest)
        self.assertNotIn("python", manifest["workspace"]["members"])
        self.assertIn("crates/sparkless-server", manifest["workspace"]["members"])
        self.assertEqual(manifest["dev-dependencies"], {"tempfile": "3"})


class ReleaseExceptionTest(unittest.TestCase):
    def setUp(self):
        self.policy = tomllib.loads(
            (
                Path(__file__).resolve().parents[1]
                / "security/zstd-4.17-exceptions.toml"
            ).read_text()
        )
        self.report = {
            "vulnerabilities": {
                "found": True,
                "count": 3,
                "list": [
                    {
                        "advisory": {"id": e["advisory"]},
                        "package": {
                            "name": e["package"],
                            "version": e["version"],
                            "source": "registry+https://github.com/rust-lang/crates.io-index",
                        },
                    }
                    for e in self.policy["exceptions"]
                ],
            }
        }

    def decide(self, report=None, policy=None):
        return audit_decision(
            report or self.report,
            1,
            policy or self.policy,
            "4.17.0",
            date(2026, 10, 27),
        )

    def test_only_documented_tuples_are_accepted_and_raw_report_is_unchanged(self):
        original = deepcopy(self.report)
        decision = self.decide()
        self.assertEqual(decision["status"], "accepted_exception")
        self.assertEqual(len(decision["accepted"]), 3)
        self.assertEqual(decision["expires_on"], "2026-11-26")
        self.assertEqual(self.report, original)

    def test_new_advisory_package_version_or_source_remains_blocking(self):
        for table, field, replacement in [
            ("advisory", "id", "RUSTSEC-2026-9999"),
            ("package", "name", "other-package"),
            ("package", "version", "0.13.4"),
            ("package", "source", "git+https://example.com/fork"),
        ]:
            with self.subTest(field=field):
                report = deepcopy(self.report)
                report["vulnerabilities"]["list"][0][table][field] = replacement
                decision = self.decide(report)
                self.assertEqual(decision["status"], "blocked")
                self.assertEqual(len(decision["unaccepted"]), 1)

    def test_exception_expires_and_cannot_cover_another_release(self):
        for release, today in [
            ("4.17.0", date(2026, 11, 27)),
            ("4.17.1", date(2026, 10, 27)),
            ("4.17.0", date(2026, 10, 26)),
        ]:
            with self.subTest(release=release, today=today):
                with self.assertRaises(ValueError):
                    audit_decision(self.report, 1, self.policy, release, today)

    def test_strict_audit_still_fails_known_vulnerabilities(self):
        self.assertEqual(
            audit_decision(self.report, 1, None, "4.17.0", date(2026, 10, 27))[
                "status"
            ],
            "blocked",
        )

    def test_tool_errors_and_inconsistent_reports_cannot_be_waived(self):
        for code in (0, 2, 101):
            with self.assertRaises(ValueError):
                audit_decision(
                    self.report, code, self.policy, "4.17.0", date(2026, 10, 27)
                )
        for report in [
            dict(self.report, error="network failure"),
            {"vulnerabilities": dict(self.report["vulnerabilities"], count=0)},
        ]:
            with self.assertRaises(ValueError):
                self.decide(report)

    def test_fixed_upstream_graph_needs_no_exception_even_after_expiry(self):
        report = {"vulnerabilities": {"found": False, "count": 0, "list": []}}
        self.assertEqual(
            audit_decision(report, 0, self.policy, "4.18.0", date(2026, 12, 1))[
                "status"
            ],
            "clean",
        )

    def test_seekable_feature_invalidates_the_exception_scope(self):
        metadata = {
            "packages": [{"id": "safe", "name": "zstd-safe", "version": "7.3.0"}],
            "resolve": {"nodes": [{"id": "safe", "features": ["std"]}]},
        }
        verify_consumer_features(metadata)
        metadata["resolve"]["nodes"][0]["features"].append("seekable")
        with self.assertRaisesRegex(ValueError, "seekable"):
            verify_consumer_features(metadata)
