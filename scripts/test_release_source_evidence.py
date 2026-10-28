"""Test the source archive and derived-wheel digest evidence contract."""

from __future__ import annotations

import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from verify_release_source_evidence import record, verify


RELEASE_ENV = {
    "GITHUB_SHA": "b" * 40,
    "GITHUB_WORKFLOW": "Release",
    "GITHUB_RUN_ID": "9876",
    "GITHUB_RUN_ATTEMPT": "1",
    "RELEASE_MODE": "stable_tag",
}


class ReleaseSourceEvidenceTests(unittest.TestCase):
    def test_verifies_sdist_and_derived_wheel_bytes_and_provenance(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            sdist_dir = root / "sdist"
            wheel_dir = root / "wheel"
            sdist_dir.mkdir()
            wheel_dir.mkdir()
            sdist = sdist_dir / "sparkless-4.17.0.tar.gz"
            wheel = wheel_dir / "sparkless-4.17.0-cp311-abi3-linux_x86_64.whl"
            sdist.write_bytes(b"source archive")
            wheel.write_bytes(b"wheel built from extracted source")
            evidence = root / "evidence.json"
            with patch.dict(os.environ, RELEASE_ENV):
                record(sdist, wheel, evidence)
            self.assertEqual(
                verify(
                    evidence,
                    sdist_dir,
                    wheel_dir,
                    source_sha=RELEASE_ENV["GITHUB_SHA"],
                    run_id=RELEASE_ENV["GITHUB_RUN_ID"],
                    run_attempt=RELEASE_ENV["GITHUB_RUN_ATTEMPT"],
                    workflow=RELEASE_ENV["GITHUB_WORKFLOW"],
                    mode=RELEASE_ENV["RELEASE_MODE"],
                ),
                [],
            )

    def test_rejects_a_changed_sdist_and_derived_wheel(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            sdist_dir = root / "sdist"
            wheel_dir = root / "wheel"
            sdist_dir.mkdir()
            wheel_dir.mkdir()
            sdist = sdist_dir / "sparkless-4.17.0.tar.gz"
            wheel = wheel_dir / "sparkless-4.17.0-cp311-abi3-linux_x86_64.whl"
            sdist.write_bytes(b"source archive")
            wheel.write_bytes(b"wheel built from extracted source")
            evidence = root / "evidence.json"
            with patch.dict(os.environ, RELEASE_ENV):
                record(sdist, wheel, evidence)
            sdist.write_bytes(b"altered source archive")
            wheel.write_bytes(b"altered wheel")
            errors = verify(
                evidence,
                sdist_dir,
                wheel_dir,
                source_sha=RELEASE_ENV["GITHUB_SHA"],
                run_id=RELEASE_ENV["GITHUB_RUN_ID"],
                run_attempt=RELEASE_ENV["GITHUB_RUN_ATTEMPT"],
                workflow=RELEASE_ENV["GITHUB_WORKFLOW"],
                mode=RELEASE_ENV["RELEASE_MODE"],
            )
            self.assertTrue(any("source distribution digest" in error for error in errors))
            self.assertTrue(any("derived wheel digest" in error for error in errors))


if __name__ == "__main__":
    unittest.main()
