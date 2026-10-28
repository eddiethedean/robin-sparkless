"""Test release evidence for the native server wheels and container image."""

from __future__ import annotations

import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

from verify_release_artifacts import (
    record_container,
    record_server_wheel,
    verify_container,
    verify_server_wheels,
)


RELEASE_ENV = {
    "GITHUB_SHA": "a" * 40,
    "GITHUB_WORKFLOW": "Release",
    "GITHUB_RUN_ID": "12345",
    "GITHUB_RUN_ATTEMPT": "2",
    "RELEASE_MODE": "release_candidate",
}


def _write_wheel(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("sparkless_server_bin/__init__.py", "")
        archive.writestr("sparkless_server_bin/sparkless-server", payload)


class ReleaseArtifactEvidenceTests(unittest.TestCase):
    def test_records_and_verifies_exact_server_wheel_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            evidence_dir = root / "evidence"
            wheel_dir = root / "wheels"
            wheel_dir.mkdir()
            wheels = {
                "server-bin-linux-x86_64": wheel_dir
                / "sparkless_server_bin-4.17.0-py3-none-manylinux_2_28_x86_64.whl",
                "server-bin-macos-aarch64": wheel_dir
                / "sparkless_server_bin-4.17.0-py3-none-macosx_14_0_arm64.whl",
            }
            with patch.dict(os.environ, RELEASE_ENV):
                for target, wheel in wheels.items():
                    _write_wheel(wheel, target.encode())
                    record_server_wheel(
                        wheel,
                        target,
                        evidence_dir / (target + ".json"),
                    )
            self.assertEqual(
                verify_server_wheels(
                    evidence_dir,
                    wheel_dir,
                    source_sha=RELEASE_ENV["GITHUB_SHA"],
                    run_id=RELEASE_ENV["GITHUB_RUN_ID"],
                    run_attempt=RELEASE_ENV["GITHUB_RUN_ATTEMPT"],
                    workflow=RELEASE_ENV["GITHUB_WORKFLOW"],
                    mode=RELEASE_ENV["RELEASE_MODE"],
                ),
                [],
            )

    def test_rejects_wheel_bytes_changed_after_runtime_verification(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            evidence_dir = root / "evidence"
            wheel_dir = root / "wheels"
            wheel_dir.mkdir()
            wheels = {
                "server-bin-linux-x86_64": wheel_dir
                / "sparkless_server_bin-4.17.0-py3-none-manylinux_2_28_x86_64.whl",
                "server-bin-macos-aarch64": wheel_dir
                / "sparkless_server_bin-4.17.0-py3-none-macosx_14_0_arm64.whl",
            }
            with patch.dict(os.environ, RELEASE_ENV):
                for target, wheel in wheels.items():
                    _write_wheel(wheel, target.encode())
                    record_server_wheel(
                        wheel, target, evidence_dir / (target + ".json")
                    )
            _write_wheel(wheels["server-bin-linux-x86_64"], b"changed executable")
            errors = verify_server_wheels(
                evidence_dir,
                wheel_dir,
                source_sha=RELEASE_ENV["GITHUB_SHA"],
                run_id=RELEASE_ENV["GITHUB_RUN_ID"],
                run_attempt=RELEASE_ENV["GITHUB_RUN_ATTEMPT"],
                workflow=RELEASE_ENV["GITHUB_WORKFLOW"],
                mode=RELEASE_ENV["RELEASE_MODE"],
            )
            self.assertTrue(
                any("wheel digest does not match" in error for error in errors)
            )

    def test_records_and_verifies_container_archive_and_candidate_identity(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            archive = root / "sparkless-server.tar"
            archive.write_bytes(b"docker archive bytes")
            evidence = root / "container.json"
            image = "sparkless-server:candidate-{}".format(RELEASE_ENV["GITHUB_SHA"])
            image_id = "sha256:" + "1" * 64
            with patch.dict(os.environ, RELEASE_ENV):
                record_container(archive, image, image_id, evidence)
            self.assertEqual(
                verify_container(
                    evidence,
                    archive,
                    source_sha=RELEASE_ENV["GITHUB_SHA"],
                    run_id=RELEASE_ENV["GITHUB_RUN_ID"],
                    run_attempt=RELEASE_ENV["GITHUB_RUN_ATTEMPT"],
                    workflow=RELEASE_ENV["GITHUB_WORKFLOW"],
                    mode=RELEASE_ENV["RELEASE_MODE"],
                ),
                [],
            )
            archive.write_bytes(b"altered bytes")
            errors = verify_container(
                evidence,
                archive,
                source_sha=RELEASE_ENV["GITHUB_SHA"],
                run_id=RELEASE_ENV["GITHUB_RUN_ID"],
                run_attempt=RELEASE_ENV["GITHUB_RUN_ATTEMPT"],
                workflow=RELEASE_ENV["GITHUB_WORKFLOW"],
                mode=RELEASE_ENV["RELEASE_MODE"],
            )
            self.assertTrue(
                any("archive digest does not match" in error for error in errors)
            )

    def test_refuses_to_write_evidence_without_release_run_provenance(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            wheel = (
                root / "sparkless_server_bin-4.17.0-py3-none-manylinux_2_28_x86_64.whl"
            )
            _write_wheel(wheel, b"server")
            with patch.dict(
                os.environ, {"RELEASE_MODE": "manual_validation"}, clear=True
            ):
                with self.assertRaisesRegex(ValueError, "GITHUB_SHA"):
                    record_server_wheel(
                        wheel,
                        "server-bin-linux-x86_64",
                        root / "evidence.json",
                    )


if __name__ == "__main__":
    unittest.main()
