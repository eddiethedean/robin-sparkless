"""Exercise the complete exact-wheel evidence inventory and rejection paths."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import stat
import tempfile
import unittest

from release_wheel_evidence import write_evidence
from verify_release_wheel_evidence import EXPECTED, EXPECTED_MACHINE, verify


WHEEL_PLATFORM = {
    "manylinux-x86_64": "manylinux_2_17_x86_64.manylinux2014_x86_64",
    "manylinux-aarch64": "manylinux_2_28_aarch64",
    "musllinux-x86_64": "musllinux_1_2_x86_64",
    "musllinux-aarch64": "musllinux_1_2_aarch64",
    "macos-x86_64": "macosx_10_12_x86_64",
    "macos-aarch64": "macosx_11_0_arm64",
    "windows-x86_64": "win_amd64",
    "windows-arm64": "win_arm64",
}


def _write_inventory(root: Path) -> tuple[Path, Path]:
    evidence_dir = root / "evidence"
    wheel_dir = root / "wheels"
    evidence_dir.mkdir()
    wheel_dir.mkdir()
    wheel_paths: dict[str, Path] = {}
    for target, platform_tag in WHEEL_PLATFORM.items():
        wheel = wheel_dir / "sparkless-4.17.0-cp311-abi3-{}.whl".format(platform_tag)
        wheel.write_bytes(("wheel bytes for " + target).encode())
        wheel_paths[target] = wheel
    for target, suite, python_version in EXPECTED:
        wheel = wheel_paths[target]
        digest = hashlib.sha256(wheel.read_bytes()).hexdigest()
        record = {
            "schema_version": 1,
            "mode": "release_candidate",
            "source_sha": "abc123",
            "workflow": "Release",
            "run_id": "42",
            "run_attempt": "1",
            "target": target,
            "platform": "test-platform",
            "platform_tag": WHEEL_PLATFORM[target],
            "machine": EXPECTED_MACHINE[target],
            "python": python_version + ".1",
            "python_architecture": EXPECTED_MACHINE[target],
            "build": {
                "profile": "release",
                "features": ["delta"],
                "rust_toolchain": "1.93.1",
            },
            "artifact": wheel.name,
            "sha256": digest,
            "preinstall_sha256": digest,
            "suite": suite,
            "result": "success",
        }
        filename = "{}-{}-py{}.json".format(target, suite, python_version)
        (evidence_dir / filename).write_text(json.dumps(record))
    return evidence_dir, wheel_dir


class ReleaseWheelEvidenceTests(unittest.TestCase):
    def test_evidence_is_readable_by_the_host_artifact_uploader(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "evidence.json"
            write_evidence({"result": "success"}, output)
            if os.name == "posix":
                self.assertEqual(stat.S_IMODE(output.stat().st_mode), 0o644)

    def test_accepts_complete_digest_bound_candidate_matrix(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            evidence_dir, wheel_dir = _write_inventory(Path(temporary))
            self.assertEqual(
                verify(
                    evidence_dir,
                    wheel_dir,
                    source_sha="abc123",
                    run_id="42",
                    run_attempt="1",
                    workflow="Release",
                    mode="release_candidate",
                ),
                [],
            )

    def test_rejects_missing_cell_and_changed_wheel_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            evidence_dir, wheel_dir = _write_inventory(Path(temporary))
            (evidence_dir / "windows-arm64-platform-runtime-py3.14.json").unlink()
            wheel = next(wheel_dir.glob("*manylinux_2_28_aarch64.whl"))
            wheel.write_bytes(b"modified wheel")
            errors = verify(
                evidence_dir,
                wheel_dir,
                source_sha="abc123",
                run_id="42",
                run_attempt="1",
                workflow="Release",
                mode="release_candidate",
            )
            self.assertTrue(any("missing required" in issue for issue in errors))
            self.assertTrue(any("digest does not match" in issue for issue in errors))


if __name__ == "__main__":
    unittest.main()
