import hashlib
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
import zipfile

from release_inventory import distribution_specs
from verify_candidate_inventory import (
    build_candidate_inventory,
    verify_published_inputs,
)


class ReleaseInventoryTests(unittest.TestCase):
    def _candidate_root(self, root: Path) -> None:
        (root / "python").mkdir(parents=True)
        (root / "python/native").mkdir(parents=True)
        (root / "python/pyproject.toml").write_text('[project]\nversion = "4.19.0"\n')
        (root / "python/native/pyproject.toml").write_text(
            '[project]\nversion = "4.19.0"\n'
        )
        dist = root / "dist"
        for spec in distribution_specs("4.19.0", "4.19.0"):
            directory = dist / spec.directory
            directory.mkdir(parents=True)
            normalized = spec.project.replace("-", "_")
            dependencies = "".join(
                f"Requires-Dist: {dependency}\n"
                for dependency in sorted(spec.required_dependencies)
            )
            metadata = (
                "Metadata-Version: 2.1\n"
                f"Name: {spec.project}\n"
                "Version: 4.19.0\n"
                "Requires-Python: >=3.11\n"
                f"{dependencies}\n"
            )
            if spec.project == "sparkless":
                python_tag, abi_tag = "py3", "none"
            elif spec.project == "sparkless-native":
                python_tag, abi_tag = "cp311", "abi3"
            else:
                python_tag, abi_tag = "py3", "none"
            for platform in sorted(spec.platforms):
                filename = f"{normalized}-4.19.0-{python_tag}-{abi_tag}-{platform}.whl"
                with zipfile.ZipFile(directory / filename, "w") as archive:
                    archive.writestr(
                        f"{normalized}-4.19.0.dist-info/METADATA", metadata
                    )
            if spec.sdists:
                filename = f"{spec.project}-4.19.0.tar.gz"
                with tarfile.open(directory / filename, "w:gz") as archive:
                    payload = metadata.encode()
                    info = tarfile.TarInfo(f"{spec.project}-4.19.0/PKG-INFO")
                    info.size = len(payload)
                    archive.addfile(info, io.BytesIO(payload))

    def test_inventory_lists_every_419_python_project_and_platform(self):
        specs = distribution_specs("4.19.0", "4.19.0")
        self.assertEqual(
            [spec.project for spec in specs],
            [
                "sparkless",
                "sparkless-native",
                "sparkless-server",
                "sparkless-server-duckdb",
                "sparkless-connect",
                "sparkless-testing",
            ],
        )
        self.assertEqual(sum(spec.wheels for spec in specs), 15)
        self.assertEqual(sum(spec.sdists for spec in specs), 4)
        self.assertEqual(
            next(
                spec for spec in specs if spec.project == "sparkless-native"
            ).platforms,
            frozenset(
                {
                    "macosx_10_12_x86_64",
                    "macosx_11_0_arm64",
                    "manylinux2014_x86_64.manylinux_2_17_x86_64",
                    "manylinux_2_28_aarch64",
                    "musllinux_1_2_aarch64",
                    "musllinux_1_2_x86_64",
                    "win_amd64",
                    "win_arm64",
                }
            ),
        )

    def test_publisher_accepts_the_exact_verified_bytes(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            directory = root / "dist"
            directory.mkdir()
            payload = b"candidate wheel bytes"
            filename = "sparkless_connect-4.19.0-py3-none-any.whl"
            (directory / filename).write_bytes(payload)
            manifest = root / "candidate-inventory.json"
            manifest.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "source_revision": "a" * 40,
                        "distributions": {
                            "sparkless-connect": {
                                "files": [
                                    {
                                        "name": filename,
                                        "size_bytes": len(payload),
                                        "sha256": hashlib.sha256(payload).hexdigest(),
                                    }
                                ]
                            }
                        },
                    }
                )
            )
            verify_published_inputs(manifest, directory, ["sparkless-connect"])

            (directory / filename).write_bytes(b"replaced wheel bytes")
            with self.assertRaisesRegex(
                ValueError, "changed after candidate verification"
            ):
                verify_published_inputs(manifest, directory, ["sparkless-connect"])

    def test_publisher_rejects_missing_or_extra_files(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            directory = root / "dist"
            directory.mkdir()
            manifest = root / "candidate-inventory.json"
            manifest.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "source_revision": "a" * 40,
                        "distributions": {
                            "sparkless-connect": {
                                "files": [
                                    {
                                        "name": "expected.whl",
                                        "size_bytes": 1,
                                        "sha256": hashlib.sha256(b"x").hexdigest(),
                                    }
                                ]
                            }
                        },
                    }
                )
            )
            with self.assertRaisesRegex(ValueError, "inputs differ"):
                verify_published_inputs(manifest, directory, ["sparkless-connect"])

            with self.assertRaisesRegex(ValueError, "different source revision"):
                verify_published_inputs(
                    manifest,
                    directory,
                    ["sparkless-connect"],
                    expected_revision="b" * 40,
                )

            (directory / "expected.whl").touch()
            (directory / "extra.whl").touch()
            with self.assertRaisesRegex(ValueError, "inputs differ"):
                verify_published_inputs(manifest, directory, ["sparkless-connect"])

    def test_full_candidate_build_writes_one_hash_bound_manifest(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self._candidate_root(root)
            inventory = build_candidate_inventory(root, "a" * 40)

            self.assertEqual(inventory["source_revision"], "a" * 40)
            self.assertEqual(len(inventory["distributions"]), 6)
            self.assertEqual(
                sum(
                    len(entry["files"]) for entry in inventory["distributions"].values()
                ),
                19,
            )
            for entry in inventory["distributions"].values():
                for file in entry["files"]:
                    self.assertEqual(len(file["sha256"]), 64)
                    self.assertGreater(file["size_bytes"], 0)

    def test_full_candidate_build_rejects_an_uninventoried_package(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self._candidate_root(root)
            (root / "dist/legacy-server-bin").mkdir()
            with self.assertRaisesRegex(ValueError, "directories differ"):
                build_candidate_inventory(root, "a" * 40)


if __name__ == "__main__":
    unittest.main()
