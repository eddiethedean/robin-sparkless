import unittest
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory

from check_release_versions import (
    check_release,
    validate_server_inventory,
    validate_versions,
)


class ReleaseVersionsTest(unittest.TestCase):
    def setUp(self):
        self.packages = {
            name: "4.13.3"
            for name in (
                "robin-sparkless",
                "robin-sparkless-core",
                "robin-sparkless-polars",
                "sparkless-native",
                "sparkless",
            )
        }
        self.dependencies = {"facade/core": "4.13.3", "facade/polars": "4.13.3"}
        self.lock = self.packages.copy()

    def validate(self, **overrides):
        arguments = {
            "package_versions": self.packages,
            "dependency_versions": self.dependencies,
            "lock_versions": self.lock,
            "python_version": "4.13.3",
            "changelog": "## [4.13.3] - 2026-09-14\n",
            "tag": "v4.13.3",
        }
        arguments.update(overrides)
        return validate_versions(**arguments)

    def test_matching_metadata(self):
        self.assertEqual(self.validate(), [])
        self.assertEqual(self.validate(tag=None), [])

    def test_package_mismatch(self):
        self.packages["sparkless-native"] = "4.13.2"
        self.assertTrue(
            any("sparkless-native: version" in error for error in self.validate())
        )

    def test_lock_mismatch(self):
        self.lock["robin-sparkless-core"] = "4.13.2"
        self.assertTrue(any("Cargo.lock" in error for error in self.validate()))

    def test_dependency_mismatch(self):
        self.dependencies["facade/core"] = "4.13.2"
        self.assertTrue(any("dependency version" in error for error in self.validate()))

    def test_python_mismatch(self):
        self.assertTrue(self.validate(python_version="4.13.2"))

    def test_tag_mismatch(self):
        self.assertTrue(any("tag" in error for error in self.validate(tag="v4.13.2")))

    def test_changelog_required(self):
        self.assertTrue(self.validate(changelog="## [Unreleased]\n"))
        self.assertTrue(self.validate(changelog="## [4.13.3] - 2026-99-14\n"))

    def test_stable_version_required(self):
        self.packages["robin-sparkless"] = "4.13.3-rc.1"
        self.assertTrue(any("semantic version" in error for error in self.validate()))

    def test_current_server_binary_and_extra_are_coupled(self):
        root = Path(__file__).resolve().parents[1]
        self.assertEqual(validate_server_inventory(root), [])
        self.assertEqual(check_release(root), [])

    def test_coordinated_candidate_requires_parser_dependency_alignment(self):
        source = Path(__file__).resolve().parents[1]
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifests = (
                "Cargo.toml",
                "Cargo.lock",
                "crates/robin-sparkless-core/Cargo.toml",
                "crates/robin-sparkless-polars/Cargo.toml",
                "crates/spark-sql-parser/Cargo.toml",
                "crates/sparkless-server/Cargo.toml",
                "python/pyproject.toml",
                "python/server-bin/pyproject.toml",
            )
            for relative in manifests:
                destination = root / relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source / relative, destination)

            server_manifest = root / "crates/sparkless-server/Cargo.toml"
            server_manifest.write_text(
                server_manifest.read_text().replace(
                    'spark-sql-parser = { version = "4.17.0"',
                    'spark-sql-parser = { version = "4.16.0"',
                    1,
                )
            )
            errors = validate_server_inventory(root, "v4.17.0")
            self.assertTrue(
                any("sparkless-server/spark-sql-parser" in error for error in errors)
            )

    def test_candidate_branch_requires_parser_manifest_and_lock_alignment(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            files = {
                "Cargo.toml": '[package]\nname="robin-sparkless"\nversion="4.17.0"\n',
                "crates/robin-sparkless-core/Cargo.toml": '[package]\nname="robin-sparkless-core"\nversion="4.17.0"\n',
                "crates/robin-sparkless-polars/Cargo.toml": (
                    '[package]\nname="robin-sparkless-polars"\nversion="4.17.0"\n'
                    '[dependencies]\nspark-sql-parser={version="4.17.0"}\n'
                ),
                "crates/spark-sql-parser/Cargo.toml": '[package]\nname="spark-sql-parser"\nversion="4.17.0"\n',
                "crates/sparkless-server/Cargo.toml": (
                    '[package]\nname="sparkless-server"\nversion="4.17.0"\n'
                    '[dependencies]\nrobin-sparkless={version="4.17.0"}\n'
                    'robin-sparkless-core={version="4.17.0"}\n'
                    'spark-sql-parser={version="4.17.0"}\n'
                ),
                "Cargo.lock": "".join(
                    '[[package]]\nname="{}"\nversion="4.17.0"\n'.format(name)
                    for name in (
                        "sparkless-server",
                        "spark-sql-parser",
                    )
                ),
                "python/pyproject.toml": (
                    "[project]\n[project.optional-dependencies]\n"
                    'server=["sparkless-server-bin==4.17.0"]\n'
                ),
                "python/server-bin/pyproject.toml": (
                    '[project]\nname="sparkless-server-bin"\nversion="4.17.0"\n'
                ),
            }
            for relative, contents in files.items():
                path = root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(contents)

            self.assertEqual(validate_server_inventory(root), [])
            for relative, label in (
                (
                    "crates/robin-sparkless-polars/Cargo.toml",
                    "robin-sparkless-polars/spark-sql-parser",
                ),
                (
                    "crates/sparkless-server/Cargo.toml",
                    "sparkless-server/spark-sql-parser",
                ),
            ):
                manifest = root / relative
                manifest.write_text(
                    manifest.read_text().replace(
                        'spark-sql-parser={version="4.17.0"}',
                        'spark-sql-parser={version="4.16.0"}',
                    )
                )
                errors = validate_server_inventory(root)
                self.assertTrue(any(label in error for error in errors), errors)
                manifest.write_text(
                    manifest.read_text().replace(
                        'spark-sql-parser={version="4.16.0"}',
                        'spark-sql-parser={version="4.17.0"}',
                    )
                )
            parser_manifest = root / "crates/spark-sql-parser/Cargo.toml"
            parser_manifest.write_text(
                '[package]\nname="spark-sql-parser"\nversion="0.5.0"\n'
            )
            errors = validate_server_inventory(root)
            self.assertTrue(
                any("spark-sql-parser: version" in error for error in errors)
            )
            self.assertTrue(
                any("spark-sql-parser: Cargo.lock" in error for error in errors)
            )


if __name__ == "__main__":
    unittest.main()
