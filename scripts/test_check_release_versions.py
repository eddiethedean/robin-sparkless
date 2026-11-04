import unittest
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory

from check_release_versions import (
    check_release,
    read_toml,
    validate_server_inventory,
    validate_versions,
)


SERVER_INVENTORY_FILES = (
    "Cargo.toml",
    "Cargo.lock",
    "crates/robin-sparkless-core/Cargo.toml",
    "crates/robin-sparkless-duckdb/Cargo.toml",
    "crates/robin-sparkless-polars/Cargo.toml",
    "crates/spark-sql-parser/Cargo.toml",
    "crates/sparkless-server-core/Cargo.toml",
    "crates/sparkless-server-runtime/Cargo.toml",
    "crates/sparkless-server-http/Cargo.toml",
    "crates/sparkless-server-connect/Cargo.toml",
    "crates/sparkless-server-polars/Cargo.toml",
    "crates/sparkless-server/Cargo.toml",
    "crates/sparkless-server-duckdb/Cargo.toml",
    "python/pyproject.toml",
    "python/connect/pyproject.toml",
    "python/server/pyproject.toml",
    "python/server-duckdb/pyproject.toml",
    "python/testing/pyproject.toml",
)


def copy_server_inventory(source, destination):
    for relative in SERVER_INVENTORY_FILES:
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source / relative, target)


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

    def test_python_patch_can_reuse_a_versioned_engine(self):
        self.assertEqual(
            self.validate(
                release_version="4.13.4",
                python_version="4.13.4",
                changelog="## [4.13.4] - 2026-10-30\n",
                tag="v4.13.4",
            ),
            [],
        )

    def test_api_dependency_cannot_silently_drift_from_the_engine(self):
        source = Path(__file__).resolve().parents[1]
        from unittest.mock import patch
        from check_release_versions import read_toml

        def changed(path):
            result = read_toml(path)
            if path == source / "python/pyproject.toml":
                result["project"]["dependencies"] = ["sparkless-native>=4.18.0"]
            return result

        with patch("check_release_versions.read_toml", side_effect=changed):
            errors = check_release(source)
        self.assertTrue(
            any("pin the exact sparkless-native" in error for error in errors)
        )

    def test_coordinated_candidate_requires_parser_dependency_alignment(self):
        source = Path(__file__).resolve().parents[1]
        candidate_version = read_toml(source / "Cargo.toml")["package"]["version"]
        stale_version = "4.17.0" if candidate_version != "4.17.0" else "4.16.0"
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            copy_server_inventory(source, root)

            polars_manifest = root / "crates/robin-sparkless-polars/Cargo.toml"
            polars_manifest.write_text(
                polars_manifest.read_text().replace(
                    f'spark-sql-parser = {{ version = "{candidate_version}"',
                    f'spark-sql-parser = {{ version = "{stale_version}"',
                    1,
                )
            )
            errors = validate_server_inventory(root, f"v{candidate_version}")
            self.assertTrue(
                any(
                    "robin-sparkless-polars/spark-sql-parser" in error
                    for error in errors
                )
            )

    def test_candidate_branch_requires_parser_manifest_and_lock_alignment(self):
        source = Path(__file__).resolve().parents[1]
        candidate_version = read_toml(source / "Cargo.toml")["package"]["version"]
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            copy_server_inventory(source, root)
            self.assertEqual(validate_server_inventory(root, "v4.17.0"), [])

            server_polars_manifest = root / "crates/sparkless-server-polars/Cargo.toml"
            server_polars_manifest.write_text(
                server_polars_manifest.read_text().replace(
                    f'spark-sql-parser = {{ version = "{candidate_version}"',
                    'spark-sql-parser = { version = "4.16.0"',
                    1,
                )
            )
            errors = validate_server_inventory(root, "v4.17.0")
            self.assertTrue(
                any(
                    "sparkless-server-polars/spark-sql-parser" in error
                    for error in errors
                ),
                errors,
            )
            server_polars_manifest.write_text(
                server_polars_manifest.read_text().replace(
                    'spark-sql-parser = { version = "4.16.0"',
                    f'spark-sql-parser = {{ version = "{candidate_version}"',
                    1,
                )
            )
            parser_manifest = root / "crates/spark-sql-parser/Cargo.toml"
            parser_manifest.write_text(
                '[package]\nname="spark-sql-parser"\nversion="0.5.0"\n'
            )
            errors = validate_server_inventory(root, "v4.17.0")
            self.assertTrue(
                any("spark-sql-parser: version" in error for error in errors)
            )
            self.assertTrue(
                any("spark-sql-parser: Cargo.lock" in error for error in errors)
            )


if __name__ == "__main__":
    unittest.main()
