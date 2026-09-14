import unittest

from check_release_versions import validate_versions


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


if __name__ == "__main__":
    unittest.main()
