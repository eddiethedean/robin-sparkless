import tempfile
import unittest
from pathlib import Path
import tarfile
import zipfile

from verify_release_distributions import (
    PYPI_FILE_LIMIT_BYTES,
    PYPI_PROJECT_LIMIT_BYTES,
    verify_distributions,
)


class VerifyReleaseDistributionsTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)

    def make_wheel(
        self,
        filename,
        project="sparkless",
        version="4.16.0",
        requires_python=None,
        dependency=None,
    ):
        path = self.directory / filename
        metadata = "Metadata-Version: 2.1\nName: {}\nVersion: {}\n\n".format(
            project, version
        )
        if requires_python:
            metadata = (
                metadata.rstrip() + "\nRequires-Python: " + requires_python + "\n\n"
            )
        if dependency:
            metadata = metadata.rstrip() + "\nRequires-Dist: " + dependency + "\n\n"
        with zipfile.ZipFile(path, "w") as archive:
            archive.writestr(
                "{}-{}.dist-info/METADATA".format(project, version), metadata
            )
        return path

    def make_sdist(self, project="sparkless", version="4.16.0"):
        path = self.directory / "{}-{}.tar.gz".format(project, version)
        metadata = "Metadata-Version: 2.1\nName: {}\nVersion: {}\n\n".format(
            project, version
        )
        with tarfile.open(path, "w:gz") as archive:
            data = metadata.encode("utf-8")
            info = tarfile.TarInfo("{}-{}/PKG-INFO".format(project, version))
            info.size = len(data)
            import io

            archive.addfile(info, io.BytesIO(data))
        return path

    def test_accepts_exact_wheel_and_sdist_inventory(self):
        self.make_wheel("sparkless-4.16.0-cp38-abi3-manylinux_2_17_x86_64.whl")
        self.make_sdist()
        files = verify_distributions(
            self.directory,
            "sparkless",
            "4.16.0",
            wheel_count=1,
            sdist_count=1,
            platforms={"manylinux_2_17_x86_64"},
        )
        self.assertEqual(len(files), 2)

    def test_accepts_canonical_manylinux2014_dual_platform_tag(self):
        self.make_wheel(
            "sparkless-4.18.0-cp311-abi3-manylinux2014_x86_64.manylinux_2_17_x86_64.whl",
            version="4.18.0",
            requires_python=">=3.11",
        )
        verify_distributions(
            self.directory,
            "sparkless",
            "4.18.0",
            wheel_count=1,
            sdist_count=0,
            platforms={"manylinux2014_x86_64.manylinux_2_17_x86_64"},
        )

    def test_rejects_wheel_with_mismatched_metadata(self):
        self.make_wheel("sparkless-4.16.0-cp38-abi3-win_amd64.whl", version="4.15.3")
        with self.assertRaisesRegex(ValueError, "has version 4.15.3"):
            verify_distributions(
                self.directory,
                "sparkless",
                "4.16.0",
                wheel_count=1,
                sdist_count=0,
            )

    def test_rejects_incomplete_or_wrong_platform_inventory(self):
        self.make_wheel("sparkless-4.16.0-cp38-abi3-win_amd64.whl")
        with self.assertRaisesRegex(ValueError, "wheel platforms differ"):
            verify_distributions(
                self.directory,
                "sparkless",
                "4.16.0",
                wheel_count=1,
                sdist_count=0,
                platforms={"macosx_11_0_arm64"},
            )

    def test_accepts_normalized_server_binary_package_metadata(self):
        self.make_wheel(
            "sparkless_server_bin-4.16.0-py3-none-win_amd64.whl",
            project="sparkless-server-bin",
        )
        verify_distributions(
            self.directory,
            "sparkless-server-bin",
            "4.16.0",
            wheel_count=1,
            sdist_count=0,
            platforms={"win_amd64"},
        )

    def test_417_requires_python_311_and_the_new_abi_floor(self):
        wheel = self.make_wheel(
            "sparkless-4.17.0-cp38-abi3-win_amd64.whl",
            version="4.17.0",
            requires_python=">=3.11",
        )
        with self.assertRaisesRegex(ValueError, "cp311-abi3"):
            verify_distributions(self.directory, "sparkless", "4.17.0", 1, 0)
        wheel.unlink()
        wheel = self.make_wheel(
            "sparkless-4.17.0-cp311-abi3-win_amd64.whl",
            version="4.17.0",
            requires_python=">=3.8",
        )
        with self.assertRaisesRegex(ValueError, "Requires-Python"):
            verify_distributions(self.directory, "sparkless", "4.17.0", 1, 0)
        wheel.unlink()
        self.make_wheel(
            "sparkless-4.17.0-cp311-abi3-win_amd64.whl",
            version="4.17.0",
            requires_python=">=3.11",
        )
        verify_distributions(self.directory, "sparkless", "4.17.0", 1, 0)

    def test_split_api_requires_universal_wheel_and_exact_native_dependency(self):
        wheel = self.make_wheel(
            "sparkless-4.18.1-py3-none-any.whl",
            version="4.18.1",
            requires_python=">=3.11",
            dependency="sparkless-native==4.18.0",
        )
        arguments = (self.directory, "sparkless", "4.18.1", 1, 0, {"any"})
        verify_distributions(
            *arguments, required_dependencies={"sparkless-native==4.18.0"}
        )
        with self.assertRaisesRegex(ValueError, "exact component"):
            verify_distributions(
                *arguments, required_dependencies={"sparkless-native==4.17.0"}
            )
        with zipfile.ZipFile(wheel, "a") as archive:
            archive.writestr("sparkless/_native.abi3.so", b"stale extension")
        with self.assertRaisesRegex(ValueError, "must not bundle native"):
            verify_distributions(*arguments)

    def test_native_distribution_retains_abi_floor(self):
        self.make_wheel(
            "sparkless_native-4.18.0-cp38-abi3-win_arm64.whl",
            project="sparkless-native",
            version="4.18.0",
            requires_python=">=3.11",
        )
        with self.assertRaisesRegex(ValueError, "cp311-abi3"):
            verify_distributions(self.directory, "sparkless-native", "4.18.0", 1, 0)

    def test_rejects_a_distribution_over_the_pypi_file_limit(self):
        oversized = self.directory / "oversized.whl"
        with oversized.open("wb") as distribution:
            distribution.truncate(PYPI_FILE_LIMIT_BYTES + 1)

        with self.assertRaisesRegex(ValueError, "100 MiB per-file limit"):
            verify_distributions(self.directory, "sparkless", "4.18.1", 0, 0)

    def test_rejects_candidate_inventory_over_the_pypi_project_limit(self):
        count = PYPI_PROJECT_LIMIT_BYTES // PYPI_FILE_LIMIT_BYTES + 1
        for index in range(count):
            oversized_inventory_file = self.directory / f"candidate-{index}.whl"
            with oversized_inventory_file.open("wb") as distribution:
                distribution.truncate(PYPI_FILE_LIMIT_BYTES)

        with self.assertRaisesRegex(ValueError, "10 GiB project limit"):
            verify_distributions(self.directory, "sparkless-native", "4.18.0", count, 0)


if __name__ == "__main__":
    unittest.main()
