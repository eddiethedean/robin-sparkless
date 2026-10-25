import tempfile
import unittest
from pathlib import Path
import tarfile
import zipfile

from verify_release_distributions import verify_distributions


class VerifyReleaseDistributionsTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)

    def make_wheel(self, filename, project="sparkless", version="4.16.0"):
        path = self.directory / filename
        metadata = "Metadata-Version: 2.1\nName: {}\nVersion: {}\n\n".format(
            project, version
        )
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
        self.make_wheel(
            "sparkless-4.16.0-cp38-abi3-manylinux_2_17_x86_64.whl"
        )
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

    def test_rejects_wheel_with_mismatched_metadata(self):
        self.make_wheel(
            "sparkless-4.16.0-cp38-abi3-win_amd64.whl", version="4.15.3"
        )
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


if __name__ == "__main__":
    unittest.main()
