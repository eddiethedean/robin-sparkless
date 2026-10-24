import hashlib
import tempfile
import unittest
from pathlib import Path

from verify_public_release_416 import PYPI_PLATFORMS, validate_pypi_release


def release_metadata(project, version="4.16.0"):
    suffixes = sorted(PYPI_PLATFORMS[project])
    wheel_project = project.replace("-", "_")
    urls = [
        {
            "filename": "{}-{}-cp38-abi3-{}.whl".format(
                wheel_project, version, platform
            ),
            "digests": {"sha256": "a" * 64},
        }
        for platform in suffixes
    ]
    if project == "sparkless":
        urls.append(
            {
                "filename": "sparkless-{}.tar.gz".format(version),
                "digests": {"sha256": "b" * 64},
            }
        )
    return {"info": {"name": project, "version": version}, "urls": urls}


class VerifyPublicRelease416Test(unittest.TestCase):
    def test_accepts_complete_server_binary_inventory(self):
        validate_pypi_release(release_metadata("sparkless-server-bin"), "sparkless-server-bin")

    def test_accepts_complete_sparkless_inventory(self):
        validate_pypi_release(release_metadata("sparkless"), "sparkless")

    def test_rejects_missing_wheel_platform(self):
        metadata = release_metadata("sparkless")
        metadata["urls"].pop(0)
        with self.assertRaisesRegex(ValueError, "wheel platforms differ"):
            validate_pypi_release(metadata, "sparkless")

    def test_rejects_incomplete_digest(self):
        metadata = release_metadata("sparkless-server-bin")
        metadata["urls"][0]["digests"]["sha256"] = "bad"
        with self.assertRaisesRegex(ValueError, "malformed SHA-256"):
            validate_pypi_release(metadata, "sparkless-server-bin")

    def test_checks_published_hashes_against_verified_builds(self):
        metadata = release_metadata("sparkless-server-bin")
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            for entry in metadata["urls"]:
                local_file = directory / entry["filename"]
                local_file.write_bytes(entry["filename"].encode("utf-8"))
                entry["digests"]["sha256"] = hashlib.sha256(
                    local_file.read_bytes()
                ).hexdigest()
            validate_pypi_release(
                metadata, "sparkless-server-bin", local_directory=directory
            )

    def test_rejects_published_hash_that_differs_from_verified_build(self):
        metadata = release_metadata("sparkless-server-bin")
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            for entry in metadata["urls"]:
                (directory / entry["filename"]).write_bytes(
                    entry["filename"].encode("utf-8")
                )
            with self.assertRaisesRegex(ValueError, "differ from verified inputs"):
                validate_pypi_release(
                    metadata, "sparkless-server-bin", local_directory=directory
                )


if __name__ == "__main__":
    unittest.main()
