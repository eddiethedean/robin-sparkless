import hashlib
import tempfile
import unittest
from pathlib import Path

from unittest.mock import patch

from verify_public_release_416 import (
    PYPI_PLATFORMS_BY_VERSION,
    SPLIT_PYPI_PLATFORMS,
    preserve_existing_pypi_files,
    validate_crate_release,
    validate_pypi_release,
    verify_public_release,
)


def release_metadata(project, version="4.16.0"):
    split = project == "sparkless-native" or version == "4.18.1"
    suffixes = sorted(
        (SPLIT_PYPI_PLATFORMS if split else PYPI_PLATFORMS_BY_VERSION[version])[project]
    )
    wheel_project = project.replace("-", "_")
    wheel_python_abi = (
        "py3-none"
        if project == "sparkless-server-bin" or (split and project == "sparkless")
        else "cp311-abi3"
        if split
        else "cp38-abi3"
    )
    urls = [
        {
            "filename": "{}-{}-{}-{}.whl".format(
                wheel_project, version, wheel_python_abi, platform
            ),
            "digests": {"sha256": "a" * 64},
        }
        for platform in suffixes
    ]
    if project in {"sparkless", "sparkless-native"}:
        urls.append(
            {
                "filename": "{}-{}.tar.gz".format(wheel_project, version),
                "digests": {"sha256": "b" * 64},
            }
        )
    return {"info": {"name": project, "version": version}, "urls": urls}


class VerifyPublicRelease416Test(unittest.TestCase):
    def test_split_api_and_native_inventories_have_independent_versions(self):
        validate_pypi_release(
            release_metadata("sparkless", "4.18.1"), "sparkless", "4.18.1"
        )
        validate_pypi_release(
            release_metadata("sparkless-native", "4.18.0"), "sparkless-native", "4.18.0"
        )

    def test_component_job_can_preserve_only_its_own_platform(self):
        metadata = release_metadata("sparkless-server-bin", "4.18.0")
        contents = b"immutable published component"
        entry = metadata["urls"][0]
        entry["digests"]["sha256"] = hashlib.sha256(contents).hexdigest()
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            wheel = directory / entry["filename"]
            wheel.write_bytes(b"rebuilt bytes")
            with (
                patch("verify_public_release_416._fetch_json", return_value=metadata),
                patch(
                    "verify_public_release_416._fetch_distribution",
                    return_value=contents,
                ),
            ):
                preserve_existing_pypi_files(
                    directory, "sparkless-server-bin", "4.18.0", allow_subset=True
                )
            self.assertEqual(wheel.read_bytes(), contents)

    def test_accepts_complete_server_binary_inventory(self):
        validate_pypi_release(
            release_metadata("sparkless-server-bin"), "sparkless-server-bin"
        )

    def test_accepts_complete_sparkless_inventory(self):
        validate_pypi_release(release_metadata("sparkless"), "sparkless")

    def test_accepts_the_staged_417_server_binary_platforms(self):
        metadata = release_metadata("sparkless-server-bin", "4.17.0")
        validate_pypi_release(metadata, "sparkless-server-bin", version="4.17.0")

    def test_accepts_the_corrective_4161_server_binary_platforms(self):
        metadata = release_metadata("sparkless-server-bin", "4.16.1")
        validate_pypi_release(metadata, "sparkless-server-bin", version="4.16.1")

    def test_accepts_the_corrective_4161_sparkless_platforms(self):
        metadata = release_metadata("sparkless", "4.16.1")
        validate_pypi_release(metadata, "sparkless", version="4.16.1")

    def test_verifies_public_prerequisite_without_local_build_directories(self):
        responses = {
            "https://pypi.org/pypi/sparkless-server-bin/4.16.1/json": release_metadata(
                "sparkless-server-bin", "4.16.1"
            ),
            "https://pypi.org/pypi/sparkless/4.16.1/json": release_metadata(
                "sparkless", "4.16.1"
            ),
        }
        for crate in (
            "spark-sql-parser",
            "robin-sparkless-core",
            "robin-sparkless-polars",
            "robin-sparkless",
            "sparkless-server",
        ):
            responses["https://crates.io/api/v1/crates/{}/4.16.1".format(crate)] = {
                "version": {"num": "4.16.1", "crate": crate}
            }

        def fetch(url):
            return responses[url]

        with (
            patch("verify_public_release_416._fetch_json", side_effect=fetch),
            patch(
                "verify_public_release_416._retry", side_effect=lambda check: check()
            ),
        ):
            verify_public_release(None, None, version="4.16.1")

    def test_417_inventory_requires_exact_published_build_hashes(self):
        metadata = release_metadata("sparkless-server-bin", "4.17.0")
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            for entry in metadata["urls"]:
                path = directory / entry["filename"]
                path.write_bytes(entry["filename"].encode("utf-8"))
                entry["digests"]["sha256"] = hashlib.sha256(
                    path.read_bytes()
                ).hexdigest()
            validate_pypi_release(
                metadata,
                "sparkless-server-bin",
                version="4.17.0",
                local_directory=directory,
            )
            metadata["urls"][0]["digests"]["sha256"] = "a" * 64
            with self.assertRaisesRegex(ValueError, "differ from verified inputs"):
                validate_pypi_release(
                    metadata,
                    "sparkless-server-bin",
                    version="4.17.0",
                    local_directory=directory,
                )

    def test_ignores_pypi_publish_attestation_sidecars(self):
        metadata = release_metadata("sparkless-server-bin", "4.17.0")
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            for entry in metadata["urls"]:
                path = directory / entry["filename"]
                path.write_bytes(entry["filename"].encode("utf-8"))
                entry["digests"]["sha256"] = hashlib.sha256(
                    path.read_bytes()
                ).hexdigest()
                Path(str(path) + ".publish.attestation").write_text(
                    "PyPI publish attestation", encoding="utf-8"
                )
            validate_pypi_release(
                metadata,
                "sparkless-server-bin",
                version="4.17.0",
                local_directory=directory,
            )

    def test_still_rejects_other_unexpected_local_files(self):
        metadata = release_metadata("sparkless-server-bin", "4.17.0")
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            for entry in metadata["urls"]:
                path = directory / entry["filename"]
                path.write_bytes(entry["filename"].encode("utf-8"))
                entry["digests"]["sha256"] = hashlib.sha256(
                    path.read_bytes()
                ).hexdigest()
            (directory / "unexpected.txt").write_text(
                "not a distribution", encoding="utf-8"
            )
            with self.assertRaisesRegex(ValueError, "differ from verified inputs"):
                validate_pypi_release(
                    metadata,
                    "sparkless-server-bin",
                    version="4.17.0",
                    local_directory=directory,
                )

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

    def test_accepts_current_crates_io_version_response(self):
        validate_crate_release(
            {"version": {"num": "4.16.0", "crate": "spark-sql-parser"}},
            "spark-sql-parser",
        )

    def test_accepts_legacy_nested_crates_io_response(self):
        validate_crate_release(
            {
                "version": {"num": "4.16.0"},
                "crate": {"id": "spark-sql-parser"},
            },
            "spark-sql-parser",
        )

    def test_rejects_crates_io_response_for_another_crate_or_version(self):
        with self.assertRaisesRegex(ValueError, "wrong release"):
            validate_crate_release(
                {"version": {"num": "4.16.0", "crate": "other-crate"}},
                "spark-sql-parser",
            )
        with self.assertRaisesRegex(ValueError, "wrong release"):
            validate_crate_release(
                {"version": {"num": "4.15.0", "crate": "spark-sql-parser"}},
                "spark-sql-parser",
            )

    def test_preserves_existing_pypi_bytes_for_immutable_filenames(self):
        metadata = release_metadata("sparkless-server-bin")
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            for entry in metadata["urls"]:
                contents = ("candidate:" + entry["filename"]).encode("utf-8")
                (directory / entry["filename"]).write_bytes(contents)
                entry["digests"]["sha256"] = hashlib.sha256(contents).hexdigest()

            existing = metadata["urls"][0]
            published_bytes = b"immutable PyPI file"
            existing["digests"]["sha256"] = hashlib.sha256(published_bytes).hexdigest()
            existing["url"] = "https://files.pythonhosted.org/packages/file.whl"
            with (
                patch("verify_public_release_416._fetch_json", return_value=metadata),
                patch(
                    "verify_public_release_416._fetch_distribution",
                    return_value=published_bytes,
                ),
            ):
                preserved = preserve_existing_pypi_files(
                    directory, "sparkless-server-bin"
                )

            self.assertEqual(preserved, [existing["filename"]])
            self.assertEqual(
                (directory / existing["filename"]).read_bytes(), published_bytes
            )

    def test_rejects_unexpected_existing_pypi_filename(self):
        metadata = release_metadata("sparkless-server-bin")
        metadata["urls"][0]["filename"] = "unexpected.whl"
        with tempfile.TemporaryDirectory() as temporary:
            with patch("verify_public_release_416._fetch_json", return_value=metadata):
                with self.assertRaisesRegex(ValueError, "not present in the verified"):
                    preserve_existing_pypi_files(
                        Path(temporary), "sparkless-server-bin"
                    )


if __name__ == "__main__":
    unittest.main()
