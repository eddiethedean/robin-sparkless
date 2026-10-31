"""Regression tests for PyPI's per-file and cumulative upload limits."""

from __future__ import annotations

import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

from check_pypi_storage import check_project_storage, fetch_project_files
from verify_release_distributions import (
    PYPI_FILE_LIMIT_BYTES,
    PYPI_PROJECT_LIMIT_BYTES,
)


class PyPIStorageCheckTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)

    def make_file(self, name: str, content: bytes = b"candidate") -> Path:
        path = self.directory / name
        path.write_bytes(content)
        return path

    def test_reads_existing_file_sizes_and_hashes_from_pypi_json(self):
        payload = {
            "releases": {
                "1.0": [
                    {
                        "filename": "sparkless-1.0.whl",
                        "digests": {"sha256": "a" * 64},
                        "size": 123,
                    }
                ]
            }
        }
        with patch(
            "check_pypi_storage.urlopen",
            return_value=io.BytesIO(json.dumps(payload).encode()),
        ) as mocked_urlopen:
            files = fetch_project_files("sparkless")

        self.assertEqual(files, {"sparkless-1.0.whl": ("a" * 64, 123)})
        self.assertEqual(
            mocked_urlopen.call_args.args[0].full_url,
            "https://pypi.org/pypi/sparkless/json",
        )

    def test_new_pypi_project_has_no_existing_storage(self):
        missing = HTTPError(
            "https://pypi.org/pypi/new-project/json", 404, "Not Found", {}, None
        )
        with patch("check_pypi_storage.urlopen", side_effect=missing):
            self.assertEqual(fetch_project_files("new-project"), {})

    def test_new_project_fits_with_candidate_files(self):
        first = self.make_file("sparkless_native-4.18.0.whl", b"wheel")
        second = self.make_file("sparkless_native-4.18.0.tar.gz", b"source")

        current, added, projected = check_project_storage(self.directory, {})

        expected = first.stat().st_size + second.stat().st_size
        self.assertEqual((current, added, projected), (0, expected, expected))

    def test_identical_existing_file_does_not_use_additional_quota(self):
        path = self.make_file("sparkless-4.18.1.whl", b"same bytes")
        digest = hashlib.sha256(path.read_bytes()).hexdigest()

        current, added, projected = check_project_storage(
            self.directory, {path.name: (digest, path.stat().st_size)}
        )

        self.assertEqual((current, added, projected), (path.stat().st_size, 0, current))

    def test_existing_filename_with_different_bytes_is_rejected(self):
        path = self.make_file("sparkless-4.18.1.whl", b"new bytes")
        with self.assertRaisesRegex(ValueError, "different contents"):
            check_project_storage(self.directory, {path.name: ("0" * 64, 10)})

    def test_individual_files_cannot_exceed_pypi_limit(self):
        path = self.directory / "oversized.whl"
        with path.open("wb") as oversized:
            oversized.truncate(PYPI_FILE_LIMIT_BYTES + 1)

        with self.assertRaisesRegex(ValueError, "100 MiB per-file limit"):
            check_project_storage(self.directory, {})

    def test_cumulative_project_size_cannot_exceed_pypi_limit(self):
        self.make_file("new.whl", b"new")
        published = {"old.whl": ("a" * 64, PYPI_PROJECT_LIMIT_BYTES - 2)}

        with self.assertRaisesRegex(ValueError, "10 GiB project limit"):
            check_project_storage(self.directory, published)


if __name__ == "__main__":
    unittest.main()
