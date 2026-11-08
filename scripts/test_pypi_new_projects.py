"""Tests for the release workflow's PyPI authentication fallback."""

from __future__ import annotations

import contextlib
import io
import os
import unittest
from unittest.mock import patch

import check_pypi_new_projects


class PyPINewProjectTests(unittest.TestCase):
    def test_empty_value_keeps_existing_projects_on_trusted_publishing(self):
        self.assertEqual(check_pypi_new_projects.parse_new_projects(""), [])

    def test_accepts_known_new_projects(self):
        self.assertEqual(
            check_pypi_new_projects.parse_new_projects(
                "sparkless-connect,sparkless-server-duckdb"
            ),
            ["sparkless-connect", "sparkless-server-duckdb"],
        )

    def test_rejects_whitespace_duplicates_and_unknown_projects(self):
        for raw in (
            "sparkless, sparkless-connect",
            "sparkless,sparkless",
            "sparkless-unexpected",
        ):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                check_pypi_new_projects.parse_new_projects(raw)

    def test_token_is_required_only_when_new_projects_are_listed(self):
        with patch.dict(os.environ, {"PYPI_NEW_PROJECTS": ""}, clear=False):
            with patch.dict(os.environ, {}, clear=False):
                os.environ.pop("PYPI_API_TOKEN", None)
                self.assertEqual(check_pypi_new_projects.main(), 0)

        output = io.StringIO()
        with patch.dict(
            os.environ,
            {"PYPI_NEW_PROJECTS": "sparkless-connect"},
            clear=False,
        ):
            os.environ.pop("PYPI_API_TOKEN", None)
            with contextlib.redirect_stderr(output):
                self.assertEqual(check_pypi_new_projects.main(), 1)
            self.assertIn("PYPI_API_TOKEN", output.getvalue())

        with patch.dict(
            os.environ,
            {
                "PYPI_NEW_PROJECTS": "sparkless-connect",
                "PYPI_API_TOKEN": "test-token",
            },
            clear=False,
        ):
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(check_pypi_new_projects.main(), 0)


if __name__ == "__main__":
    unittest.main()
