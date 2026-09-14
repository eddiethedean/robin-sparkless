import shutil
import subprocess
import unittest
from pathlib import Path


@unittest.skipUnless(shutil.which("make"), "make is required for the quality gate")
class ReleaseQualityGateTest(unittest.TestCase):
    def assert_nested_failure_propagates(self, crate):
        result = subprocess.run(
            ["make", "check-full", "MAKE=false", f"CRATE={crate}"],
            cwd=Path(__file__).resolve().parents[1],
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertNotIn("check-full: format", result.stdout)
        self.assertNotIn("check-full (crate", result.stdout)

    def test_workspace_failure_propagates(self):
        self.assert_nested_failure_propagates("")

    def test_crate_failure_propagates(self):
        self.assert_nested_failure_propagates("spark-sql-parser")
