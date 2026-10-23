from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import check_server_release_readiness


class ReleaseReadinessTests(unittest.TestCase):
    def run_gate(self, gate_rows: str) -> int:
        document = (
            "## 4.16 release publication gate\n"
            "| Gate | Status |\n| --- | --- |\n"
            f"{gate_rows}\n"
            "## Test implementation guidance\n"
        )
        with tempfile.TemporaryDirectory() as directory:
            validation_file = Path(directory) / "VALIDATION.md"
            validation_file.write_text(document, encoding="utf-8")
            with patch.object(check_server_release_readiness, "VALIDATION_FILE", validation_file):
                return check_server_release_readiness.main()

    def test_all_required_gates_must_pass(self) -> None:
        passing_rows = "\n".join(
            f"| {gate} | passed |" for gate in check_server_release_readiness.REQUIRED_GATES
        )
        self.assertEqual(self.run_gate(passing_rows), 0)

    def test_pending_gate_blocks_publication(self) -> None:
        rows = "\n".join(
            f"| {gate} | {'pending' if gate == 'V09' else 'passed'} |"
            for gate in check_server_release_readiness.REQUIRED_GATES
        )
        self.assertEqual(self.run_gate(rows), 1)

    def test_duplicate_gate_blocks_publication(self) -> None:
        rows = "\n".join(
            f"| {gate} | passed |" for gate in check_server_release_readiness.REQUIRED_GATES
        ) + "\n| V05 | passed |"
        self.assertEqual(self.run_gate(rows), 1)

    def test_upstream_integration_does_not_block_http_release(self) -> None:
        rows = "\n".join(
            f"| V{number:02} | passed |" for number in range(5, 15)
        ) + "\n| V29 | passed |"
        self.assertEqual(self.run_gate(rows), 0)
        upstream = "\n".join(f"| V{number:02} | blocked |" for number in range(1, 5))
        self.assertEqual(self.run_gate(rows + "\n" + upstream), 0)

    def test_missing_native_gate_still_blocks_publication(self) -> None:
        rows = "\n".join(
            f"| V{number:02} | passed |" for number in range(5, 15) if number != 10
        ) + "\n| V29 | passed |"
        self.assertEqual(self.run_gate(rows), 1)


if __name__ == "__main__":
    unittest.main()
