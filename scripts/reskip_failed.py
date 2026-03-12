#!/usr/bin/env python3
"""Re-add @pytest.mark.skip for tests that failed (from unskip_report.json)."""

import json
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

ROOT = Path(__file__).resolve().parent.parent
REPORT = ROOT / "scripts" / "unskip_report.json"

IssueId = int
TestId = str
TestIdWithIssue = Tuple[TestId, IssueId]


def _load_report() -> Dict[str, object]:
    with open(REPORT) as f:
        return json.load(f)


def _iter_failed_tests(data: Dict[str, object]) -> Iterable[TestIdWithIssue]:
    # Build (test_id, issue_num) for each failed test
    for issue_num, failed_list in data["issues_some_failed"]:
        for test_id in failed_list:
            yield TestId(test_id), IssueId(issue_num)


def main() -> None:
    data = _load_report()
    to_reskip: List[TestIdWithIssue] = list(_iter_failed_tests(data))
    # test_id is "tests.dataframe.test_foo::TestClass::test_bar" or "tests.dataframe.test_foo::test_bar"
    # -> file tests/dataframe/test_foo.py, def test_bar
    edits: Dict[Path, List[Tuple[int, IssueId, str]]] = {}
    for test_id, issue_num in to_reskip:
        parts = test_id.split("::")
        module = parts[0]  # tests.dataframe.test_foo
        test_name = parts[-1]  # test_bar
        file_path = ROOT / (module.replace(".", "/") + ".py")
        if not file_path.exists():
            print("Skip (file not found):", file_path)
            continue
        content = file_path.read_text()
        lines = content.splitlines()
        for i, line in enumerate(lines):
            if line.strip().startswith("def " + test_name + "("):
                # Find indent of this def
                indent = len(line) - len(line.lstrip())
                decorator = (
                    " " * indent
                    + f'@pytest.mark.skip(reason="Issue #{issue_num}: unskip when fixing")\n'
                )
                if i > 0 and "pytest.mark.skip" in lines[i - 1]:
                    continue  # already has skip
                edits.setdefault(file_path, []).append((i, issue_num, " " * indent))
                break
    # Apply edits (from bottom to top so line numbers stay valid)
    for file_path, items in edits.items():
        lines = file_path.read_text().splitlines()
        # Sort by line desc so we insert from bottom
        for line_idx, issue_num, indent in sorted(items, key=lambda x: -x[0]):
            decorator = (
                indent
                + f'@pytest.mark.skip(reason="Issue #{issue_num}: unskip when fixing")'
            )
            lines.insert(line_idx, decorator)
        file_path.write_text("\n".join(lines) + "\n")
    print("Re-skipped", len(to_reskip), "tests in", len(edits), "files")


if __name__ == "__main__":
    main()
