#!/usr/bin/env python3
"""Unskip tests with Issue #N, run pytest, report which issues have all tests passing."""

import json
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TESTS_DIR = ROOT / "tests"


def main() -> list[int]:
    # 1) Build mapping: (file_path, issue_num) -> test_name by parsing files
    issue_tests: dict[
        int, list[str]
    ] = {}  # issue -> list of "path::class::test" or "path::test"
    files_changed: list[Path] = []

    for path in sorted(TESTS_DIR.rglob("*.py")):
        content = path.read_text()
        rel = path.relative_to(ROOT)
        module_id = str(rel.with_suffix("")).replace("/", ".").replace("\\", ".")

        # Find @pytest.mark.skip(reason="Issue #N...") then next def test_xxx
        # Single line
        for m in re.finditer(
            r'\n\s*@pytest\.mark\.skip\(reason="(Issue #(\d+)[^"]*)"\)\s*\n\s*def (test_\w+)\s*\(',
            content,
        ):
            issue_num = int(m.group(2))
            test_name = m.group(3)
            # Check if we're inside a class
            before = content[: m.start()]
            classes = re.findall(r"class (\w+)\s*:", before)
            if classes:
                full_id = f"{module_id}::{classes[-1]}::{test_name}"
            else:
                full_id = f"{module_id}::{test_name}"
            issue_tests.setdefault(issue_num, []).append(full_id)

        # Multiline skip
        for m in re.finditer(
            r'\n\s*@pytest\.mark\.skip\(\s*\n\s*reason="(Issue #(\d+)[^"]*)"\s*\)\s*\n\s*def (test_\w+)\s*\(',
            content,
        ):
            issue_num = int(m.group(2))
            test_name = m.group(3)
            before = content[: m.start()]
            classes = re.findall(r"class (\w+)\s*:", before)
            if classes:
                full_id = f"{module_id}::{classes[-1]}::{test_name}"
            else:
                full_id = f"{module_id}::{test_name}"
            issue_tests.setdefault(issue_num, []).append(full_id)

    # 2) Remove decorators: single-line first
    for path in TESTS_DIR.rglob("*.py"):
        text = path.read_text()
        original = text
        # Single line
        text = re.sub(
            r'\n\s*@pytest\.mark\.skip\(reason="Issue #\d+[^"]*"\)\s*\n',
            "\n",
            text,
        )
        # Multiline
        text = re.sub(
            r'\n\s*@pytest\.mark\.skip\(\s*\n\s*reason="Issue #\d+[^"]*"\s*\)\s*\n',
            "\n",
            text,
        )
        if text != original:
            path.write_text(text)
            files_changed.append(path)

    print(
        "Unskipped",
        sum(len(v) for v in issue_tests.values()),
        "tests across",
        len(issue_tests),
        "issues",
    )
    print("Modified", len(files_changed), "files")

    # 3) Run pytest -v, capture output
    result = subprocess.run(
        [
            str(ROOT / ".venv" / "bin" / "python"),
            "-m",
            "pytest",
            "tests",
            "-v",
            "--tb=no",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=900,
    )
    stdout = result.stdout + result.stderr

    # 4) Parse PASSED / FAILED from "path::test PASSED [ 33%]" or "path::Class::test FAILED"
    passed = set()
    failed = set()
    for line in stdout.splitlines():
        if " PASSED " in line and "::" in line:
            tid = line.split(" PASSED ")[0].strip()
            passed.add(tid)
        if " FAILED " in line and "::" in line:
            tid = line.split(" FAILED ")[0].strip()
            failed.add(tid)

    # Normalize ids: pytest might output tests/dataframe/test_x.py::test_y
    def norm(s: str) -> str:
        s = s.replace("/", ".").replace("\\", ".").replace(".py", "")
        if not s.startswith("tests."):
            s = "tests." + s.lstrip(".")
        return s

    passed = {norm(p) for p in passed}
    failed = {norm(f) for f in failed}

    # 5) For each issue, check if all its tests passed
    issues_all_passed = []
    issues_some_failed = []
    for issue_num in sorted(issue_tests.keys()):
        tests = issue_tests[issue_num]
        normalized = [norm(t) for t in tests]
        if all(t in passed for t in normalized):
            issues_all_passed.append(issue_num)
        else:
            issues_some_failed.append(
                (issue_num, [t for t in normalized if t in failed])
            )

    # Write report
    report = ROOT / "scripts" / "unskip_report.json"
    report.parent.mkdir(exist_ok=True)
    with open(report, "w") as f:
        json.dump(
            {
                "issues_all_passed": issues_all_passed,
                "issues_some_failed": [(i, list(f)) for i, f in issues_some_failed],
                "passed_count": len(passed),
                "failed_count": len(failed),
            },
            f,
            indent=2,
        )
    print("Report written to", report)
    print("Issues with ALL tests passing:", issues_all_passed)
    if issues_some_failed:
        print(
            "Issues with some failures:", [i for i, _ in issues_some_failed[:10]], "..."
        )
    return issues_all_passed


if __name__ == "__main__":
    main()
