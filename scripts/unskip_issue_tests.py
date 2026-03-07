#!/usr/bin/env python3
"""Unskip tests marked with Issue #N, record mapping, run tests, report and optionally re-skip failures."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

TESTS_DIR = Path(__file__).resolve().parent.parent / "tests"


def extract_issue_number(reason: str) -> Optional[int]:
    m = re.search(r"Issue #(\d+)", reason)
    return int(m.group(1)) if m else None


def unskip_file(path: Path) -> List[Tuple[str, int]]:
    """Remove @pytest.mark.skip(reason=\"Issue #N...\") from file. Return list of (test_name, issue_num)."""
    text = path.read_text()
    mapping: List[Tuple[str, int]] = []

    # Find all skip decorators and the test name that follows
    # Pattern: optional other decorators, then @pytest.mark.skip(...), then def test_xxx
    out_parts = []
    last_end = 0

    # Match single-line skip
    single_pat = re.compile(
        r'^\s*@pytest\.mark\.skip\(reason="(Issue #\d+[^"]*)"\)\s*\n',
        re.MULTILINE,
    )
    # Match multi-line skip
    multi_pat = re.compile(
        r'^\s*@pytest\.mark\.skip\(\s*\n\s*reason="(Issue #\d+[^"]*)"\s*\)\s*\n',
        re.MULTILINE,
    )

    for pat, kind in [(multi_pat, "multi"), (single_pat, "single")]:
        for m in pat.finditer(text):
            issue = extract_issue_number(m.group(1))
            if issue is None:
                continue
            # Find next "def test_xxx" after this match
            after = text[m.end() : m.end() + 200]
            def_m = re.search(r"^\s*def (test_\w+)\s*\(", after, re.MULTILINE)
            if def_m:
                test_name = def_m.group(1)
                # Class name if inside a class
                before = text[max(0, m.start() - 500) : m.start()]
                class_m = re.findall(r"class (\w+)\s*:", before)
                if class_m:
                    full_id = f"{path.relative_to(TESTS_DIR).with_suffix('').as_posix().replace('/', '.')}::{class_m[-1]}::{test_name}"
                else:
                    full_id = f"{path.relative_to(TESTS_DIR).with_suffix('').as_posix().replace('/', '.')}::{test_name}"
                mapping.append((full_id, issue))
            out_parts.append((last_end, m.start()))
            last_end = m.end()

    # Sort by start position descending so we remove from end first (preserve indices)
    if not out_parts:
        return []

    # Build new text by removing matched regions
    new_text = text
    for start, end in sorted([(s, e) for s, e in out_parts], key=lambda x: -x[0]):
        new_text = new_text[:start] + new_text[end:]
    path.write_text(new_text)
    return mapping


def find_and_unskip_all() -> Dict[int, List[str]]:
    """Unskip all Issue #N marked tests; return issue_num -> list of test full ids."""
    issue_to_tests: Dict[int, List[str]] = {}
    for path in TESTS_DIR.rglob("*.py"):
        try:
            mapping = unskip_file(path)
        except Exception as e:
            print(f"Error processing {path}: {e}", file=sys.stderr)
            continue
        for full_id, issue in mapping:
            issue_to_tests.setdefault(issue, []).append(full_id)
    return issue_to_tests


def main():
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--dry-run", action="store_true", help="Only report, do not modify files"
    )
    ap.add_argument(
        "--run-tests", action="store_true", help="Run pytest and report pass/fail"
    )
    ap.add_argument(
        "--close-issues",
        action="store_true",
        help="Close GitHub issues where all tests passed",
    )
    args = ap.parse_args()

    if args.dry_run:
        # Just list what would be unskipped
        for path in TESTS_DIR.rglob("*.py"):
            text = path.read_text()
            for m in re.finditer(
                r'@pytest\.mark\.skip\([^)]*reason="(Issue #\d+[^"]*)"', text, re.DOTALL
            ):
                print(path, m.group(1))
        return

    # Unskip and record
    issue_to_tests = find_and_unskip_all()
    mapping_file = Path(__file__).parent.parent / "scripts" / "issue_test_mapping.json"
    mapping_file.parent.mkdir(exist_ok=True)
    with open(mapping_file, "w") as f:
        json.dump(issue_to_tests, f, indent=2)
    print("Unskipped; issue -> tests mapping written to", mapping_file)
    print("Issue counts:", {k: len(v) for k, v in sorted(issue_to_tests.items())})


if __name__ == "__main__":
    main()
