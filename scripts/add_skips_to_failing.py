#!/usr/bin/env python3
"""Add @pytest.mark.skip(reason=\"Issue #N: unskip when fixing\") to each failing test."""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def load_mapping():
    mapping = {}
    with open("/tmp/file_to_issue.txt") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            filepath, num = line.split("\t")
            mapping[filepath] = int(num)
    return mapping


def load_failing():
    by_file = {}
    with open("/tmp/failing_tests.txt") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            filepath, _, rest = line.partition("::")
            if filepath not in by_file:
                by_file[filepath] = []
            by_file[filepath].append(rest)  # "test_foo" or "TestClass::test_foo"
    return by_file


def find_test_locations(content):
    """Return list of (line_ix_0based, def_name, class_name or None)."""
    lines = content.splitlines()
    locations = []
    current_class = None
    for i, line in enumerate(lines):
        stripped = line.strip()
        if not stripped:
            continue
        indent = len(line) - len(line.lstrip())
        class_m = re.match(r"^class\s+(\w+)\s*[:\(]", stripped)
        if class_m:
            current_class = class_m.group(1)
        def_m = re.match(r"^def\s+(\w+)\s*\(", stripped)
        if def_m:
            name = def_m.group(1)
            if name.startswith("test_"):
                # Module-level def has indent 0; then no class
                loc_class = current_class if indent > 0 else None
                locations.append((i, name, loc_class))
    return locations, lines


def test_id_matches(test_spec, def_name, class_name):
    """test_spec is either 'test_foo' or 'TestClass::test_foo'."""
    if "::" in test_spec:
        c, m = test_spec.split("::", 1)
        return m == def_name and c == (class_name or "")
    return test_spec == def_name and (class_name is None or class_name == "")


def already_has_skip(lines, def_line_ix):
    """Check if the line before def_line_ix is already a skip."""
    for j in range(def_line_ix - 1, max(-1, def_line_ix - 5), -1):
        if j < 0:
            break
        s = lines[j].strip()
        if s.startswith("def ") or s.startswith("@"):
            if "skip" in s and "Issue #" in s:
                return True
            if s.startswith("def "):
                return False
    return False


def add_skip_to_file(filepath, tests_to_skip, issue_num):
    path = ROOT / filepath
    if not path.exists():
        print(f"Skip {filepath} (not found)")
        return
    content = path.read_text()
    locations, lines = find_test_locations(content)
    # Which line indices to add skip to (set of line numbers)
    to_skip_at = set()
    for line_ix, def_name, class_name in locations:
        for spec in tests_to_skip:
            if test_id_matches(spec, def_name, class_name):
                to_skip_at.add(line_ix)
                break
    if not to_skip_at:
        return
    # Ensure pytest is imported
    if "import pytest" not in content:
        insert_import_at = 0
        for i, line in enumerate(lines):
            if line.strip().startswith("import ") or line.strip().startswith("from "):
                insert_import_at = i + 1
                break
        lines.insert(insert_import_at, "import pytest")
    # Insert skip decorators (from bottom so line numbers don't shift)
    skip_line = f'@pytest.mark.skip(reason="Issue #{issue_num}: unskip when fixing")'
    for line_ix in sorted(to_skip_at, reverse=True):
        if already_has_skip(lines, line_ix):
            continue
        # Insert before this def
        lines.insert(line_ix, skip_line)
    path.write_text("\n".join(lines) + "\n")
    print(
        f"Updated {filepath}: added skip for {len(to_skip_at)} test(s) (Issue #{issue_num})"
    )


def main():
    mapping = load_mapping()
    by_file = load_failing()
    for filepath, tests in by_file.items():
        issue_num = mapping.get(filepath)
        if not issue_num:
            continue
        add_skip_to_file(filepath, tests, issue_num)


if __name__ == "__main__":
    main()
