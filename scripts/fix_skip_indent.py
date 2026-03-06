#!/usr/bin/env python3
"""Fix @pytest.mark.skip decorators that are at column 0 but should be indented (before class methods)."""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TESTS = ROOT / "tests"

def fix_file(path: Path) -> bool:
    text = path.read_text()
    lines = text.splitlines()
    changed = False
    i = 0
    while i < len(lines):
        line = lines[i]
        # Line that is exactly the skip decorator at column 0
        if re.match(r'^@pytest\.mark\.skip\(reason="Issue #\d+: unskip when fixing"\)\s*$', line):
            # Find next non-blank line for indent (decorator may have blank line after)
            j = i + 1
            while j < len(lines) and not lines[j].strip():
                j += 1
            if j < len(lines):
                next_line = lines[j]
                # Next line is an indented def (class method)
                m = re.match(r'^(\s+)def\s+\w+\s*\(', next_line)
                if m:
                    indent = m.group(1)
                    if indent and not line.startswith(" "):
                        lines[i] = indent + line.strip()
                        changed = True
        i += 1
    if changed:
        path.write_text("\n".join(lines) + "\n")
    return changed

def main():
    for py in TESTS.rglob("*.py"):
        if fix_file(py):
            print(f"Fixed {py.relative_to(ROOT)}")

if __name__ == "__main__":
    main()
