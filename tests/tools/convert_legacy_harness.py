#!/usr/bin/env python3
"""Convert test files from legacy harness (get_spark, get_functions, get_session, get_window_cls)
to new harness (spark fixture + get_spark_imports). Run from repo root."""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]


def convert(path: Path) -> bool:
    text = path.read_text()
    orig = text

    # Must have legacy imports from tests.utils
    if "from tests.utils import" not in text:
        return False
    utils_line = re.search(r"from tests\.utils import ([^\n]+)", text)
    if not utils_line:
        return False
    imports_str = utils_line.group(1)
    legacy = {"get_spark", "get_session", "get_functions", "get_window_cls"}
    has_legacy = any(x in imports_str for x in legacy)
    if not has_legacy:
        return False

    # Build new utils import (drop legacy, keep _row_to_dict, assert_rows_equal, etc.)
    parts = [p.strip() for p in re.split(r",\s*", imports_str)]
    kept = [p for p in parts if p.split()[0] not in legacy]
    new_utils = "from tests.utils import " + ", ".join(kept) if kept else None

    needs_F = "get_functions" in imports_str
    needs_Window = "get_window_cls" in imports_str

    # 1) Replace utils import line
    if new_utils:
        text = re.sub(r"from tests\.utils import [^\n]+", new_utils, text, count=1)
    else:
        text = re.sub(r"from tests\.utils import [^\n]+\n", "", text, count=1)

    # 2) Add get_spark_imports block after first import block if needed
    if (needs_F or needs_Window) and "get_spark_imports" not in text:
        block = "from tests.fixtures.spark_imports import get_spark_imports\n\n_imports = get_spark_imports()\nF = _imports.F\n"
        if needs_Window:
            block += "Window = _imports.Window\n"
        # Insert after first 'from tests.' or 'from __future__'
        m = re.search(r"^(from __future__ import[^\n]+\n\n)", text, re.MULTILINE)
        if m:
            pos = m.end()
        else:
            m2 = re.search(r"^from tests\.(utils|fixtures)[^\n]+\n", text, re.MULTILINE)
            pos = m2.end() if m2 else 0
        text = text[:pos] + block + "\n" + text[pos:]

    # 3) Remove F = get_functions() and Window = get_window_cls()
    text = re.sub(r"^F = get_functions\(\)\s*\n", "", text, flags=re.MULTILINE)
    text = re.sub(r"^Window = get_window_cls\(\)\s*\n", "", text, flags=re.MULTILINE)
    text = re.sub(
        r"    from tests\.utils import get_functions\n\s*F = get_functions\(\)\s*\n",
        "",
        text,
    )
    text = re.sub(r"    F = get_functions\(\)\s*\n", "", text)

    # 4) Remove spark = get_spark(...), spark = get_session(), spark = _spark()
    text = re.sub(r"\n\s*spark = get_spark\([^)]+\)\s*\n", "\n", text)
    text = re.sub(r"\n\s*spark = get_session\(\)\s*\n", "\n", text)
    text = re.sub(r"\n\s*spark = _spark\(\)\s*\n", "\n", text)
    text = re.sub(r"\n\s*spark = _get_session\(\)\s*\n", "\n", text)

    # 5) Add (spark) to test_*() and test_*(self) that don't have spark
    def add_spark(m):
        pre, name, args, rest = m.group(1), m.group(2), m.group(3) or "", m.group(4)
        if "spark" in args:
            return m.group(0)
        if args.strip() == "self":
            return f"{pre}def {name}(self, spark){rest}"
        return f"{pre}def {name}(spark){rest}"

    text = re.sub(
        r"^(\s*)def (test_\w+)\((self)?\)(.*)$", add_spark, text, flags=re.MULTILINE
    )

    # 6) Remove def _spark(): return get_spark(...) blocks
    text = re.sub(
        r"\ndef _spark\(\)[^:]*:\s*\n\s*return get_spark\([^)]+\)\s*\n", "\n", text
    )
    text = re.sub(
        r"\ndef _get_session\(\)[^:]*:\s*\n\s*return get_spark\([^)]+\)\s*\n",
        "\n",
        text,
    )

    if text != orig:
        path.write_text(text)
        return True
    return False


def main():
    n = 0
    for p in sorted(REPO.glob("tests/**/test_*.py")):
        if "convert_legacy" in str(p):
            continue
        try:
            if convert(p):
                n += 1
                print(p.relative_to(REPO))
        except Exception as e:
            print(f"ERR {p}: {e}", file=__import__("sys").stderr)
    print(f"Converted {n} files", file=__import__("sys").stderr)


if __name__ == "__main__":
    main()
