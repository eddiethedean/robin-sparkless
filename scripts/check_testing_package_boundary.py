#!/usr/bin/env python3
"""Verify sparkless_testing imports without either execution backend installed."""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--package-dir",
        type=Path,
        help="explicit package directory for source-tree checks; default tests the installed distribution",
    )
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    if args.package_dir is not None and not args.package_dir.is_dir():
        parser.error("--package-dir must name an existing directory")
    code = r"""
import importlib.abc
import sys

class BackendBlocker(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.partition(".")[0] in {"sparkless", "sparkless_native", "pyspark"}:
            raise AssertionError(f"backend import attempted: {fullname}")
        return None

sys.meta_path.insert(0, BackendBlocker())
import sparkless_testing
from sparkless_testing import Mode, get_imports, assert_rows_equal
assert Mode.SPARKLESS.value == "sparkless"
assert callable(get_imports) and callable(assert_rows_equal)
assert not any(name in sys.modules for name in ("sparkless", "sparkless_native", "pyspark"))
"""
    env = os.environ.copy()
    # Run outside the repository tree so the default exercises the installed
    # distribution. Source tests opt in with --package-dir.
    env.pop("PYTHONPATH", None)
    if args.package_dir is not None:
        env["PYTHONPATH"] = str(args.package_dir.resolve())
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=root.parent,
        env=env,
        check=False,
        text=True,
        capture_output=True,
    )
    if result.returncode:
        sys.stderr.write(result.stdout)
        sys.stderr.write(result.stderr)
        return result.returncode
    print("sparkless_testing import boundary passed without Sparkless/PySpark backends")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
