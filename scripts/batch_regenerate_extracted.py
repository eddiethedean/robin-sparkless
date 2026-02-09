#!/usr/bin/env python3
"""
Batch regenerate expected sections for pyspark_extracted fixtures.

Runs regenerate_expected_from_pyspark.py on all JSON files in tests/fixtures/pyspark_extracted/
and reports pass/fail/skip counts.

Usage:
  python scripts/batch_regenerate_extracted.py [--dry-run]

Requires: PySpark (pip install pyspark) and Java 17+.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Batch regenerate pyspark_extracted fixture expected sections"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print diffs without writing",
    )
    args = parser.parse_args()

    fixtures_dir = Path("tests/fixtures/pyspark_extracted")
    if not fixtures_dir.exists():
        print(f"Directory not found: {fixtures_dir}", file=sys.stderr)
        return 1

    json_files = sorted(fixtures_dir.glob("*.json"))
    if not json_files:
        print(f"No JSON fixtures in {fixtures_dir}", file=sys.stderr)
        return 0

    cmd = [
        sys.executable,
        "tests/regenerate_expected_from_pyspark.py",
        str(fixtures_dir),
        "--include-skipped",
    ]
    if args.dry_run:
        cmd.append("--dry-run")

    result = subprocess.run(cmd, capture_output=False)
    if result.returncode != 0:
        return result.returncode

    print(f"\nProcessed {len(json_files)} fixtures in {fixtures_dir}")
    print(
        "Run 'make pyspark-parity' or 'cargo test pyspark_parity_fixtures' to verify."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
