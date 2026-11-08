"""Run published package examples from clean temporary directories."""

from __future__ import annotations

import argparse
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

from run_documentation_examples import run_profile

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "profile", choices=["local", "testing", "provider", "http", "connect", "duckdb"]
    )
    args = parser.parse_args()
    if args.profile != "testing":
        run_profile(args.profile)
        return
    with tempfile.TemporaryDirectory(prefix="sparkless-testing-guide-") as directory:
        work = Path(directory)
        for name in ("conftest.py", "test_pipeline_example.py"):
            shutil.copy2(ROOT / "examples/python" / name, work)
        subprocess.run(
            [sys.executable, "-m", "pytest", "test_pipeline_example.py"],
            cwd=work,
            check=True,
        )


if __name__ == "__main__":
    main()
