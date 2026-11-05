"""Run the published package quickstarts from a clean working directory."""

from __future__ import annotations

import argparse
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def run(work: Path, *args: str) -> None:
    subprocess.run([sys.executable, *args], cwd=work, check=True)


def run_quickstart(work: Path, page: str) -> None:
    text = (ROOT / "docs" / page).read_text()
    # The first Python block is the complete quickstart for each of these pages.
    block = re.search(r"^```python\n(.*?)^```", text, re.M | re.S)
    if block is None:
        raise ValueError(f"Missing Python quickstart in {page}")
    script = work / "quickstart.py"
    script.write_text(block[1])
    run(work, script.name)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "profile", choices=["local", "testing", "http", "connect", "duckdb"]
    )
    args = parser.parse_args()
    examples = ROOT / "examples" / "python"
    with tempfile.TemporaryDirectory(prefix="sparkless-quickstart-") as directory:
        work = Path(directory)
        if args.profile == "local":
            run(work, str(examples / "basic_usage.py"))
        elif args.profile == "testing":
            for name in ("conftest.py", "test_pipeline_example.py"):
                shutil.copy2(examples / name, work)
            run(work, "-m", "pytest", "test_pipeline_example.py")
        elif args.profile == "http":
            run_quickstart(work, "HTTP_API.md")
        elif args.profile == "connect":
            run_quickstart(work, "SPARK_CONNECT.md")
            run(work, str(examples / "connect_load_query.py"))
            suite = work / "connect_xdist"
            shutil.copytree(
                ROOT / "examples" / "connect_xdist",
                suite,
                ignore=shutil.ignore_patterns("__pycache__"),
            )
            run(suite, "-m", "pytest", "-n", "2", "test_orders.py")
        elif args.profile == "duckdb":
            run_quickstart(work, "DUCKDB_SOURCE.md")


if __name__ == "__main__":
    main()
