"""Execute adoption examples with an installed wheel outside the repository."""

from __future__ import annotations

import argparse
import os
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
        "--backend", choices=["sparkless", "pyspark"], default="sparkless"
    )
    args = parser.parse_args()
    import sparkless

    if ROOT in Path(sparkless.__file__).resolve().parents:
        raise SystemExit(
            "Use an installed wheel in a separate environment; checkout/editable imports cannot verify distribution examples"
        )
    env = os.environ.copy()
    env.pop("PYTHONPATH", None)
    env["SPARKLESS_TEST_MODE"] = args.backend
    env["PYSPARK_PYTHON"] = sys.executable
    env["PYSPARK_DRIVER_PYTHON"] = sys.executable
    env["SPARK_LOCAL_IP"] = "127.0.0.1"
    env["SPARKLESS_SHARED_SESSION"] = "0"
    with tempfile.TemporaryDirectory(prefix="sparkless-doc-examples-") as directory:
        work = Path(directory)
        for source in (ROOT / "examples/python").glob("*.py"):
            shutil.copy2(source, work / source.name)
        subprocess.run(
            [sys.executable, "-m", "pytest", str(work), "-v", "-s", "--strict-markers"],
            cwd=work,
            env=env,
            check=True,
        )
        if args.backend == "sparkless":
            for name in ["basic_usage.py", "sql_and_temp_views.py"]:
                subprocess.run(
                    [sys.executable, str(work / name)], cwd=work, env=env, check=True
                )
            run_profile("local")
            run_profile("provider")
            print("README and reference fences executed against", sparkless.__file__)


if __name__ == "__main__":
    main()
