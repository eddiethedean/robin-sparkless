"""Run explicitly registered complete examples; fragments are not treated as programs."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "docs/reference/examples.json"


def run_profile(profile: str, *, allow_checkout: bool = False) -> None:
    entries = json.loads(MANIFEST.read_text())
    selected = [entry for entry in entries if profile in entry["profiles"]]
    if not selected:
        raise ValueError(f"No complete examples registered for {profile}")
    env = os.environ.copy()
    if not allow_checkout:
        env.pop("PYTHONPATH", None)
        if profile in {"local", "provider", "native-sqlite"}:
            import sparkless
            import sparkless_native._native as native

            if any(
                ROOT in Path(module.__file__).resolve().parents
                for module in (sparkless, native)
            ):
                raise ValueError(
                    "Use an installed wheel; --allow-checkout is for source validation only"
                )
    for entry in selected:
        with tempfile.TemporaryDirectory(prefix="sparkless-doc-example-") as directory:
            work = Path(directory)
            print(f"Running {entry['id']} ({profile})", flush=True)
            if entry["kind"] == "rust":
                subprocess.run(
                    [
                        "cargo",
                        "run",
                        "--locked",
                        "--example",
                        entry["example"],
                        "--features",
                        ",".join(entry["features"]),
                    ],
                    cwd=ROOT,
                    env=env,
                    check=True,
                )
                continue
            commands = []
            if entry["kind"] == "python-fences":
                blocks = re.findall(
                    r"^```python\n(.*?)^```",
                    (ROOT / entry["source"]).read_text(),
                    re.M | re.S,
                )
                indices = entry.get("fences", list(range(len(blocks))))
                for index in indices:
                    script = work / f"example_{index}.py"
                    script.write_text(blocks[index])
                    commands.append([sys.executable, str(script)])
            elif entry["kind"] == "python-script":
                source = ROOT / entry["source"]
                shutil.copy2(source, work / source.name)
                commands.append([sys.executable, str(work / source.name)])
            elif entry["kind"] == "pytest":
                source = ROOT / entry["source"]
                shutil.copytree(
                    source, work / "suite", ignore=shutil.ignore_patterns("__pycache__")
                )
                commands.append(
                    [
                        sys.executable,
                        "-m",
                        "pytest",
                        str(work / "suite"),
                        *entry["args"],
                    ]
                )
            else:
                raise ValueError(f"Unknown example kind: {entry['kind']}")
            for command in commands:
                result = subprocess.run(
                    command,
                    cwd=work,
                    env=env,
                    check=True,
                    text=True,
                    stdout=subprocess.PIPE,
                )
                print(result.stdout, end="", flush=True)
                for expected in entry.get("stdout_contains", []):
                    if expected not in result.stdout:
                        raise AssertionError(
                            f"{entry['id']} did not print expected output: {expected}"
                        )
    print(f"Passed {len(selected)} registered {profile} examples", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "profile",
        choices=[
            "local",
            "provider",
            "native-sqlite",
            "http",
            "connect",
            "duckdb",
            "rust",
        ],
    )
    parser.add_argument("--allow-checkout", action="store_true")
    args = parser.parse_args()
    run_profile(args.profile, allow_checkout=args.allow_checkout)


if __name__ == "__main__":
    main()
