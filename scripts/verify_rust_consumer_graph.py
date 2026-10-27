"""Audit the downstream Cargo graph without inheriting workspace patches."""

from __future__ import annotations

import argparse
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import tomllib


def consumer_manifest(source: str) -> str:
    manifest = re.sub(r"(?ms)^\[patch\.crates-io\]\n.*?(?=^\[|\Z)", "", source)
    manifest = re.sub(
        r"(?m)^members\s*=\s*\[[^\]]*\]",
        'members = [".", "crates/robin-sparkless-core", '
        '"crates/robin-sparkless-polars", "crates/spark-sql-parser", '
        '"crates/sparkless-server"]',
        manifest,
        count=1,
    )
    if "patch" in tomllib.loads(manifest):
        raise ValueError("consumer manifest still contains Cargo patches")
    return manifest


def verify(root: Path, output: Path, offline: bool = False) -> None:
    with tempfile.TemporaryDirectory(prefix="sparkless-rust-consumer-") as scratch:
        consumer = Path(scratch)
        for name in ("src", "crates", "examples", "benches"):
            shutil.copytree(
                root / name,
                consumer / name,
                ignore=shutil.ignore_patterns("target", ".git"),
            )
        (consumer / "Cargo.toml").write_text(
            consumer_manifest((root / "Cargo.toml").read_text())
        )
        shutil.copyfile(root / "Cargo.lock", consumer / "Cargo.lock")
        command = ["cargo", "metadata", "--format-version", "1", "--all-features"]
        if offline:
            command.append("--offline")
        subprocess.run(command, cwd=consumer, stdout=subprocess.DEVNULL, check=True)
        command = ["cargo", "audit", "--file", str(consumer / "Cargo.lock"), "--json"]
        if offline:
            command.append("--no-fetch")
        result = subprocess.run(command, cwd=root, stdout=subprocess.PIPE, check=False)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(result.stdout)
        if result.returncode:
            raise RuntimeError(
                "downstream Rust dependency audit failed; see " + str(output)
            )
    print("unpatched downstream Rust dependency graph passed its audit")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--offline", action="store_true")
    args = parser.parse_args()
    verify(Path(__file__).resolve().parents[1], args.output.resolve(), args.offline)


if __name__ == "__main__":
    main()
