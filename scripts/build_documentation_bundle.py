"""Combine the strict MkDocs site and versioned, locally browsable rustdoc."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import shutil
import subprocess

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--site", type=Path, default=ROOT / "site")
    parser.add_argument("--rustdoc", type=Path, default=ROOT / "target/doc")
    args = parser.parse_args()
    version = json.loads((ROOT / "docs/reference/release.json").read_text())["version"]
    if not (args.site / "index.html").is_file():
        parser.error("Build the MkDocs site first")
    for crate in ("robin_sparkless", "robin_sparkless_core"):
        if not (args.rustdoc / crate / "index.html").is_file():
            parser.error(
                f"Missing rustdoc for {crate}; build the facade and core reference first"
            )
    destination = args.site / "rust-api" / version
    if destination.resolve().is_relative_to(args.rustdoc.resolve()):
        parser.error("The bundle destination must be separate from the rustdoc source")
    shutil.copytree(args.rustdoc, destination, dirs_exist_ok=True)
    (destination / "index.html").write_text(
        '<!doctype html><html lang="en"><meta charset="utf-8">'
        f"<title>Sparkless {version} Rust API</title><h1>Rust API {version}</h1>"
        '<p><a href="robin_sparkless/index.html">Rust facade</a></p>'
        '<p><a href="robin_sparkless_core/index.html">Engine-neutral core</a></p></html>'
    )
    source = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
    ).strip()
    dirty = bool(
        subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True)
    )
    (args.site / "documentation-build.json").write_text(
        json.dumps(
            {
                "version": version,
                "source_commit": source,
                "working_tree_modified": dirty,
                "rust_api": f"rust-api/{version}/robin_sparkless/index.html",
                "features": ["sql"],
                "toolchain": "1.93.1",
            },
            indent=2,
        )
        + "\n"
    )
    print(f"Browsable bundle: {destination / 'index.html'}")


if __name__ == "__main__":
    main()
