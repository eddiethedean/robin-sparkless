"""Check the isolated base, DuckDB and transport-only Rust dependency graphs."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import sys


GRAPH_CASES = (
    (
        "base",
        "sparkless-server",
        "http,connect",
        ("robin-sparkless-duckdb", "duckdb v", "libduckdb-sys"),
        (),
    ),
    ("http-only", "sparkless-server", "http", ("sparkless-server-connect",), ()),
    ("connect-only", "sparkless-server", "connect", ("sparkless-server-http",), ()),
    (
        "duckdb",
        "sparkless-server-duckdb",
        "http,connect",
        (),
        ("robin-sparkless-duckdb",),
    ),
)


def verify_feature_graphs(root: Path, output_dir: Path) -> list[Path]:
    root = root.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    outputs = []
    for name, package, features, forbidden, required in GRAPH_CASES:
        target_name = "server-" + name.replace("-only", "")
        environment = os.environ.copy()
        environment["CARGO_TARGET_DIR"] = str(root / "target" / target_name)
        result = subprocess.run(
            [
                "cargo",
                "tree",
                "--locked",
                "-e",
                "features",
                "-p",
                package,
                "--no-default-features",
                "--features",
                features,
            ],
            cwd=root,
            env=environment,
            check=True,
            capture_output=True,
            text=True,
        )
        graph = result.stdout
        for needle in forbidden:
            if needle in graph:
                raise ValueError(
                    f"{name} dependency graph unexpectedly contains {needle!r}"
                )
        for needle in required:
            if needle not in graph:
                raise ValueError(f"{name} dependency graph is missing {needle!r}")
        path = output_dir / f"{name}.txt"
        path.write_text(graph, encoding="utf-8")
        outputs.append(path)
    return outputs


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        outputs = verify_feature_graphs(args.root, args.output_dir)
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print(f"server feature graph check failed: {error}", file=sys.stderr)
        if isinstance(error, subprocess.CalledProcessError) and error.stderr:
            print(error.stderr, file=sys.stderr)
        return 1
    print(f"verified {len(outputs)} isolated server feature graphs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
