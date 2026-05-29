#!/usr/bin/env python3
"""Compare two PySpark API JSON snapshots (e.g. 3.5 vs 4.1) and optionally write markdown."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def load_functions(path: Path) -> dict[str, dict]:
    data = json.loads(path.read_text(encoding="utf-8"))
    return {f["name"]: f for f in data.get("functions", [])}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--baseline",
        type=Path,
        default=Path("docs/pyspark_api_from_repo.json"),
        help="Baseline API JSON (3.5)",
    )
    parser.add_argument(
        "--target",
        type=Path,
        default=Path("docs/pyspark_api_4.x.json"),
        help="Target API JSON (4.x)",
    )
    parser.add_argument(
        "--write-md",
        type=Path,
        default=None,
        help="Optional markdown output path",
    )
    args = parser.parse_args()

    if not args.baseline.exists():
        raise SystemExit(f"Missing baseline: {args.baseline}")
    if not args.target.exists():
        raise SystemExit(
            f"Missing target: {args.target}\n"
            "Run: python scripts/extract_pyspark_api_from_repo.py --output docs/pyspark_api_4.x.json"
        )

    base = load_functions(args.baseline)
    target = load_functions(args.target)
    added = sorted(set(target) - set(base))
    removed = sorted(set(base) - set(target))
    changed = sorted(
        n
        for n in set(base) & set(target)
        if base[n].get("args") != target[n].get("args")
    )

    lines = [
        "# PySpark API delta (baseline vs target)",
        "",
        f"- Baseline: `{args.baseline}` ({len(base)} functions)",
        f"- Target: `{args.target}` ({len(target)} functions)",
        "",
        f"## Added in target ({len(added)})",
        "",
    ]
    lines.extend(f"- `{n}`" for n in added[:50])
    if len(added) > 50:
        lines.append(f"- ... and {len(added) - 50} more")
    lines.extend(["", f"## Removed from baseline ({len(removed)})", ""])
    lines.extend(f"- `{n}`" for n in removed[:50])
    lines.extend(["", f"## Signature changes ({len(changed)})", ""])
    for n in changed[:30]:
        lines.append(f"- `{n}`")
    if len(changed) > 30:
        lines.append(f"- ... and {len(changed) - 30} more")

    report = "\n".join(lines) + "\n"
    print(report)
    if args.write_md:
        args.write_md.write_text(report, encoding="utf-8")
        print(f"Wrote {args.write_md}")


if __name__ == "__main__":
    main()
