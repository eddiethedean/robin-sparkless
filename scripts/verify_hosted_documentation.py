"""Verify hosted versions after publication; never infer freshness from a badge."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
from urllib.request import Request, urlopen


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected", required=True)
    parser.add_argument(
        "--base-url", default="https://robin-sparkless.readthedocs.io/en"
    )
    parser.add_argument(
        "--rust-url", help="The actual deployed versioned Rust facade route"
    )
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    checks = []
    for alias in ("latest", "stable"):
        url = f"{args.base_url.rstrip('/')}/{alias}/"
        request = Request(url, headers={"Cache-Control": "no-cache"})
        with urlopen(request, timeout=30) as response:
            html = response.read().decode()
        found = re.search(r"Docs\s+([0-9]+\.[0-9]+\.[0-9]+)", html)
        version = found[1] if found else None
        checks.append({"url": url, "version": version, "ok": version == args.expected})
    if args.rust_url:
        with urlopen(args.rust_url, timeout=30) as response:
            html = response.read().decode()
        checks.append(
            {
                "url": args.rust_url,
                "ok": "robin_sparkless" in html and "rustdoc" in html,
            }
        )
    report = {"expected_version": args.expected, "checks": checks}
    if args.report:
        args.report.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))
    if not all(check["ok"] for check in checks):
        raise SystemExit(
            "Hosted documentation is missing, stale or mismatched; inspect publication routing"
        )


if __name__ == "__main__":
    main()
