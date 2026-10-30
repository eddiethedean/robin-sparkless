"""Validate release metadata and select the permitted publication targets."""

from __future__ import annotations

import os
from pathlib import Path
import re

from check_release_versions import check_release


def publication_targets(event: str, ref: str, publish_rust: bool) -> tuple[bool, bool]:
    """Return (Python publication, Rust publication); all other runs are dry runs."""
    if event == "push":
        if publish_rust:
            raise ValueError("push events cannot set manual publication inputs")
        if re.fullmatch(r"refs/tags/v\d+\.\d+\.\d+", ref):
            return True, True
        if ref.startswith("refs/heads/codex/release-"):
            return False, False
    elif event == "workflow_dispatch":
        if publish_rust:
            if ref != "refs/heads/main":
                raise ValueError("manual Rust publication requires main")
            return False, True
        if ref.startswith("refs/heads/") or re.fullmatch(
            r"refs/tags/v\d+\.\d+\.\d+", ref
        ):
            return False, False
    raise ValueError(f"unsupported release context: {event} {ref}")


def main() -> None:
    event, ref = os.environ["GITHUB_EVENT_NAME"], os.environ["GITHUB_REF"]
    python, rust = publication_targets(
        event, ref, os.environ.get("PUBLISH_RUST", "false").lower() == "true"
    )
    errors = check_release(
        Path(__file__).resolve().parents[1],
        tag=ref.removeprefix("refs/tags/") if ref.startswith("refs/tags/") else None,
    )
    if errors:
        raise SystemExit("\n".join(errors))
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
        output.write(f"publish_python={str(python).lower()}\n")
        output.write(f"publish_rust={str(rust).lower()}\n")


if __name__ == "__main__":
    main()
