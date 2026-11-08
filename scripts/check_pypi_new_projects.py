"""Validate per-project PyPI API-token fallback configuration."""

from __future__ import annotations

import os
import sys


PYPI_PROJECTS = frozenset(
    {
        "sparkless",
        "sparkless-connect",
        "sparkless-native",
        "sparkless-server",
        "sparkless-server-duckdb",
        "sparkless-testing",
    }
)


def parse_new_projects(raw: str) -> list[str]:
    if not raw:
        return []
    projects = raw.split(",")
    if any(not project or project.strip() != project for project in projects):
        raise ValueError(
            "PYPI_NEW_PROJECTS must be a comma-separated list without whitespace."
        )
    if len(projects) != len(set(projects)):
        raise ValueError("PYPI_NEW_PROJECTS contains duplicate project names.")
    unknown = sorted(set(projects) - PYPI_PROJECTS)
    if unknown:
        raise ValueError(
            "PYPI_NEW_PROJECTS contains unsupported project names: "
            + ", ".join(unknown)
        )
    return projects


def main() -> int:
    try:
        projects = parse_new_projects(os.environ.get("PYPI_NEW_PROJECTS", ""))
        if projects and not os.environ.get("PYPI_API_TOKEN"):
            raise ValueError(
                "PYPI_API_TOKEN GitHub Actions secret is required for new PyPI projects."
            )
    except ValueError as error:
        print(error, file=sys.stderr)
        return 1
    print("PyPI API token fallback configured for: " + ", ".join(projects))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
