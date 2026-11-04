"""Validate candidate packages and retain their exact SHA-256 inventory."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tomllib
from typing import Any

from release_inventory import distribution_specs
from verify_release_distributions import verify_distributions


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _versions(root: Path) -> tuple[str, str]:
    api = tomllib.loads((root / "python/pyproject.toml").read_text())
    engine = tomllib.loads((root / "python/native/pyproject.toml").read_text())
    return api["project"]["version"], engine["project"]["version"]


def build_candidate_inventory(
    root: Path, source_revision: str | None = None
) -> dict[str, Any]:
    root = root.resolve()
    api_version, engine_version = _versions(root)
    specs = distribution_specs(api_version, engine_version)
    expected_directories = {spec.directory for spec in specs}
    dist_entries = list((root / "dist").iterdir())
    unexpected_root_entries = [
        entry.name
        for entry in dist_entries
        if not entry.is_dir() and entry.name != "candidate-inventory.json"
    ]
    if unexpected_root_entries:
        raise ValueError(
            "candidate distribution root contains unexpected files: "
            f"{sorted(unexpected_root_entries)}"
        )
    actual_directories = {entry.name for entry in dist_entries if entry.is_dir()}
    if actual_directories != expected_directories:
        raise ValueError(
            "candidate distribution directories differ: "
            f"expected {sorted(expected_directories)}, found {sorted(actual_directories)}"
        )

    distributions: dict[str, Any] = {}
    for spec in specs:
        directory = root / "dist" / spec.directory
        if any(not item.is_file() for item in directory.iterdir()):
            raise ValueError(
                f"{spec.project} distribution directory contains a non-file entry"
            )
        files = verify_distributions(
            directory=directory,
            project=spec.project,
            version=spec.version,
            wheel_count=spec.wheels,
            sdist_count=spec.sdists,
            platforms=set(spec.platforms),
            required_dependencies=set(spec.required_dependencies),
        )
        distributions[spec.project] = {
            "directory": spec.directory,
            "version": spec.version,
            "wheels": spec.wheels,
            "sdists": spec.sdists,
            "platforms": sorted(spec.platforms),
            "files": [
                {
                    "name": path.name,
                    "size_bytes": path.stat().st_size,
                    "sha256": _sha256(path),
                }
                for path in files
            ],
        }

    if source_revision is None:
        source_revision = os.environ.get("GITHUB_SHA")
    if source_revision is None:
        source_revision = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    if not re.fullmatch(r"[0-9a-f]{40}", source_revision):
        raise ValueError(f"invalid source revision: {source_revision!r}")
    return {
        "schema_version": 1,
        "source_revision": source_revision,
        "api_version": api_version,
        "engine_version": engine_version,
        "distributions": distributions,
    }


def verify_published_inputs(
    manifest_path: Path,
    directory: Path,
    projects: list[str],
    expected_revision: str | None = None,
) -> None:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(manifest, dict):
        raise ValueError("candidate inventory must be a JSON object")
    if manifest.get("schema_version") != 1:
        raise ValueError("unsupported candidate inventory schema")
    source_revision = manifest.get("source_revision")
    if not isinstance(source_revision, str) or not re.fullmatch(
        r"[0-9a-f]{40}", source_revision
    ):
        raise ValueError("candidate inventory has no valid source revision")
    if expected_revision is not None and source_revision != expected_revision:
        raise ValueError(
            "candidate inventory belongs to a different source revision: "
            f"expected {expected_revision}, found {source_revision}"
        )
    distributions = manifest.get("distributions")
    if not isinstance(distributions, dict) or not projects:
        raise ValueError("candidate inventory is missing distribution entries")

    expected: dict[str, dict[str, Any]] = {}
    for project in projects:
        entry = distributions.get(project)
        if not isinstance(entry, dict):
            raise ValueError(f"candidate inventory has no entry for {project}")
        files = entry.get("files")
        if not isinstance(files, list) or not files:
            raise ValueError(f"candidate inventory has no files for {project}")
        for file in files:
            if not isinstance(file, dict):
                raise ValueError(f"invalid inventory file record for {project}")
            name = file.get("name")
            digest = file.get("sha256")
            size = file.get("size_bytes")
            if (
                not isinstance(name, str)
                or not name
                or Path(name).name != name
                or name in expected
                or not isinstance(digest, str)
                or not re.fullmatch(r"[0-9a-f]{64}", digest)
                or isinstance(size, bool)
                or not isinstance(size, int)
                or size < 1
            ):
                raise ValueError(f"invalid or duplicate inventory file for {project}")
            expected[name] = file

    directory = directory.resolve()
    entries = list(directory.iterdir())
    if any(not item.is_file() for item in entries):
        raise ValueError("publisher input directory contains a non-file entry")
    actual_paths = entries
    actual_names = {item.name for item in actual_paths}
    if actual_names != set(expected):
        raise ValueError(
            "publisher inputs differ from the verified candidate inventory: "
            f"expected {sorted(expected)}, found {sorted(actual_names)}"
        )
    for path in actual_paths:
        recorded = expected[path.name]
        digest = _sha256(path)
        size = path.stat().st_size
        if digest != recorded.get("sha256") or size != recorded.get("size_bytes"):
            raise ValueError(
                f"publisher input changed after candidate verification: {path.name}"
            )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--output", type=Path)
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--directory", type=Path)
    parser.add_argument("--project", action="append", default=[])
    args = parser.parse_args(argv)
    try:
        if args.manifest is not None:
            if args.directory is None:
                parser.error("--directory is required with --manifest")
            verify_published_inputs(
                args.manifest,
                args.directory,
                args.project,
                expected_revision=os.environ.get("GITHUB_SHA"),
            )
            print(f"verified publisher inputs for {', '.join(args.project)}")
            return 0
        if args.directory is not None or args.project:
            parser.error("--directory and --project are only used with --manifest")
        inventory = build_candidate_inventory(args.root)
        output = args.output or args.root / "dist" / "candidate-inventory.json"
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(inventory, indent=2, sort_keys=True) + "\n")
    except (
        OSError,
        KeyError,
        ValueError,
        json.JSONDecodeError,
        subprocess.CalledProcessError,
    ) as error:
        print(f"candidate inventory check failed: {error}", file=sys.stderr)
        return 1
    file_count = sum(
        len(entry["files"]) for entry in inventory["distributions"].values()
    )
    print(
        f"verified {file_count} files across {len(inventory['distributions'])} "
        f"distributions for {inventory['api_version']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
