"""Verify Cargo package archives contain registry-ready manifests."""

from __future__ import annotations

import argparse
import sys
import tarfile
from pathlib import Path

import tomllib


PUBLISHABLE_MANIFESTS = (
    "Cargo.toml",
    "crates/robin-sparkless-core/Cargo.toml",
    "crates/robin-sparkless-polars/Cargo.toml",
    "crates/spark-sql-parser/Cargo.toml",
    "crates/sparkless-server/Cargo.toml",
)
DEPENDENCY_TABLES = ("dependencies", "dev-dependencies", "build-dependencies")


def _check_dependency_table(dependencies: dict, manifest_name: str) -> list[str]:
    errors = []
    for name, specification in dependencies.items():
        if isinstance(specification, dict) and "path" in specification:
            errors.append(
                "{}: packaged dependency {} still has a local path".format(
                    manifest_name, name
                )
            )
    return errors


def _check_manifest(
    manifest: dict, expected_name: str, expected_version: str
) -> list[str]:
    errors = []
    package = manifest.get("package", {})
    if (
        package.get("name") != expected_name
        or package.get("version") != expected_version
    ):
        errors.append(
            "archive {}-{} contains mismatched package metadata".format(
                expected_name, expected_version
            )
        )
    if "patch" in manifest:
        errors.append(
            "{}-{}: packaged manifest still contains a patch table".format(
                expected_name, expected_version
            )
        )
    for table in DEPENDENCY_TABLES:
        dependencies = manifest.get(table, {})
        errors.extend(_check_dependency_table(dependencies, expected_name))
    for target in manifest.get("target", {}).values():
        for table in DEPENDENCY_TABLES:
            dependencies = target.get(table, {})
            errors.extend(_check_dependency_table(dependencies, expected_name))
    return errors


def verify_archives(root: Path, archive_dir: Path) -> list[str]:
    errors = []
    for relative in PUBLISHABLE_MANIFESTS:
        source = root / relative
        try:
            with source.open("rb") as stream:
                package = tomllib.load(stream)["package"]
            name, version = package["name"], package["version"]
            archive = archive_dir / "{}-{}.crate".format(name, version)
            if not archive.is_file():
                errors.append("missing package archive {}".format(archive))
                continue
            with tarfile.open(archive, "r:gz") as crate:
                members = [
                    member
                    for member in crate.getmembers()
                    if member.isfile() and member.name.endswith("/Cargo.toml")
                ]
                if len(members) != 1:
                    errors.append(
                        "{}: expected one packaged Cargo.toml".format(archive.name)
                    )
                    continue
                content = crate.extractfile(members[0])
                if content is None:
                    errors.append("{}: cannot read Cargo.toml".format(archive.name))
                    continue
                packaged_manifest = tomllib.loads(content.read().decode("utf-8"))
            errors.extend(_check_manifest(packaged_manifest, name, version))
        except (
            OSError,
            KeyError,
            tarfile.TarError,
            UnicodeDecodeError,
            ValueError,
        ) as error:
            errors.append(
                "{}: cannot verify package archive: {}".format(relative, error)
            )
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root", type=Path, default=Path(__file__).resolve().parents[1]
    )
    parser.add_argument("--archive-dir", type=Path, default=Path("target/package"))
    args = parser.parse_args()
    errors = verify_archives(args.root, args.archive_dir)
    if errors:
        for error in errors:
            print("ERROR: {}".format(error), file=sys.stderr)
        return 1
    print("all publishable crate archives contain matching registry-ready manifests")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
