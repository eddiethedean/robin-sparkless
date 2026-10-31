"""Validate wheel and source distributions before publishing a release."""

from __future__ import annotations

import argparse
from email.parser import BytesParser
from pathlib import Path
import re
import sys
import tarfile
import zipfile

PYPI_FILE_LIMIT_BYTES = 100 * 1024 * 1024
PYPI_PROJECT_LIMIT_BYTES = 10 * 1024 * 1024 * 1024


def _normalize_project(name: str) -> str:
    return re.sub(r"[-_.]+", "_", name).lower()


def _read_metadata(
    path: Path,
    minimum_python: str | None = None,
    required_dependencies: set[str] | None = None,
) -> tuple[str, str]:
    if path.name.endswith(".whl"):
        with zipfile.ZipFile(path) as archive:
            metadata_paths = [
                item
                for item in archive.namelist()
                if item.endswith(".dist-info/METADATA")
            ]
            if len(metadata_paths) != 1:
                raise ValueError(
                    "{} must contain exactly one wheel METADATA file".format(path)
                )
            metadata_bytes = archive.read(metadata_paths[0])
    elif path.name.endswith(".tar.gz"):
        with tarfile.open(path, "r:gz") as archive:
            metadata_members = [
                item
                for item in archive.getmembers()
                if item.name.endswith("/PKG-INFO") and len(Path(item.name).parts) == 2
            ]
            if len(metadata_members) != 1:
                raise ValueError(
                    "{} must contain exactly one source PKG-INFO file".format(path)
                )
            source = archive.extractfile(metadata_members[0])
            if source is None:
                raise ValueError("{} has an unreadable PKG-INFO file".format(path))
            metadata_bytes = source.read()
    else:
        raise ValueError("unsupported distribution file: {}".format(path.name))

    metadata = BytesParser().parsebytes(metadata_bytes)
    dependencies = {
        re.sub(r"\s+", "", value) for value in metadata.get_all("Requires-Dist", [])
    }
    if required_dependencies and not required_dependencies.issubset(dependencies):
        raise ValueError(f"{path.name} is missing its exact component dependencies")
    if minimum_python is not None and metadata.get("Requires-Python") != minimum_python:
        raise ValueError(
            "{} must declare Requires-Python: {}".format(path.name, minimum_python)
        )
    name = metadata.get("Name")
    version = metadata.get("Version")
    if not name or not version:
        raise ValueError("{} is missing Name or Version metadata".format(path.name))
    return name, version


def verify_distributions(
    directory: Path,
    project: str,
    version: str,
    wheel_count: int,
    sdist_count: int,
    platforms: set[str] | None = None,
    required_dependencies: set[str] | None = None,
) -> list[Path]:
    files = sorted(item for item in directory.iterdir() if item.is_file())
    wheels = [item for item in files if item.name.endswith(".whl")]
    sdists = [item for item in files if item.name.endswith(".tar.gz")]
    if len(files) != len(wheels) + len(sdists):
        raise ValueError("distribution folder contains unexpected non-package files")
    for path in files:
        if path.stat().st_size > PYPI_FILE_LIMIT_BYTES:
            raise ValueError(
                "{} exceeds PyPI's 100 MiB per-file limit".format(path.name)
            )
    candidate_size = sum(path.stat().st_size for path in files)
    if candidate_size > PYPI_PROJECT_LIMIT_BYTES:
        raise ValueError("candidate distributions exceed PyPI's 10 GiB project limit")
    if len(wheels) != wheel_count:
        raise ValueError(
            "expected {} wheels for {}, found {}".format(
                wheel_count, project, len(wheels)
            )
        )
    if len(sdists) != sdist_count:
        raise ValueError(
            "expected {} source distributions for {}, found {}".format(
                sdist_count, project, len(sdists)
            )
        )

    expected_name = _normalize_project(project)
    pure_api = expected_name == "sparkless" and tuple(map(int, version.split("."))) >= (
        4,
        18,
        1,
    )
    minimum_python = (
        ">=3.11" if tuple(map(int, version.split(".")[:2])) >= (4, 17) else None
    )
    wheel_platforms = set()
    for path in wheels:
        fields = path.name[:-4].split("-")
        if len(fields) != 5:
            raise ValueError("{} is not a valid wheel filename".format(path.name))
        filename_name, filename_version, _python, _abi, platform = fields
        if pure_api:
            if (_python, _abi, platform) != ("py3", "none", "any"):
                raise ValueError("sparkless API wheel must use py3-none-any")
            with zipfile.ZipFile(path) as archive:
                if any(
                    name.endswith((".so", ".pyd", ".dll"))
                    for name in archive.namelist()
                ):
                    raise ValueError(
                        "sparkless API wheel must not bundle native binaries"
                    )
            if path.stat().st_size > 1024 * 1024:
                raise ValueError("sparkless API wheel exceeds its 1 MiB storage budget")
        if (
            minimum_python
            and expected_name in {"sparkless", "sparkless_native"}
            and not pure_api
            and (_python, _abi) != ("cp311", "abi3")
        ):
            raise ValueError(
                "{} must use the Python 3.11 cp311-abi3 wheel tag".format(path.name)
            )
        if _normalize_project(filename_name) != expected_name:
            raise ValueError("{} belongs to a different project".format(path.name))
        if filename_version != version:
            raise ValueError(
                "{} has version {}, expected {}".format(
                    path.name, filename_version, version
                )
            )
        if platform in wheel_platforms:
            raise ValueError("duplicate wheel platform: {}".format(platform))
        wheel_platforms.add(platform)
        metadata_name, metadata_version = _read_metadata(
            path, minimum_python, required_dependencies
        )
        if _normalize_project(metadata_name) != expected_name:
            raise ValueError("{} metadata names a different project".format(path.name))
        if metadata_version != version:
            raise ValueError(
                "{} metadata has version {}, expected {}".format(
                    path.name, metadata_version, version
                )
            )

    for path in sdists:
        if path.name not in {
            "{}-{}.tar.gz".format(project, version),
            "{}-{}.tar.gz".format(expected_name, version),
        }:
            raise ValueError(
                "unexpected source distribution filename: {}".format(path.name)
            )
        metadata_name, metadata_version = _read_metadata(
            path, minimum_python, required_dependencies
        )
        if _normalize_project(metadata_name) != expected_name:
            raise ValueError("{} metadata names a different project".format(path.name))
        if metadata_version != version:
            raise ValueError(
                "{} metadata has version {}, expected {}".format(
                    path.name, metadata_version, version
                )
            )

    if platforms is not None and wheel_platforms != platforms:
        raise ValueError(
            "wheel platforms differ: expected {}, found {}".format(
                sorted(platforms), sorted(wheel_platforms)
            )
        )
    return files


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--project", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--wheels", type=int, required=True)
    parser.add_argument("--sdists", type=int, default=0)
    parser.add_argument("--platform", action="append")
    parser.add_argument("--requires", action="append")
    args = parser.parse_args(argv)
    try:
        files = verify_distributions(
            args.directory,
            args.project,
            args.version,
            args.wheels,
            args.sdists,
            None if args.platform is None else set(args.platform),
            None if args.requires is None else set(args.requires),
        )
    except (OSError, ValueError, zipfile.BadZipFile, tarfile.TarError) as error:
        print("release distribution check failed: {}".format(error), file=sys.stderr)
        return 1
    print(
        "verified {} distributions for {} {}".format(
            len(files), args.project, args.version
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
