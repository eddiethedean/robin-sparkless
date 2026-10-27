"""Validate wheel and source distributions before publishing a release."""

from __future__ import annotations

import argparse
from email.parser import BytesParser
from pathlib import Path
import re
import sys
import tarfile
import zipfile


def _normalize_project(name: str) -> str:
    return re.sub(r"[-_.]+", "_", name).lower()


def _read_metadata(path: Path, minimum_python: str | None = None) -> tuple[str, str]:
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
                item for item in archive.getmembers() if item.name.endswith("/PKG-INFO")
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
) -> list[Path]:
    files = sorted(item for item in directory.iterdir() if item.is_file())
    wheels = [item for item in files if item.name.endswith(".whl")]
    sdists = [item for item in files if item.name.endswith(".tar.gz")]
    if len(files) != len(wheels) + len(sdists):
        raise ValueError("distribution folder contains unexpected non-package files")
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
    minimum_python = (
        ">=3.11" if tuple(map(int, version.split(".")[:2])) >= (4, 17) else None
    )
    wheel_platforms = set()
    for path in wheels:
        fields = path.name[:-4].split("-")
        if len(fields) != 5:
            raise ValueError("{} is not a valid wheel filename".format(path.name))
        filename_name, filename_version, _python, _abi, platform = fields
        if (
            minimum_python
            and expected_name == "sparkless"
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
        metadata_name, metadata_version = _read_metadata(path, minimum_python)
        if _normalize_project(metadata_name) != expected_name:
            raise ValueError("{} metadata names a different project".format(path.name))
        if metadata_version != version:
            raise ValueError(
                "{} metadata has version {}, expected {}".format(
                    path.name, metadata_version, version
                )
            )

    for path in sdists:
        if path.name != "{}-{}.tar.gz".format(project, version):
            raise ValueError(
                "unexpected source distribution filename: {}".format(path.name)
            )
        metadata_name, metadata_version = _read_metadata(path, minimum_python)
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
    args = parser.parse_args(argv)
    try:
        files = verify_distributions(
            args.directory,
            args.project,
            args.version,
            args.wheels,
            args.sdists,
            None if args.platform is None else set(args.platform),
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
