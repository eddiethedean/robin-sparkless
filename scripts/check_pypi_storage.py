"""Check candidate distributions against PyPI's current project storage."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from verify_release_distributions import (
    PYPI_FILE_LIMIT_BYTES,
    PYPI_PROJECT_LIMIT_BYTES,
)


PublishedFile = tuple[str, int]


def fetch_project_files(project: str) -> dict[str, PublishedFile]:
    request = Request(
        f"https://pypi.org/pypi/{project}/json",
        headers={
            "Accept": "application/json",
            "Cache-Control": "no-cache",
            "User-Agent": "sparkless-release-storage-check/1.0",
        },
    )
    try:
        with urlopen(request, timeout=30) as response:
            data = json.load(response)
    except HTTPError as error:
        if error.code == 404:
            return {}
        raise

    files: dict[str, PublishedFile] = {}
    for release_files in data.get("releases", {}).values():
        for entry in release_files:
            filename = entry["filename"]
            digest = entry.get("digests", {}).get("sha256", "")
            size = entry.get("size")
            if len(digest) != 64 or not isinstance(size, int) or size < 0:
                raise ValueError(f"PyPI returned incomplete metadata for {filename}")
            published = (digest, size)
            previous = files.setdefault(filename, published)
            if previous != published:
                raise ValueError(f"PyPI returned conflicting metadata for {filename}")
    return files


def check_project_storage(
    directory: Path, published: dict[str, PublishedFile]
) -> tuple[int, int, int]:
    files = sorted(item for item in directory.iterdir() if item.is_file())
    if not files:
        raise ValueError(f"{directory} contains no distributions")
    published_hashes = {digest for digest, _size in published.values()}
    current_size = sum(size for _digest, size in published.values())
    additional_size = 0
    for path in files:
        size = path.stat().st_size
        if size > PYPI_FILE_LIMIT_BYTES:
            raise ValueError(f"{path.name} exceeds PyPI's 100 MiB per-file limit")
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        previous = published.get(path.name)
        if previous is not None:
            if previous[0] != digest:
                raise ValueError(
                    f"{path.name} already exists on PyPI with different contents"
                )
            continue
        if digest in published_hashes:
            raise ValueError(
                f"{path.name} duplicates a different file already published to PyPI"
            )
        additional_size += size

    projected_size = current_size + additional_size
    if projected_size > PYPI_PROJECT_LIMIT_BYTES:
        raise ValueError(
            "project would use {:.3f} GiB, above PyPI's 10 GiB project limit".format(
                projected_size / 1024**3
            )
        )
    return current_size, additional_size, projected_size


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--distribution",
        action="append",
        required=True,
        metavar="PROJECT=DIRECTORY",
        help="repeat once for each PyPI project in the release",
    )
    args = parser.parse_args()

    for value in args.distribution:
        project, separator, directory = value.partition("=")
        if not separator or not project or not directory:
            parser.error(f"invalid --distribution value: {value!r}")
        current, added, projected = check_project_storage(
            Path(directory), fetch_project_files(project)
        )
        print(
            "{}: current {:.3f} GiB, adding {:.2f} MiB, projected {:.3f} GiB "
            "(limit 10 GiB)".format(
                project, current / 1024**3, added / 1024**2, projected / 1024**3
            )
        )


if __name__ == "__main__":
    main()
