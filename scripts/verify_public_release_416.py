"""Verify the public 4.16.0 package inventory after recovery publication."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import time
from typing import Any
import urllib.error
import urllib.request


VERSION = "4.16.0"
PYPI_PLATFORMS = {
    "sparkless-server-bin": {
        "manylinux_2_35_x86_64",
        "macosx_15_0_arm64",
        "macosx_15_0_x86_64",
        "win_amd64",
    },
    "sparkless": {
        "macosx_10_12_x86_64",
        "macosx_11_0_arm64",
        "manylinux_2_17_x86_64.manylinux2014_x86_64",
        "manylinux_2_28_aarch64",
        "musllinux_1_2_aarch64",
        "musllinux_1_2_x86_64",
        "win_amd64",
        "win_arm64",
    },
}
CRATES = (
    "spark-sql-parser",
    "robin-sparkless-core",
    "robin-sparkless-polars",
    "robin-sparkless",
    "sparkless-server",
)
USER_AGENT = "sparkless-release-verifier (https://github.com/eddiethedean/robin-sparkless)"


def _fetch_json(url: str, timeout: float = 15.0) -> dict[str, Any]:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.load(response)


def validate_pypi_release(
    data: dict[str, Any],
    project: str,
    version: str = VERSION,
    local_directory: Path | None = None,
) -> None:
    if data.get("info", {}).get("version") != version:
        raise ValueError("{} does not report version {}".format(project, version))
    if data.get("info", {}).get("name", "").lower().replace("_", "-") != project:
        raise ValueError("PyPI returned metadata for the wrong project: {}".format(project))

    expected_platforms = PYPI_PLATFORMS[project]
    wheel_platforms = set()
    sdist_count = 0
    files = data.get("urls", [])
    for entry in files:
        filename = entry.get("filename", "")
        digest = entry.get("digests", {}).get("sha256", "")
        if not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise ValueError("{} has a malformed SHA-256 digest".format(filename))
        if filename.endswith(".whl"):
            fields = filename[:-4].split("-")
            if len(fields) != 5:
                raise ValueError("{} is not a valid wheel filename".format(filename))
            wheel_platforms.add(fields[-1])
        elif filename.endswith(".tar.gz"):
            sdist_count += 1
        else:
            raise ValueError("unexpected PyPI distribution: {}".format(filename))

    if wheel_platforms != expected_platforms:
        raise ValueError(
            "{} wheel platforms differ: expected {}, found {}".format(
                project, sorted(expected_platforms), sorted(wheel_platforms)
            )
        )
    expected_sdists = 1 if project == "sparkless" else 0
    if sdist_count != expected_sdists:
        raise ValueError(
            "{} has {} source distributions, expected {}".format(
                project, sdist_count, expected_sdists
            )
        )
    expected_files = len(expected_platforms) + expected_sdists
    if len(files) != expected_files:
        raise ValueError(
            "{} has {} files, expected {}".format(project, len(files), expected_files)
        )

    if local_directory is not None:
        local_hashes = {
            path.name: hashlib.sha256(path.read_bytes()).hexdigest()
            for path in local_directory.iterdir()
            if path.is_file()
        }
        published_hashes = {
            entry["filename"]: entry["digests"]["sha256"] for entry in files
        }
        if local_hashes != published_hashes:
            raise ValueError(
                "{} published filenames or SHA-256 digests differ from verified inputs".format(
                    project
                )
            )


def _retry(check, attempts: int = 20, interval: float = 5.0) -> None:
    last_error = None
    for attempt in range(attempts):
        try:
            check()
            return
        except (OSError, ValueError, urllib.error.URLError) as error:
            last_error = error
            if attempt + 1 < attempts:
                time.sleep(interval)
    raise RuntimeError("public release verification did not converge: {}".format(last_error))


def verify_public_release(server_bin_directory: Path, sparkless_directory: Path) -> None:
    for project in ("sparkless-server-bin", "sparkless"):
        url = "https://pypi.org/pypi/{}/{}/json".format(project, VERSION)
        local_directory = {
            "sparkless-server-bin": server_bin_directory,
            "sparkless": sparkless_directory,
        }[project]

        def check_pypi(project=project, url=url, local_directory=local_directory):
            validate_pypi_release(
                _fetch_json(url), project, local_directory=local_directory
            )

        _retry(check_pypi)

    for crate in CRATES:
        url = "https://crates.io/api/v1/crates/{}/{}".format(crate, VERSION)

        def check_crate(url=url, crate=crate):
            data = _fetch_json(url)
            record = data.get("version", {})
            if record.get("num") != VERSION or data.get("crate", {}).get("id") != crate:
                raise ValueError("crates.io returned the wrong release for {}".format(crate))

        _retry(check_crate)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server-bin-directory", type=Path, required=True)
    parser.add_argument("--sparkless-directory", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        verify_public_release(args.server_bin_directory, args.sparkless_directory)
    except (OSError, RuntimeError, ValueError, urllib.error.URLError) as error:
        print("public release verification failed: {}".format(error))
        return 1
    print("verified 4.16.0 PyPI file hashes and crates.io release artifacts")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
