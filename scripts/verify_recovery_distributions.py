"""Revalidate previously verified release files before a PyPI recovery upload."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
import tarfile
import zipfile

from verify_public_release_416 import PYPI_PLATFORMS_BY_VERSION
from verify_release_distributions import verify_distributions


def verify_recovery_distributions(
    version: str, server_bin_directory: Path, sparkless_directory: Path
) -> None:
    try:
        expected = PYPI_PLATFORMS_BY_VERSION[version]
    except KeyError as error:
        raise ValueError(
            "no recovery inventory is recorded for version {}".format(version)
        ) from error

    verify_distributions(
        server_bin_directory,
        "sparkless-server-bin",
        version,
        wheel_count=len(expected["sparkless-server-bin"]),
        sdist_count=0,
        platforms=expected["sparkless-server-bin"],
    )
    verify_distributions(
        sparkless_directory,
        "sparkless",
        version,
        wheel_count=len(expected["sparkless"]),
        sdist_count=1,
        platforms=expected["sparkless"],
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True)
    parser.add_argument("--server-bin-directory", type=Path, required=True)
    parser.add_argument("--sparkless-directory", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        verify_recovery_distributions(
            args.version, args.server_bin_directory, args.sparkless_directory
        )
    except (OSError, ValueError, zipfile.BadZipFile, tarfile.TarError) as error:
        print("PyPI recovery distribution check failed: {}".format(error), file=sys.stderr)
        return 1
    print("verified recovery distributions for {}".format(args.version))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
