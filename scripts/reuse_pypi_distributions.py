"""Use immutable published component files before testing a Python API release.

An API-only patch can pin an existing engine/server release. Download matching
published filenames and verify their hashes before the package smoke checks, so
publication retries and consumers use those exact bytes rather than a rebuild.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from verify_public_release_416 import preserve_existing_pypi_files


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--project", required=True)
    parser.add_argument("--version", required=True)
    args = parser.parse_args()
    preserved = preserve_existing_pypi_files(
        args.directory, args.project, args.version, allow_subset=True
    )
    print(f"Reused {len(preserved)} published {args.project} files before validation")


if __name__ == "__main__":
    main()
