"""Preserve immutable files already uploaded for the 4.16.0 recovery."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

from verify_public_release_416 import preserve_existing_pypi_files


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument(
        "--project", choices=("sparkless", "sparkless-server-bin"), required=True
    )
    args = parser.parse_args(argv)
    try:
        preserved = preserve_existing_pypi_files(args.directory, args.project)
    except (OSError, ValueError) as error:
        print(
            "could not preserve existing PyPI files: {}".format(error), file=sys.stderr
        )
        return 1
    if preserved:
        print("preserved existing PyPI files: {}".format(", ".join(preserved)))
    else:
        print("no existing PyPI files need preservation")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
