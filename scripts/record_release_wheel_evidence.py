"""Record a successful suite against the exact pre-install wheel digest."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
import tempfile

from release_wheel_evidence import build_evidence, write_evidence
from verify_wheel_runtime import _verify_native_binary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wheel", type=Path, required=True)
    parser.add_argument("--expected-sha256", required=True)
    parser.add_argument("--suite", required=True)
    parser.add_argument("--target", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        with tempfile.TemporaryDirectory(
            prefix="sparkless-wheel-inspect-"
        ) as temporary:
            _verify_native_binary(
                args.wheel,
                Path(temporary),
                build_evidence(
                    wheel=args.wheel,
                    expected_sha256=args.expected_sha256,
                    suite=args.suite,
                    target=args.target,
                )["machine"],
            )
        evidence = build_evidence(
            wheel=args.wheel,
            expected_sha256=args.expected_sha256,
            suite=args.suite,
            target=args.target,
        )
        write_evidence(evidence, args.output)
    except (OSError, ValueError, ImportError, ModuleNotFoundError) as error:
        print("Release wheel evidence failed: {}".format(error), file=sys.stderr)
        return 1
    print(
        "Recorded {} evidence for {} ({})".format(
            args.suite, args.wheel.name, evidence["sha256"]
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
