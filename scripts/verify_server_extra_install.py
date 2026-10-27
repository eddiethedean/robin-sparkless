"""Smoke-test a freshly installed sparkless[server] consumer environment."""

from __future__ import annotations

import argparse
import importlib.metadata
import os
import sys
from pathlib import Path

from verify_server_http import verify_endpoint, verify_mutations


def verify(
    expected_version: str, protocol: str = "current", mutations: bool = False
) -> None:
    if mutations and protocol == "legacy-4.16":
        raise RuntimeError("mutation checks require the current server protocol")
    import sparkless
    from sparkless.server import SparklessServer
    from sparkless_server_bin import executable_path

    for distribution in ("sparkless", "sparkless-server-bin"):
        actual = importlib.metadata.version(distribution)
        if actual != expected_version:
            raise RuntimeError(
                "{} version {} does not match {}".format(
                    distribution, actual, expected_version
                )
            )
    if sparkless.__version__ != expected_version:
        raise RuntimeError("sparkless.__version__ does not match installed metadata")

    binary = Path(executable_path())
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise RuntimeError("server extra does not provide an executable binary")

    server = SparklessServer(startup_timeout=30, shutdown_timeout=15, kill_timeout=5)
    server.start()
    try:
        if protocol == "current" and not server.server_instance_id:
            raise RuntimeError("owned server did not report an instance identity")
        verify_endpoint(server.http_url)
        if mutations:
            verify_mutations(server.http_url)
    except BaseException:
        if server.diagnostics:
            print(server.diagnostics[-4096:], file=sys.stderr)
        raise
    finally:
        server.stop()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("version", help="expected coordinated release version")
    parser.add_argument(
        "--mutations", action="store_true", help="verify Phase 1 HTTP mutation outcomes"
    )
    parser.add_argument(
        "--protocol",
        choices=("current", "legacy-4.16"),
        default="current",
        help="control protocol expected from the packaged server binary",
    )
    args = parser.parse_args()
    try:
        verify(args.version, args.protocol, args.mutations)
    except (OSError, RuntimeError, importlib.metadata.PackageNotFoundError) as error:
        print("ERROR: {}".format(error), file=sys.stderr)
        return 1
    print("fresh sparkless[server] install launched and queried its packaged binary")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
