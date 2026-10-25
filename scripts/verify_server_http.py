"""Exercise the public HTTP health and SQL contract of a running server."""

from __future__ import annotations

import argparse
import json
import time
import urllib.error
import urllib.request


def verify_endpoint(base_url: str, startup_timeout: float = 60.0) -> None:
    endpoint = base_url.rstrip("/")
    deadline = time.monotonic() + startup_timeout
    while True:
        try:
            with urllib.request.urlopen(endpoint + "/health", timeout=5) as response:
                health = json.load(response)
            if health.get("status") == "ready":
                break
        except urllib.error.HTTPError as error:
            if error.code != 503:
                raise
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            pass

        if time.monotonic() >= deadline:
            raise RuntimeError("server did not become ready before the deadline")
        time.sleep(1)

    request = urllib.request.Request(
        endpoint + "/statements",
        data=json.dumps(
            {
                "session": "public-release-smoke",
                "kind": "sql",
                "code": "SELECT 1 AS result",
            }
        ).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        result = json.load(response)
    if result.get("status") != "ok":
        raise RuntimeError("server did not execute the SQL smoke query")
    values = result.get("data", {}).get("application/json", {}).get("data")
    if values != [[1]]:
        raise RuntimeError("server returned an unexpected SQL result")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("base_url", help="base HTTP URL of the running server")
    parser.add_argument("--startup-timeout", type=float, default=60.0)
    args = parser.parse_args()
    try:
        verify_endpoint(args.base_url, args.startup_timeout)
    except (OSError, RuntimeError) as error:
        parser.error(str(error))
    print("server health and HTTP SQL smoke passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
