"""Smoke the DuckDB execution mode of the release container image."""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path
import time
from urllib.error import URLError
from urllib.request import urlopen

from pyspark.sql.connect.session import SparkSession


def get_json(url: str, timeout: float = 2.0) -> dict[str, object]:
    with urlopen(url, timeout=timeout) as response:
        return json.load(response)


def wait_ready(http_url: str, timeout_seconds: float = 60.0) -> dict[str, object]:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            health = get_json(f"{http_url}/health")
            if health.get("status") == "ready":
                return get_json(f"{http_url}/capabilities")
        except (OSError, URLError, TimeoutError):
            pass
        time.sleep(0.25)
    raise TimeoutError(f"DuckDB container did not become ready at {http_url}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--http-url", required=True)
    parser.add_argument("--connect-url", required=True)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()

    capabilities = wait_ready(args.http_url)
    assert capabilities.get("server_product") == "duckdb", capabilities
    assert capabilities.get("execution_backend") == "duckdb", capabilities
    connect_url = args.connect_url.rstrip("/")
    if ";user_id=" not in connect_url:
        connect_url += "/;user_id=phase420-duckdb-container"

    spark = SparkSession.builder.remote(connect_url).getOrCreate()
    try:
        assert spark.sql("SELECT 7 AS value").collect()[0].value == 7
        spark.sql("CREATE TABLE container_events (id BIGINT, label STRING)").collect()
        spark.sql(
            "INSERT INTO container_events VALUES (1, 'started'), (2, 'finished')"
        ).collect()
        rows = sorted(
            (row.id, row.label) for row in spark.table("container_events").collect()
        )
        assert rows == [(1, "started"), (2, "finished")]
    finally:
        spark.stop()

    report = {
        "schema_version": 1,
        "phase": "4.20",
        "timestamp_utc": datetime.now(UTC).isoformat(),
        "http_url": args.http_url,
        "connect_url": args.connect_url,
        "server_product": capabilities["server_product"],
        "execution_backend": capabilities["execution_backend"],
        "read": "SELECT 7 AS value",
        "writes": ["typed CREATE TABLE", "literal INSERT INTO VALUES"],
        "rows": [list(row) for row in rows],
    }
    if args.report is not None:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print("Phase 4.20 DuckDB release-container smoke passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
