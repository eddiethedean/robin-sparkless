"""Exercise the DuckDB Connect backend and a standard DuckDB client on one local file.

The local owner allows one writable process at a time, so the standalone DuckDB client and
Sparkless server open the same file sequentially. This smoke test covers public-client SQL
preflight/result behavior, typed writes, cross-client visibility, and restart persistence.
"""

from __future__ import annotations

import argparse
import importlib.metadata
import json
from pathlib import Path
import tempfile

import duckdb
from sparkless_server_duckdb import SparklessConnect


def server_options(server_bin: Path, database_path: Path) -> dict[str, object]:
    return {
        "executable_path": str(server_bin.resolve()),
        "startup_timeout": 90.0,
        "port": 0,
        "connect_port": 0,
        "env": {
            "SPARKLESS_EXECUTION_BACKEND": "duckdb",
            "SPARKLESS_DUCKDB_EXECUTION_OWNER": json.dumps(
                {"mode": "local", "database_path": str(database_path)}
            ),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--server-bin", type=Path, required=True)
    args = parser.parse_args()
    if importlib.metadata.version("pyspark-client") != "4.0.5":
        parser.error("the public client must be pyspark-client==4.0.5")
    if not args.server_bin.is_file():
        parser.error(f"server binary does not exist: {args.server_bin}")

    with tempfile.TemporaryDirectory(prefix="sparkless-phase420-duckdb-") as temp_dir:
        database_path = Path(temp_dir) / "shared.duckdb"
        with duckdb.connect(str(database_path)) as connection:
            connection.execute(
                "CREATE TABLE python_events (id BIGINT, label VARCHAR, active BOOLEAN)"
            )
            connection.executemany(
                "INSERT INTO python_events VALUES (?, ?, ?)",
                [(1, "one", True), (2, "two", False)],
            )

        options = server_options(args.server_bin, database_path)
        with SparklessConnect(server_options=options) as spark:
            source_rows = sorted(
                (row.id, row.label, row.active)
                for row in spark.table("python_events").collect()
            )
            assert source_rows == [(1, "one", True), (2, "two", False)]
            assert [
                row.value
                for row in spark.range(3).selectExpr("id + 1 AS value").collect()
            ] == [1, 2, 3]
            assert [row.value for row in spark.sql("SELECT 7 AS value").collect()] == [
                7
            ]
            assert (
                spark.sql(
                    "CREATE TABLE connect_events (id BIGINT, label STRING)"
                ).collect()
                == []
            )
            assert (
                spark.sql(
                    "INSERT INTO connect_events VALUES (10, 'ten'), (20, 'twenty')"
                ).collect()
                == []
            )
            assert sorted(
                (row.id, row.label) for row in spark.table("connect_events").collect()
            ) == [(10, "ten"), (20, "twenty")]
            assert [
                row.value
                for row in spark.sql(
                    "SELECT id + 1 AS value FROM python_events WHERE active = true"
                ).collect()
            ] == [2]

        # The configured local database is exclusive to one writable process at a time.
        # Reopen it through the standard DuckDB client after the server has stopped.
        with duckdb.connect(str(database_path)) as connection:
            assert connection.execute(
                "SELECT id, label FROM connect_events ORDER BY id"
            ).fetchall() == [(10, "ten"), (20, "twenty")]
            connection.execute("CREATE TABLE duckdb_events (id BIGINT, label VARCHAR)")
            connection.execute("INSERT INTO duckdb_events VALUES (30, 'thirty')")

        # Restart Sparkless against the same owner and verify DuckDB's commit is visible.
        with SparklessConnect(server_options=options) as spark:
            assert [
                (row.id, row.label) for row in spark.table("duckdb_events").collect()
            ] == [(30, "thirty")]

    print("Phase 4.20 public-client DuckDB execution smoke passed")


if __name__ == "__main__":
    main()
