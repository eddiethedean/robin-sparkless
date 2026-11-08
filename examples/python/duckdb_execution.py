"""Verify Connect writes and sequential independent reads across a restart."""

import json
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb
from sparkless_server_duckdb import SparklessConnect


def main():
    with TemporaryDirectory() as directory:
        database = Path(directory) / "events.duckdb"
        options = {
            "env": {
                "SPARKLESS_EXECUTION_BACKEND": "duckdb",
                "SPARKLESS_DUCKDB_EXECUTION_OWNER": json.dumps(
                    {"mode": "local", "database_path": str(database)}
                ),
            }
        }
        with SparklessConnect(server_options=options) as spark:
            spark.sql("CREATE TABLE events (id BIGINT, label VARCHAR)").collect()
            spark.sql(
                "INSERT INTO events VALUES (1, 'started'), (2, 'finished')"
            ).collect()
            assert sorted((r.id, r.label) for r in spark.table("events").collect()) == [
                (1, "started"),
                (2, "finished"),
            ]
        # A local database has only one writable owner process at a time.
        with duckdb.connect(str(database)) as db:
            assert db.execute("SELECT count(*) FROM events").fetchone() == (2,)
        with SparklessConnect(server_options=options) as spark:
            assert spark.table("events").count() == 2
    print("DuckDB Connect write, independent read and restart passed")


if __name__ == "__main__":
    main()
