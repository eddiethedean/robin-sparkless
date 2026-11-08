"""Run with a native extension built with the sqlite Cargo feature."""

from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory

from sparkless.sql import SparkSession


def main():
    spark = SparkSession.builder.app_name("sqlite-guide").get_or_create()
    try:
        with TemporaryDirectory() as directory:
            database = Path(directory) / "fixture.db"
            with sqlite3.connect(database) as db:
                db.execute("CREATE TABLE users (id INTEGER, name TEXT)")
                db.executemany(
                    "INSERT INTO users VALUES (?, ?)", [(1, "Alice"), (2, "Bob")]
                )
                db.execute("CREATE TABLE users_backup (id INTEGER, name TEXT)")
            url = "jdbc:sqlite:" + str(database)
            frame = spark.read.jdbc(url, "users", properties={})
            assert frame.count() == 2
            frame.write.jdbc(url, "users_backup", properties={}, mode="append")
            with sqlite3.connect(database) as db:
                assert db.execute(
                    "SELECT id, name FROM users_backup ORDER BY id"
                ).fetchall() == [(1, "Alice"), (2, "Bob")]
        print("SQLite native read/write passed")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
