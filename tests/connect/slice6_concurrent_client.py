"""Concurrent native count client coordinated by marker files from the harness."""

from __future__ import annotations

import argparse
import os
import time

from pyspark.sql.connect.session import SparkSession


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--ready-file", required=True)
    parser.add_argument("--barrier-file", required=True)
    parser.add_argument("--queries", type=int, default=4)
    args = parser.parse_args()

    spark = SparkSession.builder.remote(args.url).getOrCreate()
    try:
        with open(args.ready_file, "w", encoding="utf-8") as marker:
            marker.write("ready\n")
        deadline = time.monotonic() + 30
        while not os.path.exists(args.barrier_file):
            if time.monotonic() >= deadline:
                raise TimeoutError("timed out waiting for the concurrent-client barrier")
            time.sleep(0.01)

        for index in range(args.queries):
            value = spark.range(10_000_000).filter("id > 5").count()
            assert value == 9_999_994, value
            print(f"count {index + 1}/{args.queries} passed", flush=True)
            time.sleep(0.05)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
