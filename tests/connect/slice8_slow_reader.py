"""Consume a multi-batch public result slowly while the harness probes health."""

from __future__ import annotations

import argparse
import json
import time

from pyspark.sql.connect.session import SparkSession


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--rows", type=int, default=200_000)
    parser.add_argument("--pause-every", type=int, default=4096)
    parser.add_argument("--pause-ms", type=float, default=10)
    args = parser.parse_args()
    if args.rows < 1 or args.pause_every < 1 or args.pause_ms < 0:
        raise ValueError(
            "slow-reader settings must be non-negative and rows must be positive"
        )

    session = SparkSession.builder.remote(args.url).create()
    try:
        started = time.perf_counter()
        cpu_started = time.process_time()
        observed = 0
        for expected, row in enumerate(
            session.range(args.rows).select("id").toLocalIterator()
        ):
            if row.id != expected:
                raise AssertionError(
                    f"slow reader expected row {expected}, got {row.id}"
                )
            observed += 1
            if observed % args.pause_every == 0:
                time.sleep(args.pause_ms / 1000)
        elapsed = time.perf_counter() - started
        if observed != args.rows:
            raise AssertionError(
                f"slow reader consumed {observed}, expected {args.rows}"
            )
        print(
            json.dumps(
                {
                    "rows": observed,
                    "elapsed_seconds": elapsed,
                    "cpu_seconds": time.process_time() - cpu_started,
                    "pause_every": args.pause_every,
                    "pause_ms": args.pause_ms,
                },
                sort_keys=True,
            ),
            flush=True,
        )
    finally:
        session.stop()


if __name__ == "__main__":
    main()
