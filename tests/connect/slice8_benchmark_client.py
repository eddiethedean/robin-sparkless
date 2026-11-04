"""Run the fixed, correctness-checked client workload for Slice 8 benchmarks."""

from __future__ import annotations

import argparse
import json
import resource
import sys
import time
from pathlib import Path

from pyspark.sql.connect.session import SparkSession


def peak_rss_bytes() -> int:
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(value if sys.platform == "darwin" else value * 1024)


def wait_for_barrier(path: Path, timeout_seconds: float = 60) -> None:
    deadline = time.monotonic() + timeout_seconds
    while not path.exists():
        if time.monotonic() >= deadline:
            raise TimeoutError("timed out waiting for the benchmark start barrier")
        time.sleep(0.01)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--rows", type=int, required=True)
    parser.add_argument("--batch-rows", type=int, default=20_000)
    parser.add_argument("--iterations", type=int, required=True)
    parser.add_argument("--profile", choices=("v26", "419"), default="v26")
    parser.add_argument("--duration-seconds", type=int, default=60)
    parser.add_argument("--ready-file", type=Path, required=True)
    parser.add_argument("--barrier-file", type=Path, required=True)
    args = parser.parse_args()

    if args.rows < 10 or args.batch_rows < 1 or args.iterations < 1:
        raise ValueError("rows, batch rows, and iterations must be positive")
    if args.profile == "419" and args.duration_seconds < 60:
        raise ValueError("the 4.19 scheduling profile requires at least 60 seconds")

    session = SparkSession.builder.remote(args.url).create()
    measurements: list[dict[str, object]] = []
    try:
        warm_count = session.range(args.rows).filter("id > 5").count()
        if warm_count != args.rows - 6:
            raise AssertionError(
                f"warmup count was {warm_count}, expected {args.rows - 6}"
            )
        warm_rows = session.range(32).select("id").collect()
        if [row.id for row in warm_rows] != list(range(32)):
            raise AssertionError("warmup projection returned incorrect rows")

        args.ready_file.write_text("ready\n", encoding="utf-8")
        wait_for_barrier(args.barrier_file)
        started = time.perf_counter()
        cpu_started = time.process_time()
        if args.profile == "v26":
            for iteration in range(args.iterations):
                query_started = time.perf_counter()
                count = session.range(args.rows).filter("id > 5").count()
                count_elapsed = time.perf_counter() - query_started
                if count != args.rows - 6:
                    raise AssertionError(f"count was {count}, expected {args.rows - 6}")
                measurements.append(
                    {
                        "kind": "filtered_count",
                        "iteration": iteration,
                        "elapsed_ms": count_elapsed * 1000,
                        "result": count,
                    }
                )

                query_started = time.perf_counter()
                rows = session.range(args.batch_rows).select("id").collect()
                collect_elapsed = time.perf_counter() - query_started
                values = [row.id for row in rows]
                if values != list(range(args.batch_rows)):
                    raise AssertionError("multi-batch collection returned incorrect rows")
                measurements.append(
                    {
                        "kind": "multi_batch_collect",
                        "iteration": iteration,
                        "elapsed_ms": collect_elapsed * 1000,
                        "row_count": len(values),
                        "first": values[0],
                        "last": values[-1],
                    }
                )
        else:
            workload_names = ("small_count", "filtered_count", "preview_25", "multi_batch_collect")
            deadline = time.perf_counter() + args.duration_seconds
            iteration = 0
            while iteration == 0 or time.perf_counter() < deadline:
                offset = iteration % len(workload_names)
                for name in workload_names[offset:] + workload_names[:offset]:
                    query_started = time.perf_counter()
                    if name == "small_count":
                        result = session.range(10).filter("id > 5").count()
                        if result != 4:
                            raise AssertionError(f"small count was {result}, expected 4")
                        measurement = {"result": result}
                    elif name == "filtered_count":
                        result = session.range(args.rows).filter("id > 5").count()
                        if result != args.rows - 6:
                            raise AssertionError(
                                f"filtered count was {result}, expected {args.rows - 6}"
                            )
                        measurement = {"result": result}
                    elif name == "preview_25":
                        rows = session.range(args.rows).select("id").limit(25).collect()
                        values = [row.id for row in rows]
                        if values != list(range(25)):
                            raise AssertionError("25-row preview returned incorrect values")
                        measurement = {"row_count": len(values)}
                    else:
                        rows = session.range(args.batch_rows).select("id").collect()
                        values = [row.id for row in rows]
                        if values != list(range(args.batch_rows)):
                            raise AssertionError("mixed collection returned incorrect values")
                        measurement = {"row_count": len(values)}
                    measurements.append(
                        {
                            "kind": name,
                            "iteration": iteration,
                            "elapsed_ms": (time.perf_counter() - query_started) * 1000,
                            **measurement,
                        }
                    )
                iteration += 1

        output = {
            "client_session_fingerprint": __import__("hashlib")
            .sha256(session._client._session_id.encode())
            .hexdigest()[:16],
            "iterations": args.iterations if args.profile == "v26" else None,
            "profile": args.profile,
            "duration_seconds": args.duration_seconds if args.profile == "419" else None,
            "elapsed_seconds": time.perf_counter() - started,
            "cpu_seconds": time.process_time() - cpu_started,
            "peak_rss_bytes": peak_rss_bytes(),
            "measurements": measurements,
        }
        print(json.dumps(output, sort_keys=True), flush=True)
    finally:
        session.stop()


if __name__ == "__main__":
    main()
