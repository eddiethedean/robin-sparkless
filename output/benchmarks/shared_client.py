import argparse
import concurrent.futures
import json
import time
from contextlib import ExitStack
from pathlib import Path

from pyspark.sql import SparkSession
from diverse_workloads import NAMES, workload
from compare_servers import timed, summary


def run(spark, name):
    if name == "orders_string_1000":
        df = spark.createDataFrame(
            [(i, i % 200) for i in range(1000)], "order_id LONG, total LONG"
        )
        assert df.filter("total > 99").count() == 500
    elif name.startswith("filtered_range_"):
        n = int(name.rsplit("_", 1)[1])
        assert spark.range(n).filter("id > 5").count() == n - 6
    else:
        workload(spark, name)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--url", required=True)
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    result = {"workloads": {}, "mixed_concurrency": {}}
    with ExitStack() as stack:
        s = SparkSession.builder.remote(args.url).create()
        stack.callback(s.stop)
        names = NAMES + [
            "orders_string_1000",
            "filtered_range_1000000",
            "filtered_range_10000000",
        ]
        samples = {n: [] for n in names}
        for name in names:
            run(s, name)
        for iteration in range(10):
            order = names[iteration % len(names) :] + names[: iteration % len(names)]
            for name in order:
                elapsed, _ = timed(lambda: run(s, name))
                samples[name].append(elapsed)
        result["workloads"] = {n: summary(v) for n, v in samples.items()}
        for workers in [1, 4, 8]:
            sessions = [
                SparkSession.builder.remote(args.url).create() for _ in range(workers)
            ]
            for session in sessions:
                stack.callback(session.stop)
                run(session, "preview_limit")
            mix = ["ingest_1000", "nullable_orders", "preview_limit", "collect_10000"]

            def client(item):
                i, session = item
                for name in mix[i % 4 :] + mix[: i % 4]:
                    run(session, name)

            rounds = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
                for _ in range(3):
                    t = time.perf_counter()
                    list(pool.map(client, enumerate(sessions)))
                    rounds.append(time.perf_counter() - t)
            result["mixed_concurrency"][str(workers)] = {
                "jobs_per_round": workers * 4,
                "rounds_seconds": rounds,
            }
    args.output.write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
