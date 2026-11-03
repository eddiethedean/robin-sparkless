"""Correctness-checked Sparkless Connect versus same-version local PySpark.

Run --engine local with full pyspark==4.0.5; connect with
sparkless[server]==4.18.1 and pyspark-client==4.0.5. Results are seconds.
"""
import argparse
import concurrent.futures
import json
import os
import platform
import statistics
import time
from contextlib import ExitStack
from importlib.metadata import version
from pathlib import Path


def timed(fn):
    start = time.perf_counter()
    value = fn()
    return time.perf_counter() - start, value


def summary(samples):
    ordered = sorted(samples)
    return {"median_ms": statistics.median(samples) * 1000,
            "p95_ms": ordered[__import__('math').ceil(len(samples)*.95)-1] * 1000,
            "samples_seconds": samples}


def workload(spark, name):
    if name.startswith("count_"):
        n = int(name.split('_')[1])
        result = spark.range(n).filter("id > 5").count()
        assert result == n - 6, (name, result)
    elif name == "collect_1000":
        result = spark.range(1000).select("id").collect()
        assert [r.id for r in result] == list(range(1000))
    elif name == "orders_1000":
        df = spark.createDataFrame([(i, i % 200) for i in range(1000)],
                                   "order_id LONG, total LONG")
        assert df.filter("total > 99").count() == 500


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--engine', choices=['local', 'connect'], required=True)
    parser.add_argument('--phase', choices=['startup', 'warm'], required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = {"engine": args.engine, "phase": args.phase,
              "python": platform.python_version(), "machine": platform.machine(),
              "pyspark": version('pyspark' if args.engine == 'local' else 'pyspark-client'),
              "cpu_threads": 4, "warmup_iterations": 2, "measured_iterations": 10}
    with ExitStack() as stack:
        start = time.perf_counter()
        from pyspark.sql import SparkSession
        if args.engine == 'connect':
            from sparkless import SparklessServer
            server = stack.enter_context(SparklessServer())
            spark = SparkSession.builder.remote(server.spark_connect_url).create()
            result['sparkless'] = version('sparkless')
            result['server_binary'] = version('sparkless-server-bin')
        else:
            spark = (SparkSession.builder.master('local[4]').appName('server-comparison')
                     .config('spark.ui.enabled', 'false')
                     .config('spark.sql.shuffle.partitions', '4')
                     .config('spark.default.parallelism', '4')
                     .config('spark.driver.host', '127.0.0.1').getOrCreate())
            spark.sparkContext.setLogLevel('ERROR')
        stack.callback(spark.stop)
        result['startup_to_session_seconds'] = time.perf_counter() - start
        elapsed, _ = timed(lambda: workload(spark, 'count_1000'))
        result['first_job_seconds'] = elapsed
        result['startup_through_first_job_seconds'] = time.perf_counter() - start
        if args.phase == 'warm':
            names = ['count_1000', 'count_1000000', 'count_10000000',
                     'collect_1000', 'orders_1000']
            for name in names:
                for _ in range(2):
                    workload(spark, name)
            samples = {name: [] for name in names}
            # Rotate workload order to reduce systematic ordering effects.
            for iteration in range(10):
                rotated = names[iteration % len(names):] + names[:iteration % len(names)]
                for name in rotated:
                    elapsed, _ = timed(lambda: workload(spark, name))
                    samples[name].append(elapsed)
            result['workloads'] = {name: summary(values) for name, values in samples.items()}
            result['concurrency'] = {}
            for workers in [1, 4, 8]:
                sessions = []
                for _ in range(workers):
                    session = (SparkSession.builder.remote(server.spark_connect_url).create()
                               if args.engine == 'connect' else spark.newSession())
                    sessions.append(session)
                    if args.engine == 'connect':
                        stack.callback(session.stop)
                    workload(session, 'count_1000000')

                def client(session):
                    return [timed(lambda: workload(session, 'count_1000000'))[0]
                            for _ in range(5)]

                rounds = []
                all_samples = []
                with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
                    for _ in range(3):
                        started = time.perf_counter()
                        batches = list(pool.map(client, sessions))
                        rounds.append(time.perf_counter() - started)
                        all_samples.extend(t for batch in batches for t in batch)
                result['concurrency'][str(workers)] = {
                    'jobs_per_round': workers * 5, 'rounds_seconds': rounds,
                    'median_jobs_per_second': workers * 5 / statistics.median(rounds),
                    **summary(all_samples)}
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({k: v for k, v in result.items() if k not in ['workloads', 'concurrency']}), flush=True)


if __name__ == '__main__':
    main()
