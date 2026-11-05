# Performance and sizing

Sparkless reduces JVM/cluster startup overhead for local tests. No universal
speedup or dataset size is promised. Runtime depends on input size/types, IO,
query shape, concurrency, Python UDF boundaries, and hardware. Compare against
your actual alternative using the same work and verify output correctness first.

## Reproducible measurements

Record commit/package version, wheel/native features, OS/libc, CPU model/core
quota, memory limit, thread settings, input seed/row counts/schema, query text,
warmup, repetitions and exact command. Separate cold process/startup, warm query,
and end-to-end IO time. Report throughput, p50/p95 latency, peak RSS, failures
and output equality; retain raw samples. Do not compare an M4 laptop baseline
with an Ubuntu runner as a regression budget.

For the native service, use the checked-in
[Connect test/benchmark entry points](https://github.com/eddiethedean/robin-sparkless/blob/main/tests/connect/README.md)
and [V26 budget checker](https://github.com/eddiethedean/robin-sparkless/blob/main/scripts/verify_connect_v26_budget.py).
Read their current `--help`/README and use same-hardware baseline/candidate
reports. [Release evidence](RELEASE_EVIDENCE.md) distinguishes current diagnostics
from publication requirements. Historical PR benchmarks are not general product claims.

## Compare a small local test

Install `sparkless==4.19.0` in one environment and `pyspark==4.0.5` with
Java 17 in another. Save this as `time_orders.py` and run
`python time_orders.py sparkless` or `python time_orders.py pyspark` in the
matching environment. Use a fresh process for each run.

```python
import argparse
from statistics import median
from time import perf_counter

parser = argparse.ArgumentParser()
parser.add_argument("backend", choices=["sparkless", "pyspark"])
backend = parser.parse_args().backend

started = perf_counter()
if backend == "sparkless":
    from sparkless.sql import SparkSession
    spark = SparkSession.builder.app_name("time_orders").get_or_create()
else:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master("local[4]").appName("time_orders").getOrCreate()

try:
    orders = spark.createDataFrame(
        [(i, i % 200) for i in range(1_000)], "order_id LONG, total LONG"
    )
    assert orders.filter("total > 99").count() == 500
    first_result = perf_counter()

    samples = []
    for _ in range(10):
        before = perf_counter()
        assert orders.filter("total > 99").count() == 500
        samples.append(perf_counter() - before)
finally:
    spark.stop()

print(f"Startup and first result: {first_result - started:.3f} s")
print(f"Warm filter and count (median): {median(samples) * 1000:.1f} ms")
print(f"Full run including cleanup: {perf_counter() - started:.3f} s")
```

Repeat on the same host with the same CPU limits and compare like metrics. The
full-run time includes the first result and all ten warm actions. This measures
an in-process test with in-memory data. For a file pipeline, measure read,
transformation, write, and cleanup together with representative files.

## Size a workload

Start at one client and representative data; measure peak RSS through joins,
sorts, groupBy and output conversion, then increase concurrency gradually. A
small output can hide large intermediates. Account for process/runtime overhead,
input buffers, intermediate plans, output copies and Python/Pandas/Arrow copies.
Keep headroom for skew and outliers. Set OS/container memory/CPU limits and
admission below saturation; bounds on output batches do not bound process RSS.

Project/filter early, avoid unnecessary collect/toPandas/toArrow, prefer built-ins
to Python UDFs, and use `toArrowBatches` with explicit limits for incremental
output. Streaming Arrow results is not Structured Streaming. Row-at-a-time Python
UDFs materialize input and can erase performance gains. Test cancellation and
failed queries under load, because cooperative native work can retain capacity.

Choose the maximum dataset and concurrency from measured stable behavior on the
deployment host. If workloads require distributed scheduling, durable execution
recovery, or hard per-query isolation, use infrastructure that supplies those guarantees.
