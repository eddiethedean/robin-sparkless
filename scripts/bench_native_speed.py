#!/usr/bin/env python3
"""
Benchmark script to measure native (Rust) Sparkless performance.

Run from repo root (e.g. from project root):

  # Time DEBUG build (default after maturin develop)
  maturin develop
  python scripts/bench_native_speed.py

  # Time RELEASE build
  maturin develop --release
  python scripts/bench_native_speed.py

  # Compare the "TOTAL" line; release is typically 2–3x faster.

Uses Sparkless native API directly. 100k rows × 5 iterations per benchmark
ensure the Rust path dominates so debug vs release difference is visible.
"""

from __future__ import annotations

import sys
import time

from sparkless.sql import SparkSession
from sparkless.sql import functions as F


def _elapsed(sec: float) -> str:
    if sec >= 1:
        return f"{sec:.2f}s"
    return f"{sec * 1000:.0f}ms"


def run_benchmark(name: str, fn, iterations: int = 5) -> float:
    """Run fn() `iterations` times; return mean elapsed seconds."""
    times: list[float] = []
    for _ in range(iterations):
        t0 = time.perf_counter()
        fn()
        times.append(time.perf_counter() - t0)
    mean_sec = sum(times) / len(times)
    print(f"  {name}: {_elapsed(mean_sec)}  (mean of {iterations} runs)")
    return mean_sec


def main() -> int:
    n_rows = 100_000
    iterations = 5

    print("Benchmarking native Sparkless (Robin backend)")
    print(f"  n_rows={n_rows:,}  iterations={iterations}")
    print()

    spark = SparkSession.builder.appName("bench_native").getOrCreate()

    # Build data once (Python side)
    data = [(i, i % 100, f"user_{i}") for i in range(n_rows)]
    columns = ["id", "value", "name"]

    results: list[tuple[str, float]] = []

    # 1. createDataFrame + collect
    def bench_create_collect() -> None:
        df = spark.createDataFrame(data, columns)
        df.collect()

    results.append(
        (
            "createDataFrame + collect",
            run_benchmark(
                "createDataFrame + collect", bench_create_collect, iterations
            ),
        )
    )

    # 2. createDataFrame + filter + collect
    def bench_filter() -> None:
        df = spark.createDataFrame(data, columns)
        df.filter(F.col("value") > 50).collect()

    results.append(
        (
            "filter + collect",
            run_benchmark("filter + collect", bench_filter, iterations),
        )
    )

    # 3. createDataFrame + withColumn (expr) + collect
    def bench_with_column() -> None:
        df = spark.createDataFrame(data, columns)
        df.withColumn("double", F.col("value") * 2).withColumn(
            "name_len", F.length(F.col("name"))
        ).collect()

    results.append(
        (
            "withColumn (expr) + collect",
            run_benchmark("withColumn (expr) + collect", bench_with_column, iterations),
        )
    )

    # 4. createDataFrame + groupBy + count + collect
    def bench_groupby() -> None:
        df = spark.createDataFrame(data, columns)
        df.groupBy("value").count().collect()

    results.append(
        (
            "groupBy + count + collect",
            run_benchmark("groupBy + count + collect", bench_groupby, iterations),
        )
    )

    # 5. Pipeline: create + filter + withColumn + select + collect
    def bench_pipeline() -> None:
        df = spark.createDataFrame(data, columns)
        (
            df.filter(F.col("value") > 20)
            .withColumn("extra", F.col("id") + F.col("value"))
            .select("id", "value", "extra")
            .collect()
        )

    results.append(
        (
            "filter+withColumn+select+collect",
            run_benchmark(
                "filter+withColumn+select+collect", bench_pipeline, iterations
            ),
        )
    )

    # 6. Heavy expressions: multiple withColumns with math/string
    def bench_heavy_expr() -> None:
        df = spark.createDataFrame(data, columns)
        (
            df.withColumn("a", F.col("value") * 2)
            .withColumn("b", F.length(F.col("name")))
            .withColumn("c", F.col("id") % 10)
            .withColumn("d", F.when(F.col("value") > 50, 1).otherwise(0))
            .filter(F.col("a") > 10)
            .collect()
        )

    results.append(
        (
            "heavy exprs + filter + collect",
            run_benchmark(
                "heavy exprs + filter + collect", bench_heavy_expr, iterations
            ),
        )
    )

    try:
        spark.stop()
    except Exception:
        pass

    total = sum(r for _, r in results)
    print()
    print("Summary (mean time per benchmark)")
    print("-" * 50)
    for name, sec in results:
        print(f"  {_elapsed(sec):>10}  {name}")
    print("-" * 50)
    print(f"  {_elapsed(total):>10}  TOTAL")
    print()
    print(
        "Run with 'maturin develop' vs 'maturin develop --release' and compare totals."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
