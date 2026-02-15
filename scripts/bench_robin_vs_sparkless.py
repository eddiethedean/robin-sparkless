#!/usr/bin/env python3
"""
Compare performance of robin-sparkless vs sparkless (PySpark drop-in) on the same pipelines.

Requirements:
  - robin_sparkless: build with `maturin develop --features pyo3` from repo root
  - sparkless: pip install sparkless

Usage:
  python scripts/bench_robin_vs_sparkless.py
  python scripts/bench_robin_vs_sparkless.py --quick          # smaller sizes, fewer iterations
  python scripts/bench_robin_vs_sparkless.py --sizes 100000 500000 1000000
  python scripts/bench_robin_vs_sparkless.py --check-results

Output: Table of mean time (and std) per backend per pipeline; speedup(robin) when both available.
"""

from __future__ import annotations

import argparse
import sys
import time
import warnings
from collections.abc import Callable
from typing import Any

# Suppress Sparkless Polars LazyFrame schema warnings
warnings.filterwarnings("ignore", message=".*LazyFrame.*")
warnings.filterwarnings("ignore", message=".*schema.*", module=".*materializer.*")

# Defaults: realistic and big
DEFAULT_SIZES = [100_000, 500_000]
QUICK_SIZES = [10_000, 50_000]
DEFAULT_ITERATIONS = 4
QUICK_ITERATIONS = 2
WARMUP_ITERATIONS = 1

# Join/union pipeline sizes (fixed)
JOIN_SIZE = 50_000  # each side
UNION_SIZE = 200_000  # each df, then union
TOP_N = 10_000


def make_data(n: int) -> list[tuple[int, int, str]]:
    """Build (id, age, name) rows: age 0..79, names user_0, user_1, ..."""
    return [(i, i % 80, f"user_{i}") for i in range(n)]


# ---------- Pipeline 1: filter -> select -> groupBy count ----------
def run_robin_filter_select_groupby(
    data: list[tuple[int, int, str]], cols: list[str]
) -> Any:
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("bench").get_or_create()
    df = spark.createDataFrame(data, cols)
    return (
        df.filter(rs.col("age").gt(rs.lit(30)))
        .select(["name", "age"])
        .group_by(["age"])
        .count()
        .collect()
    )


def run_sparkless_filter_select_groupby(
    data: list[tuple[int, int, str]], cols: list[str]
) -> Any:
    from sparkless.sql import SparkSession
    import sparkless.sql.functions as F

    spark = SparkSession.builder.appName("bench").getOrCreate()
    df = spark.createDataFrame(data, cols)
    return (
        df.filter(F.col("age") > F.lit(30))
        .select("name", "age")
        .groupBy("age")
        .count()
        .collect()
    )


# ---------- Pipeline 2: groupBy(age).agg(sum(id), count) ----------
def run_robin_groupby_multi_agg(
    data: list[tuple[int, int, str]], cols: list[str]
) -> Any:
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("bench").get_or_create()
    df = spark.createDataFrame(data, cols)
    grouped = df.group_by(["age"])
    return grouped.agg(
        [
            rs.sum(rs.col("id")).alias("sum_id"),
            rs.count(rs.col("id")).alias("count_id"),
        ]
    ).collect()


def run_sparkless_groupby_multi_agg(
    data: list[tuple[int, int, str]], cols: list[str]
) -> Any:
    from sparkless.sql import SparkSession
    import sparkless.sql.functions as F

    spark = SparkSession.builder.appName("bench").getOrCreate()
    df = spark.createDataFrame(data, cols)
    return (
        df.groupBy("age")
        .agg(F.sum("id").alias("sum_id"), F.count("id").alias("count_id"))
        .collect()
    )


# ---------- Pipeline 3: orderBy(age).limit(TOP_N) ----------
def run_robin_order_by_limit(
    data: list[tuple[int, int, str]], cols: list[str], n: int
) -> Any:
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("bench").get_or_create()
    df = spark.createDataFrame(data, cols)
    return df.order_by(["age"], ascending=[False]).limit(n).collect()


def run_sparkless_order_by_limit(
    data: list[tuple[int, int, str]], cols: list[str], n: int
) -> Any:
    from sparkless.sql import SparkSession
    import sparkless.sql.functions as F

    spark = SparkSession.builder.appName("bench").getOrCreate()
    df = spark.createDataFrame(data, cols)
    return df.orderBy(F.desc("age")).limit(n).collect()


# ---------- Pipeline 4: distinct ----------
def run_robin_distinct(data: list[tuple[int, int, str]], cols: list[str]) -> Any:
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("bench").get_or_create()
    df = spark.createDataFrame(data, cols)
    return df.distinct(None).collect()


def run_sparkless_distinct(data: list[tuple[int, int, str]], cols: list[str]) -> Any:
    from sparkless.sql import SparkSession

    spark = SparkSession.builder.appName("bench").getOrCreate()
    df = spark.createDataFrame(data, cols)
    return df.distinct().collect()


# ---------- Pipeline 5: union two DataFrames then groupBy count ----------
def run_robin_union_then_groupby(
    data_a: list[tuple[int, int, str]],
    data_b: list[tuple[int, int, str]],
    cols: list[str],
) -> Any:
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("bench").get_or_create()
    df_a = spark.createDataFrame(data_a, cols)
    df_b = spark.createDataFrame(data_b, cols)
    return df_a.union(df_b).group_by(["age"]).count().collect()


def run_sparkless_union_then_groupby(
    data_a: list[tuple[int, int, str]],
    data_b: list[tuple[int, int, str]],
    cols: list[str],
) -> Any:
    from sparkless.sql import SparkSession

    spark = SparkSession.builder.appName("bench").getOrCreate()
    df_a = spark.createDataFrame(data_a, cols)
    df_b = spark.createDataFrame(data_b, cols)
    # unionByName so column order doesn't matter (Sparkless may reorder)
    return df_a.unionByName(df_b).groupBy("age").count().collect()


# ---------- Pipeline 6: inner join (JOIN_SIZE x JOIN_SIZE) ----------
def run_robin_join(
    data_a: list[tuple[int, int, str]], data_b: list[tuple[int, int, str]]
) -> Any:
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("bench").get_or_create()
    df_a = spark.createDataFrame(data_a, ["id", "age", "name"])
    df_b = spark.createDataFrame(data_b, ["id", "x", "y"])
    return df_a.join(df_b, ["id"], "inner").collect()


def run_sparkless_join(
    data_a: list[tuple[int, int, str]], data_b: list[tuple[int, int, str]]
) -> Any:
    from sparkless.sql import SparkSession

    spark = SparkSession.builder.appName("bench").getOrCreate()
    df_a = spark.createDataFrame(data_a, ["id", "age", "name"])
    df_b = spark.createDataFrame(data_b, ["id", "x", "y"])
    return df_a.join(df_b, "id", "inner").collect()


def time_it(
    fn: Callable[[], Any],
    iterations: int,
    warmup: int = WARMUP_ITERATIONS,
) -> tuple[float, float]:
    """Run fn warmup + iterations times; return (mean, std) of iteration times."""
    for _ in range(warmup):
        fn()
    times: list[float] = []
    for _ in range(iterations):
        t0 = time.perf_counter()
        fn()
        t1 = time.perf_counter()
        times.append(t1 - t0)
    n = len(times)
    mean = sum(times) / n
    variance = sum((t - mean) ** 2 for t in times) / n if n > 1 else 0.0
    std = variance**0.5
    return mean, std


def run_table(
    name: str,
    sizes: list[int],
    has_robin: bool,
    has_sparkless: bool,
    iterations: int,
    check_results: bool,
    make_row: Callable[
        [int], tuple[float | None, float | None, float | None, float | None, Any]
    ],
) -> None:
    """Print a benchmark table. make_row(n) returns (robin_mean, robin_std, sparkless_mean, sparkless_std, match?)."""
    print()
    print(name)
    print("-" * 72)
    header = [
        "n_rows",
        "robin_mean(s)",
        "robin_std(s)",
        "sparkless_mean(s)",
        "sparkless_std(s)",
        "speedup(robin)",
    ]
    if not has_robin:
        header = ["n_rows", "sparkless_mean(s)", "sparkless_std(s)"]
    if not has_sparkless:
        header = ["n_rows", "robin_mean(s)", "robin_std(s)"]
    print("  ".join(f"{h:>18}" for h in header))

    for n in sizes:
        robin_mean, robin_std, sparkless_mean, sparkless_std, _ = make_row(n)

        parts = [f"{n:>18}"]
        if has_robin:
            parts.append(f"{robin_mean:>18.4f}" if robin_mean is not None else " " * 18)
            parts.append(f"{robin_std:>18.4f}" if robin_std is not None else " " * 18)
        if has_sparkless:
            parts.append(
                f"{sparkless_mean:>18.4f}" if sparkless_mean is not None else " " * 18
            )
            parts.append(
                f"{sparkless_std:>18.4f}" if sparkless_std is not None else " " * 18
            )
        if (
            has_robin
            and has_sparkless
            and robin_mean
            and sparkless_mean
            and sparkless_mean > 0
        ):
            parts.append(f"{(sparkless_mean / robin_mean):>18.2f}x")
        print("  ".join(parts))


def main() -> int:
    """Parse args, run benchmark tables for each pipeline, return 0 on success."""
    ap = argparse.ArgumentParser(description="Benchmark robin-sparkless vs sparkless")
    ap.add_argument(
        "--sizes",
        type=int,
        nargs="+",
        default=None,
        help="Row counts (default: 100k, 500k; use --quick for 10k, 50k)",
    )
    ap.add_argument(
        "--iterations",
        type=int,
        default=None,
        help="Timing iterations (default: 4; 2 with --quick)",
    )
    ap.add_argument(
        "--quick", action="store_true", help="Smaller sizes and fewer iterations"
    )
    ap.add_argument(
        "--check-results",
        action="store_true",
        help="Verify result equality when both backends available",
    )
    ap.add_argument("--no-sparkless", action="store_true", help="Skip sparkless")
    ap.add_argument("--no-robin", action="store_true", help="Skip robin-sparkless")
    args = ap.parse_args()

    sizes = (
        args.sizes
        if args.sizes is not None
        else (QUICK_SIZES if args.quick else DEFAULT_SIZES)
    )
    iterations = (
        args.iterations
        if args.iterations is not None
        else (QUICK_ITERATIONS if args.quick else DEFAULT_ITERATIONS)
    )

    has_robin = False
    has_sparkless = False
    if not args.no_robin:
        try:
            import robin_sparkless  # noqa: F401

            has_robin = True
        except ImportError as e:
            print("robin_sparkless not available:", e, file=sys.stderr)
    if not args.no_sparkless:
        try:
            from sparkless.sql import SparkSession  # noqa: F401

            has_sparkless = True
        except ImportError as e:
            print("sparkless not available:", e, file=sys.stderr)

    if not has_robin and not has_sparkless:
        print("Need at least one of robin_sparkless or sparkless.", file=sys.stderr)
        return 1

    # Suppress Sparkless LazyFrame schema warnings for cleaner output
    cols = ["id", "age", "name"]

    # ----- Pipeline 1: filter -> select -> groupBy count -----
    def row1(n: int):
        data = make_data(n)
        r_m, r_s = (
            time_it(lambda: run_robin_filter_select_groupby(data, cols), iterations)
            if has_robin
            else (None, None)
        )
        s_m, s_s = (
            time_it(lambda: run_sparkless_filter_select_groupby(data, cols), iterations)
            if has_sparkless
            else (None, None)
        )
        return r_m, r_s, s_m, s_s, None

    run_table(
        "Pipeline 1: filter(age>30) -> select -> groupBy(age).count()",
        sizes,
        has_robin,
        has_sparkless,
        iterations,
        args.check_results,
        row1,
    )

    # ----- Pipeline 2: groupBy(age).agg(sum(id), count(id)) -----
    def row2(n: int):
        data = make_data(n)
        r_m, r_s = (
            time_it(lambda: run_robin_groupby_multi_agg(data, cols), iterations)
            if has_robin
            else (None, None)
        )
        s_m, s_s = (
            time_it(lambda: run_sparkless_groupby_multi_agg(data, cols), iterations)
            if has_sparkless
            else (None, None)
        )
        return r_m, r_s, s_m, s_s, None

    run_table(
        "Pipeline 2: groupBy(age).agg(sum(id), count(id))",
        sizes,
        has_robin,
        has_sparkless,
        iterations,
        args.check_results,
        row2,
    )

    # ----- Pipeline 3: orderBy(age desc).limit(TOP_N) -----
    def row3(n: int):
        data = make_data(n)
        r_m, r_s = (
            time_it(lambda: run_robin_order_by_limit(data, cols, TOP_N), iterations)
            if has_robin
            else (None, None)
        )
        s_m, s_s = (
            time_it(lambda: run_sparkless_order_by_limit(data, cols, TOP_N), iterations)
            if has_sparkless
            else (None, None)
        )
        return r_m, r_s, s_m, s_s, None

    run_table(
        f"Pipeline 3: orderBy(age desc).limit({TOP_N})",
        sizes,
        has_robin,
        has_sparkless,
        iterations,
        args.check_results,
        row3,
    )

    # ----- Pipeline 4: distinct -----
    def row4(n: int):
        data = make_data(n)
        r_m, r_s = (
            time_it(lambda: run_robin_distinct(data, cols), iterations)
            if has_robin
            else (None, None)
        )
        s_m, s_s = (
            time_it(lambda: run_sparkless_distinct(data, cols), iterations)
            if has_sparkless
            else (None, None)
        )
        return r_m, r_s, s_m, s_s, None

    run_table(
        "Pipeline 4: distinct()",
        sizes,
        has_robin,
        has_sparkless,
        iterations,
        args.check_results,
        row4,
    )

    # ----- Pipeline 5: union two DataFrames then groupBy count (fixed size) -----
    n_union = min(UNION_SIZE, max(sizes)) if sizes else UNION_SIZE
    data_union_a = make_data(n_union)
    data_union_b = make_data(n_union)

    def row5(_n: int):
        r_m, r_s = (
            time_it(
                lambda: run_robin_union_then_groupby(data_union_a, data_union_b, cols),
                iterations,
            )
            if has_robin
            else (None, None)
        )
        s_m, s_s = (
            time_it(
                lambda: run_sparkless_union_then_groupby(
                    data_union_a, data_union_b, cols
                ),
                iterations,
            )
            if has_sparkless
            else (None, None)
        )
        return r_m, r_s, s_m, s_s, None

    run_table(
        f"Pipeline 5: union two dfs (n={n_union} each) -> groupBy(age).count()",
        [n_union * 2],  # total rows
        has_robin,
        has_sparkless,
        iterations,
        args.check_results,
        row5,
    )

    # ----- Pipeline 6: inner join (JOIN_SIZE x JOIN_SIZE) -----
    n_join = min(JOIN_SIZE, max(sizes)) if sizes else JOIN_SIZE
    data_join_a = [(i, i % 50, f"a_{i}") for i in range(n_join)]
    data_join_b = [(i, i * 2, f"b_{i}") for i in range(n_join)]

    def row6(_n: int):
        r_m, r_s = (
            time_it(lambda: run_robin_join(data_join_a, data_join_b), iterations)
            if has_robin
            else (None, None)
        )
        s_m, s_s = (
            time_it(lambda: run_sparkless_join(data_join_a, data_join_b), iterations)
            if has_sparkless
            else (None, None)
        )
        return r_m, r_s, s_m, s_s, None

    run_table(
        f"Pipeline 6: inner join (n={n_join} x {n_join})",
        [n_join],
        has_robin,
        has_sparkless,
        iterations,
        args.check_results,
        row6,
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())
