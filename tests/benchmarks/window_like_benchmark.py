import argparse
import json
import statistics
import time
from typing import Any, Dict, List, Optional

from sparkless.sql import SparkSession, functions as F


def _build_spark() -> SparkSession:
    """
    Construct a Sparkless SparkSession for benchmarks.

    Kept minimal so the benchmark focuses on the window-like workload
    rather than on session configuration.
    """

    return SparkSession.builder.appName("window_like_benchmark").getOrCreate()


def _generate_data(rows: int, groups: int) -> List[Dict[str, Any]]:
    """
    Generate synthetic rows for the window-like workload.

    Schema (mirrors the external benchmark at a high level):
    - group: integer group id in [0, groups)
    - id: monotonically increasing integer per row
    - value: simple numeric payload (float)
    """

    if groups <= 0:
        raise ValueError("groups must be positive")

    data: List[Dict[str, Any]] = []
    for i in range(rows):
        g = i % groups
        data.append(
            {
                "group": int(g),
                "id": int(i),
                "value": float(i % 100),
            }
        )
    return data


def _run_single_workload(
    spark: SparkSession, rows: int, groups: int
) -> Dict[str, float]:
    """
    Run a single instance of the window-like workload and return per-step timings.

    Workload shape (based on the issue/plan description):
    - createDataFrame(data)
    - groupBy("group").agg(count/sum/avg)
    - join back to original df on "group"
    - orderBy("group", "id").collect()
    """

    timings: Dict[str, float] = {}

    data = _generate_data(rows=rows, groups=groups)

    t0 = time.perf_counter()
    df = spark.createDataFrame(data)
    t1 = time.perf_counter()
    timings["createDataFrame"] = t1 - t0

    t0 = time.perf_counter()
    by_group = df.groupBy("group").agg(
        F.count(F.lit(1)).alias("count"),
        F.sum("value").alias("sum_value"),
        F.avg("value").alias("avg_value"),
    )
    t1 = time.perf_counter()
    timings["groupBy_agg"] = t1 - t0

    t0 = time.perf_counter()
    joined = df.join(by_group, on="group", how="inner")
    t1 = time.perf_counter()
    timings["join"] = t1 - t0

    t0 = time.perf_counter()
    ordered = joined.orderBy("group", "id")
    t1 = time.perf_counter()
    timings["orderBy"] = t1 - t0

    t0 = time.perf_counter()
    # The benchmark is focused on engine performance, so we collect but
    # do not retain the result.
    _ = ordered.collect()
    t1 = time.perf_counter()
    timings["collect"] = t1 - t0

    return timings


def _aggregate_timings(samples: List[Dict[str, float]]) -> Dict[str, Dict[str, float]]:
    """
    Aggregate a list of timing dictionaries into per-step statistics.

    For each step we report:
    - mean
    - median
    - min
    - max
    """

    if not samples:
        return {}

    keys = samples[0].keys()
    aggregated: Dict[str, Dict[str, float]] = {}
    for key in keys:
        values = [s[key] for s in samples]
        aggregated[key] = {
            "mean": statistics.fmean(values),
            "median": statistics.median(values),
            "min": min(values),
            "max": max(values),
        }
    return aggregated


def run_benchmark(rows: int, groups: int, repetitions: int) -> Dict[str, Any]:
    """
    Run the window-like benchmark for the given parameters.

    Returns a JSON-serializable dictionary containing:
    - params: benchmark parameters
    - per_run: list of per-step timings for each repetition
    - stats: aggregated per-step statistics across repetitions
    """

    if repetitions <= 0:
        raise ValueError("repetitions must be positive")

    spark = _build_spark()
    try:
        # Optional warmup so that subsequent runs are more stable.
        _run_single_workload(spark, rows=rows, groups=groups)

        runs: List[Dict[str, float]] = []
        for _ in range(repetitions):
            runs.append(_run_single_workload(spark, rows=rows, groups=groups))

        stats = _aggregate_timings(runs)
        return {
            "params": {
                "rows": rows,
                "groups": groups,
                "repetitions": repetitions,
            },
            "per_run": runs,
            "stats": stats,
        }
    finally:
        spark.stop()


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Window-like groupBy+join benchmark for Sparkless 4.x."
    )
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--groups", type=int, default=1_000)
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument(
        "--indent",
        type=int,
        default=2,
        help="JSON indentation level for pretty-printing results.",
    )

    args = parser.parse_args(argv)

    result = run_benchmark(
        rows=args.rows,
        groups=args.groups,
        repetitions=args.repetitions,
    )
    print(json.dumps(result, indent=args.indent, sort_keys=True))


if __name__ == "__main__":
    main()
