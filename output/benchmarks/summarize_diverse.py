import json
import statistics
from pathlib import Path

from diverse_workloads import NAMES

ROOT = Path(__file__).resolve().parent
LABELS = {
    "ingest_10": "Ingest/filter/count 10 orders",
    "ingest_100": "Ingest/filter/count 100 orders",
    "ingest_1000": "Ingest/filter/count 1,000 orders",
    "ingest_10000": "Ingest/filter/count 10,000 orders",
    "orders_pipeline": "Filter → arithmetic/aliases → preview 25 orders",
    "nullable_orders": "Filter nullable orders → collect IDs",
    "scalar_roundtrip": "Round-trip seven scalar types, 256 rows",
    "wide_projection": "12-column arithmetic projection, 1,000 rows",
    "string_payload": "Collect 1,000 × 256-character strings",
    "empty_result": "Filter → empty result",
    "selectivity_none": "Million-row count: zero matches",
    "selectivity_one": "Million-row count: one match",
    "selectivity_half": "Million-row count: 50% match",
    "selectivity_all": "Million-row count: all match",
    "arithmetic_pipeline": "Million-row arithmetic → filter → count",
    "preview_limit": "Preview 25 rows from million-row range",
    "collect_10000": "Collect 10,000 range rows",
    "collect_100000": "Collect 100,000 range rows",
    "schema_100_columns": "Analyze 100-column schema",
}
configs = ["connect-default", "local", "connect-active4"]
data = {
    config: [
        json.loads((ROOT / f"diverse-{config}-{i}.json").read_text()) for i in range(3)
    ]
    for config in configs
}
summary = {"workloads": {}, "mixed_concurrency": {}, "pytest": {}}
lines = [
    "# Diverse Sparkless Connect vs local PySpark benchmarks",
    "",
    "Measured 2026-10-31 on Apple M4 Pro (12 logical CPUs, 24 GiB RAM), macOS 26.5.2.",
    "Python 3.11.15; PySpark / pyspark-client 4.0.5; Sparkless API 4.18.1, native/server 4.18.0; Java 17.",
    "",
    "## Warm, correctness-checked workloads",
    "",
    "Medians of 24 timed actions per cell, pooled from three independent runtimes. Each runtime runs one warmup and eight timed actions per workload. Both engines use identical expressions and exact expected-result assertions. Timings include plan creation, local-data construction/ingestion, transport, execution and validation; they exclude runtime startup.",
    "",
    "| Workload | Connect default | Local Spark | Connect active=4 | Local / default time |",
    "|---|---:|---:|---:|---:|",
]
for name in NAMES:
    times = {
        config: statistics.median(
            [
                value
                for run in runs
                for value in run["workloads"][name]["samples_seconds"]
            ]
        )
        * 1000
        for config, runs in data.items()
    }
    summary["workloads"][name] = times
    ratio = times["local"] / times["connect-default"]
    lines.append(
        f"| {LABELS[name]} | {times['connect-default']:.2f} ms | {times['local']:.2f} ms | {times['connect-active4']:.2f} ms | {ratio:.2f}× |"
    )
lines += [
    "",
    "Ratios above 1 favor Sparkless. Raising active-operation admission is a concurrency setting, not an expectation of better single-client performance.",
    "",
    "## Shared-runtime mixed clients",
    "",
    "Each client performs ingestion/filter/count, nullable-data filtering/collection, a 25-row preview and a 10,000-row collection. Each concurrency level has nine rounds across three fresh runtimes. Jobs/s = jobs per round divided by median round wall time. Independent sessions use Python threads, not pytest processes.",
    "",
    "| Clients | Connect default jobs/s | Local Spark jobs/s | Connect active=4 jobs/s |",
    "|---|---:|---:|---:|",
]
for workers in ["1", "4", "8"]:
    rates = {
        config: int(workers)
        * 4
        / statistics.median(
            [
                v
                for run in runs
                for v in run["mixed_concurrency"][workers]["rounds_seconds"]
            ]
        )
        for config, runs in data.items()
    }
    summary["mixed_concurrency"][workers] = rates
    lines.append(
        f"| {workers} | {rates['connect-default']:.1f} | {rates['local']:.1f} | {rates['connect-active4']:.1f} |"
    )
lines += [
    "",
    "## Actual pytest suites",
    "",
    "Each invocation runs 38 tests (two copies of all 19 workloads) using session-scoped fixtures, once serially and once with four pytest-xdist worker processes. Medians of three complete invocations; every test must pass. Wall time includes pytest launch, imports, runtime/server startup and cleanup. The shared Sparkless server is launched before pytest, and its launch/shutdown are included.",
    "",
    "Serial local Spark uses local[4]. With xdist, each local worker owns a local[1] JVM (four total compute slots); four Connect workers share one default-configured native server with four-thread pool limits and two active-operation slots. This compares practical fixture architectures: it does not attribute every difference to execution speed.",
    "",
    "| Suite | Shared Sparkless server | Local PySpark | Local / Sparkless time |",
    "|---|---:|---:|---:|",
]
pytest_file = ROOT / "pytest-results.json"
if pytest_file.exists():
    pytest = json.loads(pytest_file.read_text())
    for workers in [0, 4]:
        times = {
            engine: statistics.median(
                [
                    v["wall_seconds"]
                    for v in pytest
                    if v["engine"] == engine and v["xdist_workers"] == workers
                ]
            )
            for engine in ["connect", "local"]
        }
        summary["pytest"][str(workers)] = times
        label = "Serial, session fixture" if workers == 0 else "Four xdist workers"
        lines.append(
            f"| {label} | {times['connect']:.2f} s | {times['local']:.2f} s | {times['local'] / times['connect']:.2f}× |"
        )
lines += [
    "",
    "## Unsupported operations",
    "",
    "Grouped SUM, JOIN, ORDER BY and SQL were explicitly probed. These execute on local Spark and return unsupported-plan errors on this released Sparkless Connect server. They are compatibility gaps, not timed wins. No filesystem readers/writers or UDFs were benchmarked.",
    "",
    "## Configuration and interpretation",
    "",
    "- The full benchmark compares three configurations: Spark local[4] (UI off, four default/shuffle partitions, UTC); Sparkless default admission (two active operations); and Sparkless with SPARKLESS_MAX_ACTIVE_OPERATIONS=4. Both native configurations set POLARS_MAX_THREADS, RAYON_NUM_THREADS and OMP_NUM_THREADS to 4. These are configuration limits, not hard CPU quotas.",
    "- Three independent runs per configuration, with rotating engine order. Engines run sequentially, not alongside one another. Filesystem caches remain warm. No explicit DataFrame caching or result caching is requested.",
    "- Input data are generated in memory; ordinary Python tuples use explicit schemas. Mixed types cover BIGINT, DOUBLE, BOOLEAN, nullable STRING, DATE, TIMESTAMP and DECIMAL. Wide projection produces twelve arithmetic columns. The string test transfers approximately 256 KB of payload plus IDs/protocol overhead.",
    "- Selectivity counts use Column >= expressions, which are supported. The earlier graphic uses SQL-string > expressions. These are distinct parser paths.",
    "- Synthetic range workloads can benefit from different engine optimizations. This suite measures supported local testing paths, not distributed clusters, production ETL or sustained traffic capacity.",
    "- Single-client microbenchmarks and whole pytest runs answer different questions. Worker imports and fixture lifecycle can dominate short test suites.",
    "- No implementation changes or benchmark-specific server code were made. All results use published packages.",
    "",
    "## Reproduce",
    "",
    "Install full pyspark==4.0.5 plus pytest==9.2.0 and pytest-xdist==3.8.0 in the local environment; install sparkless[server]==4.18.1, pyspark-client==4.0.5 and the same pytest packages in the Connect environment. Adapt interpreter/Java paths in run_diverse.py. Run python3 run_diverse.py, then run_pytest_benchmarks.py with the Connect environment Python, then python3 summarize_diverse.py. Raw samples, logs and JUnit XML are alongside this report.",
]
(ROOT / "DIVERSE_REPORT.md").write_text("\n".join(lines) + "\n")
(ROOT / "diverse-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
print("\n".join(lines[:40]))
