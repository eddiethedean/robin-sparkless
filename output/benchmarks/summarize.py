import json
import statistics
from pathlib import Path

p = Path(__file__).resolve().parent
data = {
    engine: [json.loads((p / f"{engine}-warm-{i}.json").read_text()) for i in range(3)]
    for engine in ["connect", "local"]
}
labels = {
    "count_1000": "Filter + count, 1,000 range rows",
    "count_1000000": "Filter + count, 1 million range rows",
    "count_10000000": "Filter + count, 10 million range rows",
    "collect_1000": "Project + collect 1,000 range rows",
    "orders_1000": "Create 1,000 orders + filter + count",
}
lines = [
    "# Sparkless Connect vs local PySpark benchmark",
    "",
    "Measured on 2026-10-31. Apple M4 Pro, 12 logical CPUs, 24 GiB RAM, macOS 26.5.2.",
    "Python 3.11.15; PySpark / pyspark-client 4.0.5; Sparkless 4.18.1 with native/server 4.18.0; Java 17.",
    "",
    "| Workload | Sparkless Connect | Local Spark | Local / Sparkless time |",
    "|---|---:|---:|---:|",
]
results = {}
for name, label in labels.items():
    times = {
        engine: statistics.median(
            [t for run in runs for t in run["workloads"][name]["samples_seconds"]]
        )
        * 1000
        for engine, runs in data.items()
    }
    results[name] = times
    lines.append(
        f"| {label} | {times['connect']:.2f} ms | {times['local']:.2f} ms | {times['local'] / times['connect']:.2f}x |"
    )
lines += [
    "",
    "Ratios above 1 favor Sparkless; ratios below 1 favor local Spark.",
    "",
    "| Concurrent clients | Sparkless jobs/s | Local Spark jobs/s | Sparkless / local throughput |",
    "|---|---:|---:|---:|",
]
for workers in ["1", "4", "8"]:
    rates = {
        engine: 5
        * int(workers)
        / statistics.median(
            [t for run in runs for t in run["concurrency"][workers]["rounds_seconds"]]
        )
        for engine, runs in data.items()
    }
    results[f"clients_{workers}"] = rates
    lines.append(
        f"| {workers} | {rates['connect']:.1f} | {rates['local']:.1f} | {rates['connect'] / rates['local']:.2f}x |"
    )
startup = {
    engine: statistics.median(
        [
            json.loads((p / f"{engine}-startup-{i}.json").read_text())[
                "startup_through_first_job_seconds"
            ]
            for i in range(3)
        ]
    )
    for engine in data
}
results["startup_seconds"] = startup
lines += [
    "",
    f"Fresh-process imports, runtime/session startup and first successful 1,000-row filtered count: "
    f"Sparkless {startup['connect']:.3f} s; local Spark {startup['local']:.3f} s "
    f"({startup['local'] / startup['connect']:.2f}x). Median of three fresh processes per engine.",
    "",
    "## Method and limits",
    "",
    "- Both paths execute the same PySpark expressions and assert exact output correctness.",
    "- Local Spark uses local[4], four default partitions, four shuffle partitions and disabled UI. Sparkless uses four-thread Polars/Rayon/OpenMP limits and default admission settings (two active operations). CPU allocation is approximate, not enforced process isolation.",
    "- Warm results pool 30 observations per workload: three fresh runtimes, two warmups per workload per runtime, then ten timed actions. Workload order rotates. Startup is excluded from warm timings.",
    "- Concurrency uses independent client sessions in Python threads against one shared runtime, not separate pytest worker processes. Each client runs five sequential million-row filtered counts; three rounds per runtime, three runtimes. Throughput is jobs per round divided by median round duration. Local sessions share one SparkContext.",
    "- Engine processes run sequentially with alternating order between repetitions. Tests use warm filesystem caches, loopback networking and no explicit DataFrame caching. Installation and process shutdown are excluded.",
    "- Counts use synthetic ranges and a simple id > 5 predicate. Orders are Python tuples with an explicit schema, include ingestion time, and filter total > 99. Collection includes transfer and Python row construction.",
    "- These are microbenchmarks of the currently supported Connect subset, not general ETL or distributed-cluster benchmarks. No joins, grouped aggregates, shuffles, files, UDFs or sustained production traffic were tested. Range counts may use different engine optimizations.",
    "- The local Spark baseline is unusually inexpensive once warm. Do not extrapolate startup gains into warm-job or high-traffic performance claims.",
    "",
    "## Reproduce",
    "",
    "See run_comparison.py for exact environment settings and interpreter paths. Install full pyspark==4.0.5 in the local environment, and sparkless[server]==4.18.1 plus pyspark-client==4.0.5 in the Connect environment. Then run python3 run_comparison.py and python3 summarize.py. The scripts use this machine's Java 17 path; adapt that path on other machines.",
    "",
    "Raw per-action measurements and process logs are alongside this report.",
]
(p / "REPORT.md").write_text("\n".join(lines) + "\n")
(p / "summary.json").write_text(json.dumps(results, indent=2) + "\n")
print("\n".join(lines[:27]))
