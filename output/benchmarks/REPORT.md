# Sparkless Connect vs local PySpark benchmark

Measured on 2026-10-31. Apple M4 Pro, 12 logical CPUs, 24 GiB RAM, macOS 26.5.2.
Python 3.11.15; PySpark / pyspark-client 4.0.5; Sparkless 4.18.1 with native/server 4.18.0; Java 17.

| Workload | Sparkless Connect | Local Spark | Local / Sparkless time |
|---|---:|---:|---:|
| Filter + count, 1,000 range rows | 15.46 ms | 26.60 ms | 1.72x |
| Filter + count, 1 million range rows | 18.79 ms | 26.30 ms | 1.40x |
| Filter + count, 10 million range rows | 47.31 ms | 29.58 ms | 0.63x |
| Project + collect 1,000 range rows | 24.85 ms | 12.51 ms | 0.50x |
| Create 1,000 orders + filter + count | 16.30 ms | 76.19 ms | 4.68x |

Ratios above 1 favor Sparkless; ratios below 1 favor local Spark.

| Concurrent clients | Sparkless jobs/s | Local Spark jobs/s | Sparkless / local throughput |
|---|---:|---:|---:|
| 1 | 53.4 | 41.1 | 1.30x |
| 4 | 100.7 | 133.2 | 0.76x |
| 8 | 113.3 | 164.2 | 0.69x |

Fresh-process imports, runtime/session startup and first successful 1,000-row filtered count: Sparkless 0.338 s; local Spark 2.906 s (8.59x). Median of three fresh processes per engine.

## Method and limits

- Both paths execute the same PySpark expressions and assert exact output correctness.
- Local Spark uses local[4], four default partitions, four shuffle partitions and disabled UI. Sparkless uses four-thread Polars/Rayon/OpenMP limits and default admission settings (two active operations). CPU allocation is approximate, not enforced process isolation.
- Warm results pool 30 observations per workload: three fresh runtimes, two warmups per workload per runtime, then ten timed actions. Workload order rotates. Startup is excluded from warm timings.
- Concurrency uses independent client sessions in Python threads against one shared runtime, not separate pytest worker processes. Each client runs five sequential million-row filtered counts; three rounds per runtime, three runtimes. Throughput is jobs per round divided by median round duration. Local sessions share one SparkContext.
- Engine processes run sequentially with alternating order between repetitions. Tests use warm filesystem caches, loopback networking and no explicit DataFrame caching. Installation and process shutdown are excluded.
- Counts use synthetic ranges and a simple id > 5 predicate. Orders are Python tuples with an explicit schema, include ingestion time, and filter total > 99. Collection includes transfer and Python row construction.
- These are microbenchmarks of the currently supported Connect subset, not general ETL or distributed-cluster benchmarks. No joins, grouped aggregates, shuffles, files, UDFs or sustained production traffic were tested. Range counts may use different engine optimizations.
- The local Spark baseline is unusually inexpensive once warm. Do not extrapolate startup gains into warm-job or high-traffic performance claims.

## Reproduce

See run_comparison.py for exact environment settings and interpreter paths. Install full pyspark==4.0.5 in the local environment, and sparkless[server]==4.18.1 plus pyspark-client==4.0.5 in the Connect environment. Then run python3 run_comparison.py and python3 summarize.py. The scripts use this machine's Java 17 path; adapt that path on other machines.

Raw per-action measurements and process logs are alongside this report.
