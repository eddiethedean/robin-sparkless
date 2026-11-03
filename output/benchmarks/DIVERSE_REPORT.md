# Diverse Sparkless Connect vs local PySpark benchmarks

Measured 2026-10-31 on Apple M4 Pro (12 logical CPUs, 24 GiB RAM), macOS 26.5.2.
Python 3.11.15; PySpark / pyspark-client 4.0.5; Sparkless API 4.18.1, native/server 4.18.0; Java 17.

## Warm, correctness-checked workloads

Medians of 24 timed actions per cell, pooled from three independent runtimes. Each runtime runs one warmup and eight timed actions per workload. Both engines use identical expressions and exact expected-result assertions. Timings include plan creation, local-data construction/ingestion, transport, execution and validation; they exclude runtime startup.

| Workload | Connect default | Local Spark | Connect active=4 | Local / default time |
|---|---:|---:|---:|---:|
| Ingest/filter/count 10 orders | 16.48 ms | 74.61 ms | 16.54 ms | 4.53× |
| Ingest/filter/count 100 orders | 16.19 ms | 73.49 ms | 16.24 ms | 4.54× |
| Ingest/filter/count 1,000 orders | 16.43 ms | 75.07 ms | 16.44 ms | 4.57× |
| Ingest/filter/count 10,000 orders | 19.04 ms | 85.85 ms | 19.05 ms | 4.51× |
| Filter → arithmetic/aliases → preview 25 orders | 16.79 ms | 60.70 ms | 16.80 ms | 3.61× |
| Filter nullable orders → collect IDs | 19.37 ms | 78.06 ms | 19.26 ms | 4.03× |
| Round-trip seven scalar types, 256 rows | 20.11 ms | 62.16 ms | 20.09 ms | 3.09× |
| 12-column arithmetic projection, 1,000 rows | 30.77 ms | 83.11 ms | 30.99 ms | 2.70× |
| Collect 1,000 × 256-character strings | 25.36 ms | 65.92 ms | 25.37 ms | 2.60× |
| Filter → empty result | 42.21 ms | 11.89 ms | 41.93 ms | 0.28× |
| Million-row count: zero matches | 19.19 ms | 26.16 ms | 19.21 ms | 1.36× |
| Million-row count: one match | 19.12 ms | 24.96 ms | 19.12 ms | 1.31× |
| Million-row count: 50% match | 18.99 ms | 24.19 ms | 19.03 ms | 1.27× |
| Million-row count: all match | 18.98 ms | 25.06 ms | 19.08 ms | 1.32× |
| Million-row arithmetic → filter → count | 27.55 ms | 27.11 ms | 29.28 ms | 0.98× |
| Preview 25 rows from million-row range | 15.69 ms | 8.57 ms | 15.64 ms | 0.55× |
| Collect 10,000 range rows | 112.25 ms | 16.64 ms | 113.29 ms | 0.15× |
| Collect 100,000 range rows | 1030.93 ms | 97.23 ms | 1020.67 ms | 0.09× |
| Analyze 100-column schema | 6.90 ms | 72.54 ms | 6.92 ms | 10.51× |

Ratios above 1 favor Sparkless. Raising active-operation admission is a concurrency setting, not an expectation of better single-client performance.

## Shared-runtime mixed clients

Each client performs ingestion/filter/count, nullable-data filtering/collection, a 25-row preview and a 10,000-row collection. Each concurrency level has nine rounds across three fresh runtimes. Jobs/s = jobs per round divided by median round wall time. Independent sessions use Python threads, not pytest processes.

| Clients | Connect default jobs/s | Local Spark jobs/s | Connect active=4 jobs/s |
|---|---:|---:|---:|
| 1 | 24.3 | 24.2 | 23.7 |
| 4 | 38.1 | 36.0 | 46.9 |
| 8 | 41.2 | 36.6 | 52.6 |

## Actual pytest suites

Each invocation runs 38 tests (two copies of all 19 workloads) using session-scoped fixtures, once serially and once with four pytest-xdist worker processes. Medians of three complete invocations; every test must pass. Wall time includes pytest launch, imports, runtime/server startup and cleanup. The shared Sparkless server is launched before pytest, and its launch/shutdown are included.

Serial local Spark uses local[4]. With xdist, each local worker owns a local[1] JVM (four total compute slots); four Connect workers share one default-configured native server with four-thread pool limits and two active-operation slots. This compares practical fixture architectures: it does not attribute every difference to execution speed.

| Suite | Shared Sparkless server | Local PySpark | Local / Sparkless time |
|---|---:|---:|---:|
| Serial, session fixture | 3.46 s | 6.99 s | 2.02× |
| Four xdist workers | 3.04 s | 6.60 s | 2.17× |

## Unsupported operations

Grouped SUM, JOIN, ORDER BY and SQL were explicitly probed. These execute on local Spark and return unsupported-plan errors on this released Sparkless Connect server. They are compatibility gaps, not timed wins. No filesystem readers/writers or UDFs were benchmarked.

## Configuration and interpretation

- The full benchmark compares three configurations: Spark local[4] (UI off, four default/shuffle partitions, UTC); Sparkless default admission (two active operations); and Sparkless with SPARKLESS_MAX_ACTIVE_OPERATIONS=4. Both native configurations set POLARS_MAX_THREADS, RAYON_NUM_THREADS and OMP_NUM_THREADS to 4. These are configuration limits, not hard CPU quotas.
- Three independent runs per configuration, with rotating engine order. Engines run sequentially, not alongside one another. Filesystem caches remain warm. No explicit DataFrame caching or result caching is requested.
- Input data are generated in memory; ordinary Python tuples use explicit schemas. Mixed types cover BIGINT, DOUBLE, BOOLEAN, nullable STRING, DATE, TIMESTAMP and DECIMAL. Wide projection produces twelve arithmetic columns. The string test transfers approximately 256 KB of payload plus IDs/protocol overhead.
- Selectivity counts use Column >= expressions, which are supported. The earlier graphic uses SQL-string > expressions. These are distinct parser paths.
- Synthetic range workloads can benefit from different engine optimizations. This suite measures supported local testing paths, not distributed clusters, production ETL or sustained traffic capacity.
- Single-client microbenchmarks and whole pytest runs answer different questions. Worker imports and fixture lifecycle can dominate short test suites.
- No implementation changes or benchmark-specific server code were made. All results use published packages.

## Reproduce

Install full pyspark==4.0.5 plus pytest==9.2.0 and pytest-xdist==3.8.0 in the local environment; install sparkless[server]==4.18.1, pyspark-client==4.0.5 and the same pytest packages in the Connect environment. Adapt interpreter/Java paths in run_diverse.py. Run python3 run_diverse.py, then run_pytest_benchmarks.py with the Connect environment Python, then python3 summarize_diverse.py. Raw samples, logs and JUnit XML are alongside this report.
