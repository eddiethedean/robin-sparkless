# Shared Spark Connect server comparison

Measured 2026-10-31 on the same Apple M4 Pro, 12 logical CPUs, 24 GiB RAM, macOS 26.5.2.
Python 3.11.15; identical pyspark-client==4.0.5 environment for both engines. Sparkless API 4.18.1 / native and server 4.18.0 versus Apache Spark JVM Connect server 4.0.5 with Java 17.

## Matched topology and configuration

- Both engines expose one shared loopback Spark Connect server. Four pytest-xdist workers each own an independent session on that same server. No classic JVM is launched in a test worker.
- JVM: local[4], four default and shuffle partitions, FAIR scheduling, UTC, Spark UI off, 2 GiB driver heap, -XX:ActiveProcessorCount=4. Sparkless: four active operations, POLARS_MAX_THREADS=4, RAYON_NUM_THREADS=4, OMP_NUM_THREADS=4. These approximate equal compute configurations, not hard CPU/RSS quotas; thread, partition and operation semantics differ between engines.
- Both use the same 38 correctness-checked tests: two copies of 19 supported workloads. Both use the same PySpark client, serialization, pytest plugins and test fixture. All pytest suites start fresh client processes.
- Three fresh server runs per engine, alternating engine order. One engine runs at a time. Filesystem caches remain warm; package installation is excluded. No explicit DataFrame caching.

## Actual four-worker pytest suite

| Measurement | Sparkless Connect | JVM Spark Connect | JVM / Sparkless time |
|---|---:|---:|---:|
| Server startup → first successful remote count | 0.090 s | 3.403 s | 37.98× |
| Server startup + readiness count + first 38-test suite | 2.958 s | 5.107 s | 1.73× |
| 38-test suite against already running server | 2.699 s | 1.378 s | 0.51× |

Each cell is the median of three independent server runs. Startup includes native/JVM server launch and a successful remote range(1).count(), followed by session close. The first suite immediately follows that readiness witness. Warm-suite time includes pytest launch, four client-worker imports and session setup/cleanup; server startup and server shutdown are excluded. Each server is stopped after the subsequent microbenchmarks. This avoids attributing server startup gains to warm execution.

## Warm workloads using the identical remote client

Medians pool 30 actions per workload across three independent servers, after the two pytest suites and one explicit workload warmup per server. Each action includes input/plan creation and exact output validation. This is a warm-runtime comparison.

| Workload | Sparkless Connect | JVM Spark Connect | JVM / Sparkless time |
|---|---:|---:|---:|
| Ingest/filter/count 10 orders | 16.20 ms | 13.21 ms | 0.82× |
| Ingest/filter/count 100 orders | 16.15 ms | 13.26 ms | 0.82× |
| Ingest/filter/count 1,000 orders | 16.37 ms | 25.20 ms | 1.54× |
| Ingest/filter/count 10,000 orders | 19.09 ms | 30.67 ms | 1.61× |
| Filter → arithmetic/aliases → preview 25 orders | 16.80 ms | 8.34 ms | 0.50× |
| Filter nullable orders → collect IDs | 19.37 ms | 7.50 ms | 0.39× |
| Round-trip seven scalar types, 256 rows | 20.04 ms | 7.41 ms | 0.37× |
| 12-column arithmetic projection, 1,000 rows | 30.90 ms | 13.98 ms | 0.45× |
| Collect 1,000 × 256-character strings | 25.32 ms | 7.14 ms | 0.28× |
| Filter → empty result | 42.06 ms | 8.39 ms | 0.20× |
| Million-row count: zero matches | 19.16 ms | 23.14 ms | 1.21× |
| Million-row count: one match | 19.10 ms | 22.47 ms | 1.18× |
| Million-row count: 50% match | 19.01 ms | 22.52 ms | 1.18× |
| Million-row count: all match | 19.11 ms | 22.83 ms | 1.19× |
| Million-row arithmetic → filter → count | 27.53 ms | 25.42 ms | 0.92× |
| Preview 25 rows from million-row range | 15.66 ms | 16.32 ms | 1.04× |
| Collect 10,000 range rows | 114.14 ms | 22.71 ms | 0.20× |
| Collect 100,000 range rows | 1029.67 ms | 131.79 ms | 0.13× |
| Analyze 100-column schema | 6.89 ms | 8.98 ms | 1.30× |
| Graphic test: ingest/filter/count 1,000 orders (string predicate) | 16.51 ms | 24.77 ms | 1.50× |
| Range/filter/count 1 million (string predicate) | 18.80 ms | 22.48 ms | 1.20× |
| Range/filter/count 10 million (string predicate) | 47.27 ms | 26.24 ms | 0.56× |

Ratios above one favor Sparkless; ratios below one favor JVM Spark.

## Warm mixed-client throughput

Same four-job mix per client as before: ingest/filter/count, nullable-data collection, 25-row preview, and 10k-row collection. Nine rounds across three servers per engine. Jobs/s = jobs per round / median round time. Independent sessions run in Python threads for this measurement; actual pytest above uses processes.

| Clients | Sparkless jobs/s | JVM Spark jobs/s |
|---|---:|---:|
| 1 | 24.1 | 56.0 |
| 4 | 46.9 | 136.0 |
| 8 | 52.1 | 142.2 |

## Scope and previous results

This comparison replaces the older 2.2× suite claim for a shared-server comparison. The older claim used four independent local[1] JVMs, and remains valid only for that architecture. The older 4.7× order-test claim compared Sparkless Connect with classic local[4] PySpark; use the new remote-client measurement when comparing shared servers.
Only the released Sparkless Connect subset was measured. This is a single-machine supported-workload comparison, not a distributed-cluster or production-capacity claim. JVM Spark supports operations outside this suite, including joins and grouped aggregates, which this Sparkless Connect release rejects. Large result collections and small jobs are both retained in the report.

## Reproduce

Use the environments described in DIVERSE_REPORT.md. Run run_shared_comparison.py with the pyspark-client/Sparkless interpreter, then summarize_shared.py. Adapt Spark installation and interpreter paths in shared_runtime.py and run_diverse.py. shared_runtime.py records the exact JVM launch flags. Raw action samples, server/client logs, and JUnit XML are stored alongside this report.
