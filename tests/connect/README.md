# Spark Connect fixtures and integration checks

Run these commands from the repository root. The 4.20 checks use
`pyspark-client==4.0.5` and the matching Apache Spark 4.0.5 protocol.

| To check | Start here |
| --- | --- |
| Client wire shapes and the base Connect listener | [Protocol fixtures and base smoke](#protocol-fixtures-and-base-smoke) |
| DuckDB execution, writes, and restart behavior | [Phase 4.20 DuckDB execution smoke](#phase-420-duckdb-execution-smoke) |
| Result and schema parity against Apache Spark | [Phase 4.20 Spark result and schema comparison](#phase-420-spark-result-and-schema-comparison) |

See the [server support matrix](../../docs/SUPPORT_MATRIX.md) for the release
contract and the [Spark Connect guide](../../docs/SPARK_CONNECT.md) for usage.

## Protocol fixtures and base smoke

The fixtures capture protobuf requests from public Python APIs and the client’s
reattachment and release encoder. The recording stub captures requests before
plan execution. The Connect transport builds from the
[vendored Apache Spark protocol sources](../../crates/sparkless-server-connect/proto/README.md).
The Rust
service tests decode the checked-in `execute-range-filter-count.pb` fixture and cover malformed
session IDs, duplicate operation IDs, session-generation/tombstone behavior, structural protobuf
limits, and a real gRPC listener's inbound-size rejection. Run them with:

```sh
cargo test -p sparkless-server-polars --no-default-features --features connect connect::tests --lib
```

For an end-to-end client check, start the development gRPC listener in one terminal:

```sh
cargo run -p sparkless-server --example connect-dev-server --no-default-features --features connect
```

In another terminal, create the locked client environment and run the smoke:

```sh
uv venv --python 3.11 /tmp/sparkless-connect-client-py311
uv pip sync --python /tmp/sparkless-connect-client-py311/bin/python tests/connect/client-py311.lock
/tmp/sparkless-connect-client-py311/bin/python tests/connect/smoke_public_client.py
```

The smoke checks the range/filter/count witness, scalar local relations and their Arrow physical
types, schema-only empty input, multi-batch offsets, and duplicate aliases through both
`collect()` and `toArrow()`. It is development evidence only; it does not complete V18/V20, the
reference comparison, or the response-byte/total-output failure boundaries.

With the same listener still running, test replay over a dropped gRPC connection:

```sh
/tmp/sparkless-connect-client-py311/bin/python tests/connect/smoke_lifecycle_public_client.py
```

This sends the standard Spark Connect range request over gRPC, closes the connection after the
first Arrow response, acknowledges that prefix from a new connection, reattaches after the
acknowledged response ID, and releases the completed operation. The in-process Rust tests cover
tag interruption, session-release cancellation, enriched error lookup, duplicate protection, and
capacity/accounting behavior; these focused checks do not pass V21/V22 as an integrated gate.
Stop the listener with Ctrl-C when finished.

## Phase 4.20 DuckDB execution smoke

This local integration exercise uses the pinned, unmodified `pyspark-client==4.0.5` client, a
native `sparkless-server-duckdb` binary, and `duckdb==1.5.1`. It checks the public SQL/DataFrame
subset, command result handling through `.collect()`, sequential reads and writes through the
standalone DuckDB client, and visibility after restarting Sparkless against the same database.
The local database is opened by one writable process at a time; this is not the shared Quack
owner concurrency or Spark-reference matrix required by V37/V38.

Build the local server and run the smoke from an environment with the DuckDB and Connect test
dependencies installed:

```sh
cargo build -p sparkless-server-duckdb --no-default-features --features http,connect
uv venv --python 3.11 /tmp/sparkless-phase420-client-py311
uv pip sync --python /tmp/sparkless-phase420-client-py311/bin/python \
  tests/connect/client-py311.lock tests/connect/duckdb-source-py311.lock
PYTHONPATH=python/server-duckdb:python/connect \
  /tmp/sparkless-phase420-client-py311/bin/python \
  tests/connect/smoke_phase420_duckdb_execution.py \
  --server-bin target/debug/sparkless-server-duckdb
```

The script creates a temporary database and removes it on exit. Its pass is local interoperability
evidence; it does not close the result/schema matrix, transaction fault injection, or shared-owner
gates.

### DuckDB backend shutdown cancellation

This local smoke starts a persistent Quack owner and the actual 4.20 DuckDB Connect backend,
starts a billion-row query, then requests normal Sparkless shutdown while the owner reports the
query active. It requires remote Quack cancellation to be accepted, the client stream to stop, no
active query to remain on the owner, and both processes to exit cleanly. It records its report at
`docs/plans/sparkless-server/evidence/phase-420-duckdb-backend-shutdown-local.json` by default.

```sh
cargo build --locked --release -p sparkless-server-duckdb --no-default-features \
  --features http,connect
cargo build --locked --release -p robin-sparkless-duckdb \
  --example duckdb_quack_fixture_owner
PYTHONPATH=python/server-duckdb:python/connect \
  /tmp/sparkless-phase420-client-py311/bin/python \
  tests/connect/smoke_phase420_duckdb_backend_shutdown.py \
  --server-bin target/release/sparkless-server-duckdb \
  --quack-owner-bin target/release/examples/duckdb_quack_fixture_owner \
  --quack-extension-path python/server-duckdb/build/lib.macosx-26.0-arm64-cpython-311/sparkless_server_duckdb/quack.duckdb_extension
```

The report is one-host source evidence. Release builds must rerun shutdown checks on each
supported server platform and exact package.

### Backend and launcher startup probes

The local 420-01 probe starts both server products in default mode, starts the DuckDB product
with a local DuckDB execution owner, fetches live capabilities, and verifies that they match the
readiness record. It also requires the base product, missing-owner, and missing-Connect cases to
fail before readiness. Build the base product and use the 4.20 DuckDB server binary:

```sh
cargo build --locked -p sparkless-server --no-default-features --features http,connect
PYTHONPATH=python/server-duckdb:python/connect \
  /tmp/sparkless-phase420-client-py311/bin/python \
  tests/connect/smoke_phase420_backend_selection.py \
  --base-server-bin target/debug/sparkless-server \
  --duckdb-server-bin target/debug/sparkless-server-duckdb
```

The script writes its local report to `docs/plans/sparkless-server/evidence/` by default. These
source-binary checks are not release-package installation evidence.

### DuckDB execution with a shared Quack owner

This probe starts the local Quack fixture as a separate owner process backed by a temporary DuckDB
file and runs the DuckDB execution backend through the unmodified Connect client. It checks an
independent Quack write appearing in Connect, a Connect write appearing in a second independent
Quack client, a dropped write response followed by Connect reattachment, and recovery after both
listener stop/start and a hard owner-process kill/restart at the same URI. The process-restart case
confirms persisted rows survive, an existing query fails while the owner is down, and a new Connect
write is visible through an independent client after recovery. A second lost response is followed
by hard restarts of both Quack and Sparkless; the probe checks the committed row and exact receipt
fingerprint before resending the unchanged request to the new server instance, then verifies there
is no duplicate row or receipt. It also kills the Quack owner after the first batch of a 1M-row
reattachable result stream: the result completes, HTTP health remains ready, and committed rows are
visible from an independent client after owner restart. The first existing-session query after
restart reports `Invalid connection id`; a retry in the same session succeeds. It also interrupts a
long Quack scan through the public Connect client and verifies a new query succeeds afterward. The report records executable, extension,
runner, request, and receipt evidence in
`docs/plans/sparkless-server/evidence/phase-420-quack-execution-local.json`. It does not inject a
fault at the transaction commit boundary. The smoke also verifies nested LIST, NaN/infinite DOUBLE,
and infinite DATE/TIMESTAMP values fail with server-side type-boundary errors before Arrow encoding.

```sh
cargo build --locked -p robin-sparkless-duckdb --example duckdb_quack_fixture_owner
PYTHONPATH=python/server-duckdb:python/connect \
  /tmp/sparkless-phase420-client-py311/bin/python \
  tests/connect/smoke_phase420_quack_execution.py \
  --server-bin target/debug/sparkless-server-duckdb \
  --quack-owner-bin target/debug/examples/duckdb_quack_fixture_owner \
  --quack-extension-path /path/to/quack.duckdb_extension
```

Use the Quack extension matching the locked DuckDB build. The smoke uses the fixture's local
authentication token.

### DuckDB committed-write reattachment

This fault-injection smoke creates a DuckDB table, submits an `INSERT` through the native Connect
service, drops the response stream after its first response, reattaches with the same operation ID,
and verifies the row through the standalone DuckDB client. A second dropped write response is
followed by a forced server-process kill; the smoke confirms the row and matching receipt are
durable before retry, restarts with a new server instance ID, and resends the exact request to
verify it is replayed once. The report records binary, runner, request, and receipt fingerprints.
A separate owner unit test simulates a lost acknowledgement after successful COMMIT. These
local-owner checks do not certify Quack crash recovery or determine durable state after a partial
storage failure.

```sh
PYTHONPATH=python/server-duckdb:python/connect \
  /tmp/sparkless-phase420-client-py311/bin/python \
  tests/connect/smoke_phase420_duckdb_write_reattach.py \
  --server-bin target/debug/sparkless-server-duckdb \
  --report docs/plans/sparkless-server/evidence/phase-420-duckdb-write-reattach-local.json
```

### DuckDB storage failure during COMMIT

The ignored `storage_write_failure_during_commit_returns_unknown` unit test writes a 32 MiB
high-entropy transaction value to a fresh temporary DuckDB database while the process file-size
limit is 4 MiB. DuckDB receives `File too large` while writing its WAL at COMMIT, and Sparkless
returns an unknown-outcome error. In the observed macOS run, reopening found zero rows and zero
receipts, with no partial payload. This records the outcome of this fault injection only. Compile
the test binary before applying the file-size limit; otherwise Cargo's incremental build may hit
the injected fault first:

```sh
cargo test --locked -p robin-sparkless-duckdb --no-run
trap '' XFSZ
ulimit -f 8192
cargo test --locked -p robin-sparkless-duckdb \
  execution::tests::storage_write_failure_during_commit_returns_unknown \
  -- --exact --ignored --nocapture
```

This local POSIX check covers error classification at COMMIT. It does not establish which data or
receipt effects survive every possible partial WAL write, nor replace checks on other supported
platforms. See the [local fault report](../../docs/plans/sparkless-server/evidence/phase-420-duckdb-storage-commit-fault-local.json).

## Phase 4.20 Spark result and schema comparison

Use the pinned Apache Spark 4.0.5 distribution and the ordinary
`pyspark-client==4.0.5` dependency environment from the smoke instructions. This runner sends the
same public DataFrame and SQL workloads to Sparkless DuckDB and Apache Spark, compares full
logical schemas and positional row multisets, and requires sanitized DuckDB engine witness events
for successful reads and committed writes. Its 44 workloads cover positive and negative ranges,
range partitions, empty and multi-batch results, nullable local relations, typed literals, metadata
aliases, scalar and boolean operators, count/sum including empty aggregates and `count("*")`,
inner joins, SQL cross joins, derived relations, SQL expressions and writes, scalar/decimal/integer/
date/timestamp/binary boundaries (including signed extrema), database-qualified table reads, and a
table created by a standalone DuckDB client. It compares the complete fixed 11-key SQL
configuration profile with Spark and exercises Get, GetWithDefault, GetAll, IsModifiable, Set, and
Unset through the public client.

The runner checks ANSI BIGINT overflow through a supported projection and verifies 20 explicit
rejection boundaries, including outer joins, distinct, SQL casts, aggregates outside the admitted
shape, CTEs, ordering/offsets, table functions, parameterized relations, CTAS, INSERT SELECT,
unsupported result types, UPDATE, and multiple statements. Rejected writes must leave existing data
unchanged. Spark 4.0.5 returns NaN for the non-finite DOUBLE fixture; Sparkless preserves its
finite-DOUBLE boundary, and the report records this intentional semantic difference.

```sh
PYTHONPATH=python/server-duckdb:python/connect \
  /tmp/sparkless-phase420-client-py311/bin/python \
  tests/connect/differential_phase420_duckdb.py \
  --spark-home /tmp/sparkless-oracle \
  --spark-archive /tmp/spark-4.0.5-bin-hadoop3-connect.tgz \
  --server-bin target/debug/sparkless-server-duckdb \
  --output target/connect-phase420/differential.json
```

The report records client/reference/runtime versions, platform, pinned archive and candidate
binary/runner hashes, settings, matched schemas/results, rejection cases, and DuckDB query/write
witness counts. The current local run records 44 result/schema comparisons, 20 rejected forms, 51
query witnesses, and six committed-write witnesses. This closes the source-level V37 matrix on the
recorded macOS arm64 environment; exact release artifacts and other supported platforms remain
release gates. The test starts Spark in a temporary warehouse and stops its process group on both
pass and failure. The expanded local report is retained at
`docs/plans/sparkless-server/evidence/phase-420-duckdb-connect-differential-expanded-local.json.gz`.
The separate Quack smoke covers a pre-analyzed catalog change, owner/listener restart, cancellation,
lost-response reattachment, changed-content conflict under a durable identity, and unchanged replay
after server and owner restarts. A paced 250K-row reattachable stream must return all rows or a
Quack/connection error before its 60-second RPC deadline; the V38 load matrix separately requires
paced 1M-row streams. After
the owner returns, one query attempt in the existing Connect session fails with `Invalid connection
id` and the next same-session retry succeeds. It does not close the full V38 resource, saturation,
release, or arbitrary partial-I/O recovery gates. The updated 250K-row local owner-loss report is
retained at
[`phase-420-quack-owner-loss-250k-local.json`](../../docs/plans/sparkless-server/evidence/phase-420-quack-owner-loss-250k-local.json).

## Slice 6 integrated conformance

The integrated runner builds and launches a native release executable with HTTP and Connect on
one shared server instance. It uses the locked, unmodified `pyspark-client` process with its
default retry settings, injects one `UNAVAILABLE` after the first real Arrow response, and checks
the stock client's `ReattachExecute` path against a sanitized native producer-start trace. It
also checks the normal enriched-error path, resource failures, four synchronized client
processes, HTTP statements, health progress, and graceful native shutdown. The native process
inherits no `JAVA_HOME`, `SPARK_HOME`, Python path, or PySpark process overrides; the runner
checks that it has no Python/JVM child process.
The conformance server uses a 300-second response deadline for the deliberately large total-output
limit case; client retry settings remain at their normal defaults. Its near-limit scalar workload
also records Arrow response sizes and verifies accepted responses stay below 1 MiB.

Run the integrated native and multi-client checks with:

```sh
python3 tests/connect/run_slice6_conformance.py \
  --client-python /tmp/sparkless-connect-client-py311/bin/python
```

For the matched Spark 4.0.5 reference comparison, verify and extract the Apache archive as
described above, then use the separate oracle environment:

```sh
mkdir -p /tmp/sparkless-oracle
tar -xzf /tmp/spark-4.0.5-bin-hadoop3-connect.tgz -C /tmp/sparkless-oracle --strip-components=1
uv venv /tmp/sparkless-connect-oracle-py311 --python 3.11
uv pip sync --python /tmp/sparkless-connect-oracle-py311/bin/python tests/connect/oracle-py311.lock
python3 tests/connect/run_slice6_conformance.py \
  --client-python /tmp/sparkless-connect-client-py311/bin/python \
  --oracle-python /tmp/sparkless-connect-oracle-py311/bin/python \
  --oracle-home /tmp/sparkless-oracle
```

The runner starts the reference with Apache's `start-connect-server.sh --wait` in an isolated
process group, using the frozen UTC/ANSI/case profile. Shutdown signals the whole group and waits
for the launcher and JVM to exit, including during failure cleanup. It compares client-visible
values, DataFrame schemas, Arrow schemas, empty results, duplicate labels, negative ranges,
checked expressions, and global counts. Evidence is written under `target/connect-slice6/`;
`summary.json` says whether reference comparison and concurrent-client checks passed or were
skipped. `SPARKLESS_CONNECT_TRACE=1` emits RPC method names, monotonic native producer start
numbers, and Arrow envelope byte/row/offset metadata, without plans, session IDs, or result data.

Use `--reference-url sc://host:port` to compare against an existing Spark 4.0.5 server. If the
Oracle interpreter is unavailable, the runner uses the validated standalone client for that
comparison. It leaves the externally managed server running.

## Slice 8 benchmark and proxy route

The optimized combined-server benchmark starts a fresh native process and measures correctness,
cold startup, filtered counts, multi-batch Arrow collection, p50/p95 latency, client/server RSS,
server CPU time, slow-reader behavior, health and HTTP progress, replay after a mid-stream drop, and
graceful shutdown. It synchronizes independent JVM-free public clients at 1, 2, 4, and 8 processes;
client warmups are excluded from timed cohorts. The default 1,000,000-row count and five iterations
are intentionally fixed for comparable local runs. Supply the pinned standalone-client interpreter:

```sh
python3 tests/connect/run_slice8_benchmarks.py \
  --client-python /tmp/sparkless-connect-client-py311/bin/python \
  --build --concurrency 1,2,4,8 --iterations 5 --rows 1000000
```

Each run writes `benchmark.json`, native trace, and fault-proxy logs under
`target/connect-slice8/run-*/`. The report records machine/software versions, source revision and
dirty-source digest, server binary SHA-256, raw invocation, client/server CPU and RSS measurements,
and cleanup outcome. Measurements are evidence only until numeric regression budgets are selected;
this runner does not claim a speedup or close formal V26 on its own.

For 4.19 scheduler comparisons, `--operation-history-records` and
`--operation-history-bytes` set the server's finite completed-operation history budget. Both
default to the production values (1,024 records and 4 MiB); use the same explicit values for the
released baseline and candidate when measuring a long cohort that exceeds the legacy history cap.
The report records these limits. Overrides apply only to the benchmark server process and do not
change production defaults.

For a process-wide Rust allocation witness, build the server with the opt-in
`allocator-trace` Cargo feature. Its control shutdown record reports live bytes, peak live bytes,
and cumulative allocated bytes through Rust's global allocator. Direct native C/C++ allocations
are excluded; pair these counters with the existing process RSS sampling and DuckDB connection
memory limit when interpreting source runs.

The V25 route runner is separate and requires a reachable authenticated emulator route plus its
immutable source revision. Set `SPARKLESS_PROXY_TOKEN` through the environment or secret manager; the
value is never written into the report or process arguments:

```sh
SPARKLESS_PROXY_URL=sc://proxy.example:15002 \
SPARKLESS_PROXY_REVISION=<40-to-64-character-source-commit> \
SPARKLESS_PROXY_TOKEN="$SPARKLESS_PROXY_TOKEN" \
python3 tests/connect/run_slice8_proxy_route.py \
  --client-python /tmp/sparkless-connect-client-py311/bin/python
```

The route witness checks invalid-token rejection, independent public sessions, stable backend session
affinity, stale-identity rejection, a multi-batch Arrow result, config RPC status and returned
metadata/trailers, session release isolation, interruption of a live result, enriched client errors,
and default-client reattachment after a deliberately injected disconnect. It reports but cannot
infer proxy-specific cluster target selection or shut down an externally owned proxy. An unavailable
route or missing pinned revision fails explicitly; standalone tests do not count as proxy evidence.

The interpreter-specific dependency snapshots are hash-locked for CPython 3.11:

- `client-requirements.in` and `client-py311.lock` install only the standalone client package.
- `duckdb-source-requirements.in` and `duckdb-source-py311.lock` install the standalone DuckDB fixture writer used by the 4.19 packaged-source smoke.
- `oracle-requirements.in` and `oracle-py311.lock` install the matching Python reference client and Connect dependencies (`pyspark[connect]==4.0.5`). Both locks constrain pandas to `>=2,<3`, matching the [Spark 4.0.5 documented Connect dependency range](https://spark.apache.org/docs/4.0.5/api/python/getting_started/install.html#spark-connect). The reference server is a separate Apache binary distribution: [`spark-4.0.5-bin-hadoop3-connect.tgz`](https://downloads.apache.org/spark/spark-4.0.5/spark-4.0.5-bin-hadoop3-connect.tgz), SHA-512 `4cc2e43959b5e0a67b7d895e1aa1fb3ab05fa6ad4a5912de76236bb9b70f9557b20837b05ee63474798e927130e72ed6d1fadcd273a71e65c70eac18771c0d6b`. Verify against the [Apache checksum](https://downloads.apache.org/spark/spark-4.0.5/spark-4.0.5-bin-hadoop3-connect.tgz.sha512) and signature, then run it with Java 17. Keep the reference server/client outside the request-fixture environment.

The reference starts with Apache's `sbin/start-connect-server.sh`, with UTC timezone, ANSI enabled, case sensitivity disabled, and `spark.sql.execution.arrow.useLargeVarTypes=false`. The client's local-relation cache threshold remains at its real 64 MiB default. The matching Python distribution is pinned in `oracle-py311.lock`; this is the client-side dependency lock, not the server archive checksum.

`benchmark_phase419_duckdb_source.py` creates a standalone DuckDB fixture with distinct, hash-derived string values and measures a warm public-client action through the packaged server. It records source and logical data sizes, the server binary hash, per-iteration latency, and the managed server child's peak RSS. Run it from the locked `duckdb-source-py311.lock` environment. `--expect-rejection ipc` and `--expect-rejection rows` turn the respective statement-cap failures into recorded passing outcomes; `--slow-reader-delay-ms` pauses public `toLocalIterator()` consumption after each row group to measure a bounded slow-reader case. Combine that option with `--expect-rejection result` to require the 8 MiB per-operation Connect replay/sender rejection, or `--allow-result-rejection` to retain both completed scans and cap rejections per reader.

`benchmark_phase419_exports.py --small-jobs-only --startup-samples 20` measures the 10/100/1,000/10,000-row Python-tuple ingest/filter/count cases, a nullable 25-row preview, nullable/mixed collection, and public schema analysis. It validates each result, records 20 warm iterations by default, and measures twenty fresh server/readiness/session starts in the same client runtime. Run it for three fresh runtimes per executable when collecting the paired small-job guardrails; `--iterations` changes the warm action sample count and `--startup-samples` changes the fresh-start sample count.

For the 4.20 scheduler comparison, run the 4.19 profile with both binaries. The runner starts a fresh server for each side, alternates which binary runs first, and holds a local lock across all pairs. Other local benchmark entry points use the same lock. Choose the throughput floor before running the comparison. The report compares the median paired throughput ratio with that floor and flags short-job p50 or p95 regressions that exceed the phase budget in a majority of pairs.

Each cohort runs for at least 60 seconds and probes health and HTTP SQL progress. The additional Connect reader consumes raw Arrow batches at a controlled pace and validates each row and batch offset. It defaults to reattachable execution. Use `--non-reattachable-slow-reader` to measure backpressure on a non-reattachable stream. The report records the reader's first-batch latency and matches its operation ID to the server trace for blocked send and compute readmission. Correct operations per second use the interval from barrier release to the last workload completion, excluding client report collection and the separate slow-reader join.

For a local Phase 4.20 DuckDB V38 capacity run, pass the DuckDB server, a fresh local execution owner, and `--allow-capacity-rejections`. DuckDB mode disables HTTP SQL, so this path checks health during the Connect workload and records no HTTP statement samples. The flag retains only explicit compute-admission or response-stall timeouts; data mismatches and other failures still stop the run. A report with `result: capacity_rejections_observed` is diagnostic evidence and does not pass the V38 gate. The local owner's memory is part of the server process RSS. Repeat with `--non-reattachable-slow-reader` to capture the other reader mode.

```sh
PYTHONPATH=python/server-duckdb:python/connect \
  /tmp/sparkless-phase420-client-py311/bin/python \
  tests/connect/run_slice8_benchmarks.py \
  --client-python /tmp/sparkless-phase420-client-py311/bin/python \
  --server-bin target/debug/sparkless-server-duckdb \
  --duckdb-local-owner \
  --allow-capacity-rejections \
  --profile 419 \
  --concurrency 1,4,8,16 \
  --duration-seconds 60 \
  --active-operations 8 \
  --queue-timeout-ms 5000 \
  --rows 1000000 \
  --collect-rows 100000 \
  --slow-reader-rows 1000000 \
  --output target/connect-phase420-v38
```

The [original V38 matrix](../../docs/plans/sparkless-server/evidence/phase-420-duckdb-v38-load-local-summary.json) is pre-fix evidence: its 5-second network send-stall deadline rejected paced non-reattachable streams across active limits 2/4/8. The tested 4.20 build allows 30 seconds for network send-credit replenishment and retains the 5-second reattachable response-ACK timeout. Release-profile local-owner and shared-Quack cohorts at eight clients/active_operations=16 each completed concurrent and final 1M-row readers at 100 ms pacing, with zero capacity rejections, clean health probes, and graceful shutdown. At active_operations=8, eight workload clients plus the extra slow reader caused one compute-admission rejection; the final standalone slow reader still completed. See the [post-fix V38 summary](../../docs/plans/sparkless-server/evidence/phase-420-duckdb-v38-backpressure-release-local-summary.json) and linked raw reports. This closes the reproduced send-stall defect on the tested host. A separate [Quack lifecycle probe](../../docs/plans/sparkless-server/evidence/phase-420-quack-execution-final-release-local.json) now measures four concurrent Connect reads during owner loss; supported capacity, resource-budget, cross-platform, cleanup, and durability acceptance remain open.

The bounded DuckDB read pool addresses a second bottleneck found in the active-16 Quack cohort: short queries waited up to 27.75 seconds while a non-reattachable reader was blocked sending. With one serialized write/receipt connection, up to four concurrent read connections, and at most 16 queued reads, the final local and Quack cohorts completed the same paced streams without capacity rejections. Quack small-count p99 fell to 167 ms (99.4% below the pre-pool cohort) and throughput increased from 12.4 to 79.2 ops/s. The [read-pool summary](../../docs/plans/sparkless-server/evidence/phase-420-duckdb-v38-read-pool-local-summary.json) links final-binary reports and includes local RSS observations. These one-host measurements have no accepted resource budget and do not close cross-platform or release checks.

For the shared Quack owner, replace `--duckdb-local-owner` with `--quack-owner-bin target/debug/examples/duckdb_quack_fixture_owner --quack-extension-path /path/to/quack.duckdb_extension`. The runner starts and stops the fixture owner, samples owner RSS separately, and redacts the fixture token in the report. To repeat the reattachable matrix, rerun with `--active-operations 2`, `4`, and `8`; use `--non-reattachable-slow-reader` to exercise that reader mode. `--response-timeout-ms` controls the per-operation deadline; keep it long enough for deliberately paced full-result probes so it does not hide send-stall behavior. The [Quack lifecycle smoke](../../docs/plans/sparkless-server/evidence/phase-420-quack-execution-final-release-local.json) now covers mixed-load owner loss on one host; supported-platform, transaction, cleanup, and release gates remain open.

```sh
python tests/connect/run_slice8_benchmarks.py \
  --client-python /tmp/sparkless-phase420-client-py311/bin/python \
  --baseline-bin /path/to/published-4.19.0/sparkless-server \
  --server-bin /path/to/4.20.0/sparkless-server \
  --profile 419 \
  --concurrency 1,4,8,16 \
  --duration-seconds 60 \
  --active-operations 2 \
  --rows 1000000 \
  --collect-rows 100000 \
  --slow-reader-rows 1000000 \
  --non-reattachable-slow-reader \
  --slow-reader-pause-ms 100 \
  --min-throughput-ratio 0.95 \
  --output /tmp/phase420-scheduler-active2
```

Repeat at active limits 4 and 8, and repeat in reattachable mode by omitting `--non-reattachable-slow-reader`. Each run directory contains the raw samples and its server log. The parent `comparison.json` records pair order, artifact hashes, and the comparisons.

To repeat the saturated 16-client reattachable cohort at each active limit, run three fresh paired comparisons. Omitting `--non-reattachable-slow-reader` keeps the additional reader reattachable:

```sh
for active in 2 4 8; do
  python tests/connect/run_slice8_benchmarks.py \
    --client-python /tmp/sparkless-phase420-client-py311/bin/python \
    --baseline-bin /path/to/published-4.19.0/sparkless-server \
    --server-bin /path/to/4.20.0/sparkless-server \
    --profile 419 \
    --concurrency 16 \
    --duration-seconds 60 \
    --active-operations "$active" \
    --rows 1000000 \
    --collect-rows 100000 \
    --slow-reader-rows 1000000 \
    --slow-reader-pause-ms 100 \
    --min-throughput-ratio 0.95 \
    --output "/tmp/phase420-reattachable-active${active}"
done
```

The HTTP runner starts fresh baseline and candidate servers in three alternating pairs. It measures a small SELECT, a 32-column nullable and mixed-type projection, and an approximately 8 MB JSON response. It checks the returned field names, values and byte lengths, confirms an over-limit response is rejected, and records health and SQL progress during the large reads.

```sh
python tests/connect/benchmark_phase420_http.py \
  --baseline-bin /path/to/published-4.19.0/sparkless-server \
  --server-bin /path/to/4.20.0/sparkless-server \
  --output /tmp/phase420-http
```

`benchmark_phase419_xdist.py` runs a 38-case pytest suite with four real xdist workers against one shared server per engine. Every worker creates its own Connect session. Its four job shapes cover the four-job mix described in #1738; the original case list behind the issue's earlier 38-test timing is not retained, so these timings do not reproduce that historical suite. Results, per-test durations, suite timings, rotations, and artifact hashes are retained in the JSON report. It requires a CPython 3.11 environment with `sparkless-connect`, `pyspark-client==4.0.5`, PyArrow 26.0.0, pytest, and pytest-xdist 3.8.0; `--spark-home` points to the extracted `deps` directory from the PySpark 4.0.5 source archive. Select `--product base` or `--product duckdb` to match the supplied server wheel and executable.

```sh
python tests/connect/benchmark_phase419_xdist.py \
  --server-binary /path/to/sparkless-server \
  --server-wheel /path/to/sparkless_server-4.19.0-py3-none-manylinux_2_28_x86_64.whl \
  --product base \
  --spark-archive /path/to/pyspark-4.0.5.tar.gz \
  --spark-home /path/to/pyspark-4.0.5-deps \
  --output /tmp/phase419-four-worker-pytest.json
```

`differential_phase419_duckdb.py` compares the declared source subset with local Apache Spark 4.0.5 and a packaged Sparkless 4.19 server. Its fixtures cover table schemas/rows, nullable comparisons, filter and projection, empty results/counts/aggregates/join sides, all-null grouped counts and sums, same-owner joins with duplicate/null keys, date/timestamp and decimal boundaries, and a schema change between analysis and execution. Use a CPython 3.11 environment containing `pyspark[connect]==4.0.5`, `duckdb==1.5.1`, the candidate `sparkless==4.19.0` API/native packages, and the corresponding server binary:

```sh
python tests/connect/differential_phase419_duckdb.py \
  --server-bin /path/to/sparkless-server \
  --output /tmp/phase419-duckdb-spark-differential.json
```

The script temporarily removes inherited `SPARK_HOME` so PySpark starts its bundled Spark 4.0.5 local runtime, then restores it. The retained candidate report records the exact API/server versions, executable hash, runtime versions, schemas, and rows.

The source benchmark accepts `--concurrent-readers 1..4` to run independent public clients against one server and one source alias. By default it sets the server active-operation limit to match that cohort; `--active-operations` can set a lower admission limit, and `--queue-timeout-ms` adjusts the queue deadline for readers waiting behind it. Action samples are per client, and `concurrent_batch_samples_ms` records each simultaneous round. Pair this with `--slow-reader-delay-ms` to measure concurrent source readers and replay pressure. The 4.19 release workflow runs four-reader, 100k- and 350k-row, 16-byte-payload slow-consumer cases against each packaged Linux x86_64 and macOS arm64 server extra, then retains the raw reports as a workflow artifact.

`smoke_phase419_duckdb_shutdown.py` launches the Quack owner and packaged server as separate processes, begins a billion-row public source scan, then sends the normal graceful server shutdown command. It uses the pinned client's non-reattachable, zero-retry mode because the test intentionally stops the server; it requires the Connect action to receive the shutdown status and the server trace to confirm that Quack accepted the remote query cancellation. Build the source-enabled candidate and owner fixture, then run:

```sh
cargo build --locked --release -p sparkless-server-duckdb --no-default-features \
  --features http,connect
cargo build --locked --release -p robin-sparkless-duckdb \
  --example duckdb_quack_fixture_owner
python tests/connect/smoke_phase419_duckdb_shutdown.py \
  --server-bin target/release/sparkless-server-duckdb \
  --quack-owner-bin target/release/examples/duckdb_quack_fixture_owner \
  --quack-extension-path /path/to/duckdb-2.0.0/quack.duckdb_extension
```

Recreate and verify the client fixtures in an isolated environment:

```sh
uv venv /tmp/sparkless-connect-client-py311 --python 3.11
uv pip sync --python /tmp/sparkless-connect-client-py311/bin/python tests/connect/client-py311.lock
/tmp/sparkless-connect-client-py311/bin/python tests/connect/capture_public_client_fixtures.py --check
```

Regenerate a dependency snapshot after an intentional pin change with:

```sh
uv pip compile --python-version 3.11 --generate-hashes tests/connect/client-requirements.in -o tests/connect/client-py311.lock
uv pip compile --python-version 3.11 --generate-hashes tests/connect/oracle-requirements.in -o tests/connect/oracle-py311.lock
```

Each fixture has a readable `.textproto` and deterministic protobuf `.pb` form. `manifest.json` records the RPC/type and SHA-256 for both encodings. The recorder fixes user/session/operation identities, resets plan IDs, and clears the client runtime string so OS and Python patch version do not create irrelevant diffs. It verifies that parsing either fixture produces the same protobuf bytes.

The captured public paths cover range/filter/count, Column comparisons/arithmetic/aliasing, projection/limit, `take`, `head`, `toLocalIterator`, normal and empty local relations, DDL schema parsing, schema analysis, SQL command encoding, the Python `RuntimeConfig` operations (`Get`, `GetWithDefault`, `GetAll`, `IsModifiable`, `Set`, and `Unset`), and the client's ExecutePlan reattach, prefix/all release, interrupt, and session-release messages. The protobuf-defined `GetOption` branch is captured separately through the client's low-level Config request builder; the pinned Python `RuntimeConfig` has no public method that emits it. The cached-local-relation boundary fixture forces the threshold to one byte and substitutes a stable artifact ID at the upload seam; it proves only that the real API then emits `CachedLocalRelation`. It does not capture `AddArtifacts` or imply artifact support. Spark 4.0.5's actual `spark.sql.session.localRelationCacheThreshold` default is 67,108,864 bytes (64 MiB); normal client defaults are left intact in the other captures.

The fixture harness supplies Config responses only to let the public client construct requests. In particular, a successful client-side `set` fixture is not permission for Sparkless to accept unsupported settings. Implementations must preserve optional/default lookup semantics and reject unsupported changes explicitly.
