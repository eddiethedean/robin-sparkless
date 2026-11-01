---
search:
  exclude: true
---

# Phase 4.19 implementation evidence

This is an interim record for the [4.19 release plan](RELEASE_4_19.md), not a release acceptance report. The implementation branch is `codex/release-4.19`. At the time of the measured runs, `HEAD` was `1f43130c0f96c9641442953fbb941478069922d9`, the working-tree source digest was `c5174677078c1fe0683c6424cdb268d77eb0387c37aa4889e24b73f1c75b6451`, and the Connect server binary SHA-256 was `e6c41781d633629904bc2966e35e0fc9487944d8b2522b1fa78b6a5cb8726278` (reported version `4.18.0`). The source digest covered the then-dirty source tree and `output/benchmarks` paths; those untracked benchmark workspace files are not part of this implementation change.

## Correctness and build checks

The following commands passed on the current implementation, after the Connect logical-to-physical alias-resolution fix:

```sh
CARGO_HOME=/tmp/sparkless-cargo-home cargo build --locked --release -p sparkless-server --features connect
CARGO_HOME=/tmp/sparkless-cargo-home cargo test --locked --release -p sparkless-server --features connect --lib
cargo test --locked --release -p sparkless-server --features connect connect::tests::release_all_clears_history_for_nonreattachable_operations --lib
cargo test --locked --release -p sparkless-server --features connect connect::plan::tests:: --lib
PYTHONPATH=python/sparkless /tmp/sparkless-linkedin-verify/bin/python -m pytest -q tests/server/test_launcher.py
PYTHONPATH=python/sparkless SPARKLESS_MAX_ACTIVE_OPERATIONS=4 SPARKLESS_MAX_RANGE_ROWS=10000000 /tmp/sparkless-linkedin-verify/bin/python tests/connect/smoke_phase419_client.py --server-bin target/release/sparkless-server
```

Results: release build passed; server library suite passed **85 tests**; focused ReleaseAll regression passed; Connect plan suite passed **19 tests**; the expanded launcher suite passed **17 tests**; and the public PySpark client smoke passed helper lifecycle, range/count, table join, and grouped aggregate checks. The smoke also verifies 1k/10k empty range results, an at-cap 10k-row local relation, variable-width strings including a 300k value, LIMIT, and explicit local row/scalar/IPC-size errors. The launcher coverage includes missing optional-client detection before server construction, startup/session-creation failures, cleanup retries, preservation of the primary/body errors, fork protection, separate nested helpers, sequential reuse, and an unrelated active session. A Connect-only release build after marking the test-only relation wrapper `#[cfg(test)]` passed without warnings. `cargo fmt --all -- --check` and `git diff --check` passed. These checks cover implemented behavior only; DuckDB-source-specific evidence is recorded below.

The expanded public smoke was rerun against the Connect-only server binary used for the active-8 profile (SHA-256 `92e8b76afc0547c572ba2b28316891b6c06da93a1fc3534f9766937d4dc44bb7`):

```sh
PYTHONPATH=python/sparkless /tmp/sparkless-linkedin-verify/bin/python \
  tests/connect/smoke_phase419_client.py \
  --server-bin /tmp/sparkless-phase419-candidate-target/release/sparkless-server
```

It passed the empty-result, 10,000-row local-relation, wide/variable-width string, LIMIT, oversized row/scalar/IPC, range-count, join, and aggregate checks.

## DuckDB source candidate (standalone and Quack, 2026-10-31)

The branch now contains an opt-in `duckdb-source` integration using `duckdb-rs==1.20000.1`
(bundled DuckDB 2.0.0) and Arrow IPC 60.1.0. The configured catalog accepts only trusted aliases
and either canonical local database paths or an explicit Quack endpoint/token/extension path. It
loads the exact configured Quack extension file without runtime downloads, exposes schema metadata
without materializing table rows, pins one read-only connection/transaction per execution snapshot,
and rejects reads from a second database owner in the same statement. The Polars integration
consumes IPC batches synchronously and materializes source relations before evaluating the Connect
plan.

The candidate enforces 16,384 rows and 1 MiB IPC bytes per batch; 1,000,000 source rows, 32 MiB
IPC bytes, and 64 MiB estimated decoded Polars data across one statement; and 512 KiB per VARCHAR
value. DuckDB is configured for one thread and a 256 MiB per-connection memory limit. A 5 ms
cancellation watcher interrupts the local DuckDB client and calls Quack's `quack_cancel` on the
shared owner through a short-lived control connection with a 3-second HTTP timeout. The catalog
reserves the scan and control slots together under its four-connection cap, and queues at most
sixteen waiting operations per catalog. These are
configured/enforced limits, not measured process-RSS guarantees; remote owner resource usage is
outside the local connection limit, and increasing-size/slow-consumer budgets are not measured.

The following compile checks passed after the source and Connect wiring changes:

```sh
CARGO_HOME=/tmp/sparkless-cargo-home cargo check --locked --release -p sparkless-server --features connect,duckdb-source
CARGO_HOME=/tmp/sparkless-cargo-home cargo check --locked --release -p sparkless-server --features connect
cargo fmt --all -- --check
git diff --check
```

The focused source fixture command passed five ordinary tests; the Quack fixture is ignored unless
an exact platform extension path is supplied:

```sh
CARGO_HOME=/tmp/sparkless-cargo-home cargo test --locked --release -p robin-sparkless-polars --features duckdb-source --test duckdb_source
```

An ignored integration fixture exercises an independent writer over Quack against the same owner.
It passed on the current `osx_arm64` host with the DuckDB 2.0.0 extension already installed:

```sh
SPARKLESS_TEST_QUACK_EXTENSION_PATH=/Users/odosmatthews/.duckdb/extensions/v2.0.0/osx_arm64/quack.duckdb_extension \
  CARGO_HOME=/tmp/sparkless-cargo-home cargo test --locked --release -p robin-sparkless-polars \
  --features duckdb-source --test duckdb_source \
  quack_shared_owner_pins_read_snapshot_and_observes_writer_commit_next_statement -- --ignored --nocapture
```

The fixture checks that the reader retains one row in its transaction after an independent Quack
writer commits a second row, then sees both rows in the next statement. It also cancels a scan of a
large remote view after its first batch and verifies that the shared owner records the query as
cancelled.
It uses independent DuckDB connections in one test process and loopback HTTP; clean-platform
extension installation, separate-process/service lifecycle, TLS, source-loss behavior, and wider
race/resource cases remain open.

Four source-pool unit tests passed with the crate's `sql` and `test-hooks` features enabled. They
verify the four-connection cap, weighted Quack scan/cancel reservations, queue-full rejection,
weighted waiter wakeups, and cancellation removal:

```sh
CARGO_HOME=/tmp/sparkless-cargo-home cargo test --locked --release -p robin-sparkless-polars \
  --features duckdb-source,sql,test-hooks --lib source_pool_tests -- --test-threads=1
```

This validates Arrow 60-to-Polars IPC interoperability for the fixture's BOOLEAN, SMALLINT,
INTEGER, BIGINT, finite DOUBLE, VARCHAR, DATE, TIMESTAMPTZ, DECIMAL, null and empty-table cases,
plus rejection of NaN and nested types, schema-only reads, config rejection, and snapshot owner
reuse. The public-client smoke also passed through the server and asserts the locked
`pyspark-client==4.0.5` version:

```sh
PYTHONPATH=python/sparkless:/tmp/sparkless-duckdb-python-only /tmp/sparkless-linkedin-verify/bin/python tests/connect/smoke_phase419_duckdb_client.py --server-bin target/release/sparkless-server
```

The same public-client fixture passed against the Quack owner and bundled native server:

```sh
CARGO_HOME=/tmp/sparkless-cargo-home cargo build --locked --release -p robin-sparkless-polars \
  --features duckdb-source --example duckdb_quack_fixture_owner
PYTHONPATH=python/sparkless:/tmp/sparkless-duckdb-python-only /tmp/sparkless-linkedin-verify/bin/python \
  tests/connect/smoke_phase419_duckdb_client.py \
  --server-bin target/release/sparkless-server \
  --quack-owner-bin target/release/examples/duckdb_quack_fixture_owner \
  --quack-extension-path /Users/odosmatthews/.duckdb/extensions/v2.0.0/osx_arm64/quack.duckdb_extension
```

It verifies exact values/schema, filter/project, UTC timestamp instant, `count(1)`, null-excluding
`count(column)` on populated/all-null/empty data, limit zero, empty-table schema, same-owner inner
join, grouped count/sum, unsupported source type/value errors, and rejected CREATE/INSERT/UPDATE/
DELETE/append/overwrite requests with an unchanged row count. The standalone fixture file was
created by Python DuckDB 1.5.1; the
Quack fixture separately uses the bundled DuckDB 2.0.0 owner.
Neither test replaces a Spark differential corpus. Broader disconnect/replay/source-loss acceptance,
the full source DDL/DML rejection matrix, clean platform packaging, and measured source/RSS budgets
remain open. D14 and V35/V36 are still open; the release does not claim complete DuckDB-source
acceptance.

## #1736 export and #1737 range-count measurements

The export comparison contains three fresh baseline/candidate runtime pairs and 15 samples per workload. Values are milliseconds; speedup is baseline median divided by candidate median.

| Workload | Baseline median / p95 | Candidate median / p95 | Median speedup |
| --- | ---: | ---: | ---: |
| Collect 1,000 range rows | 24.947 / 26.310 | 15.947 / 18.244 | 1.56× |
| Collect 10,000 range rows | 110.489 / 122.518 | 20.962 / 32.723 | 5.27× |
| Collect 100,000 range rows | 1,029.311 / 1,255.731 | 120.682 / 140.492 | 8.53× |
| Empty filter, 1,000 input rows | 43.236 / 51.344 | 15.532 / 20.014 | 2.78× |
| Empty filter, 10,000 input rows | 269.540 / 282.917 | 16.501 / 16.782 | 16.33× |

The 100,000-row result exceeds the initial 2× target. The report does not include the full #1736 required matrix (local relations, wide/variable-width data, LIMIT, oversized cells, allocation/buffer measurements). The exact invocation and immutable binary/package hashes for both sides were not retained in this JSON, so this is preliminary export evidence rather than a protocol-complete acceptance run. The compressed full report is [phase-419-export-smoke.json.gz](evidence/phase-419-export-smoke.json.gz), SHA-256 `9a551e66a977b5299a085cd869b5518ed4ef40be798522a20bbe00d5286a2587`.

### Paired public range-count measurements

The new `tests/connect/benchmark_phase419_counts.py` harness measured exact Connect `count()` results for string `id > 5` and Column `id >= 5` on 1k, 1M, and 10M ranges. Each fresh runtime warmed each case once, then collected 20 timed samples. Three baseline/candidate runtime pairs ran in rotating order (baseline/candidate, candidate/baseline, baseline/candidate) on the same Apple M4 Pro host with the locked `pyspark-client==4.0.5`/PyArrow 26.0.0 client. All six reports use clean runner revision `79527d182a91c00d4dee389e0eacec9e6e46e515`. The baseline server is v4.18.1 source `0ad225677ec47b20c9c6225fa9f4b73b00e54aba`, Connect-only binary SHA-256 `4bcccfb2d838e3dab57ee91f2f6ee5735fb9b8c03d13361b04dd792f68c19aae`; the candidate is Connect-only binary SHA-256 `92e8b76afc0547c572ba2b28316891b6c06da93a1fc3534f9766937d4dc44bb7`, built from the candidate runtime source at `a0edec2d`. The helper records the binary hash, exact values, raw timings, environment, limits, and source provenance in each report.

Each run used the same command shape, changing `SERVER_BIN` and `REPORT` for the six baseline/candidate artifacts:

```sh
PYTHONPATH=python/sparkless /tmp/sparkless-linkedin-verify/bin/python \
  tests/connect/benchmark_phase419_counts.py \
  --server-bin "$SERVER_BIN" --iterations 20 --output "$REPORT"
```

The following values pool the 60 warm samples per side and use nearest-rank p50/p95; speedup is baseline p50 divided by candidate p50. The 20/60-sample tails are descriptive and do not establish stable p99 behavior.

| Rows | Predicate | Baseline p50/p95 (ms) | Candidate p50/p95 (ms) | p50 speedup |
| ---: | --- | ---: | ---: | ---: |
| 1,000 | String `id > 5` | 15.513 / 16.732 | 15.508 / 17.175 | 1.00× |
| 1,000 | Column `id >= 5` | 16.548 / 18.773 | 15.972 / 18.015 | 1.04× |
| 1,000,000 | String `id > 5` | 20.327 / 22.319 | 15.686 / 17.317 | 1.30× |
| 1,000,000 | Column `id >= 5` | 20.324 / 21.318 | 15.465 / 17.790 | 1.31× |
| 10,000,000 | String `id > 5` | 46.924 / 48.487 | 15.323 / 17.326 | 3.06× |
| 10,000,000 | Column `id >= 5` | 46.794 / 48.838 | 16.062 / 17.898 | 2.91× |

Each baseline/candidate report contains 20 samples per case; artifact SHA-256 values are:

| Pair | Baseline report | SHA-256 | Candidate report | SHA-256 |
| ---: | --- | --- | --- | --- |
| 1 | [phase-419-counts-baseline-pair-1.json.gz](evidence/phase-419-counts-baseline-pair-1.json.gz) | `5540001952bec57e07af49e3442ac3177b4bf1abae0906f324d31792f74622a4` | [phase-419-counts-candidate-pair-1.json.gz](evidence/phase-419-counts-candidate-pair-1.json.gz) | `ae2ee6edc9793ca40f7a033729a620c3b4a772682f496f1ea50a363cbff2108f` |
| 2 | [phase-419-counts-baseline-pair-2.json.gz](evidence/phase-419-counts-baseline-pair-2.json.gz) | `fd5e84f4dd6562d1bc3ff9438cc88317b7c92d3a519fe23c76c3735e2c1d43ac` | [phase-419-counts-candidate-pair-2.json.gz](evidence/phase-419-counts-candidate-pair-2.json.gz) | `b624c91da9a18f51836f806d38f0bff3fc23c31bd6b32eb3b3ff401e87cd1e49` |
| 3 | [phase-419-counts-baseline-pair-3.json.gz](evidence/phase-419-counts-baseline-pair-3.json.gz) | `95b6ca4da7ad8800324ae90897feac5379e00ae7edb97afd9cbc9ad763b672c5` | [phase-419-counts-candidate-pair-3.json.gz](evidence/phase-419-counts-candidate-pair-3.json.gz) | `9756099150e3f4fb1eac34de851d24005120681d008cd383a5ea2d109aff14bc` |

The measured 10M p50 results exceed #1737's initial 2× latency target for both public predicate forms. Native correctness coverage checks all six integer comparison operators in both operand orders against materialized results for positive, negative, empty, and near-`i64`-boundary ranges. It also verifies fractional, compound-expression, and out-of-range `u64` fallback plus cancellation on the fallback count path. The command `CARGO_HOME=/tmp/sparkless-cargo-home cargo test --locked --release -p robin-sparkless-polars --features sql --lib` passed 402 tests with two ignored. Public performance measurements cover string `>` and Column `>=`; wider public-client/oracle matrices and stable tail measurements remain open.

## #1738 mixed Connect load

The retained runner reports each use a fresh server, one warmup per client, four 60-second cohorts at 1/4/8/16 public clients, a 1,000,000-row count and 100,000-row collection mix, active-operation limit 2/4/8, concurrent 1,000,000-row slow reader, health/HTTP progress probes, replay fault injection, and native phase traces. Platform: Apple M4 Pro, 12 logical CPUs, 24 GiB, macOS 26.5.2 arm64; Rust 1.93.1, Python 3.11.15, `pyspark-client==4.0.5`, PyArrow 26.0.0; UTC. Each table cell reports throughput (correct ops/s), short-job p95/p99 (ms), admission-wait p95 (ms), blocked-send p95 (ms), and server peak RSS (MiB).

| Clients | Active 2 | Active 4 | Active 8 |
| ---: | --- | --- | --- |
| 1 | 21.46; 19.6/20.0; 0.0; 15.0; 54.1 | 21.55; 19.6/20.1; 0.0; 15.0; 52.2 | 21.25; 19.4/19.8; 0.0; 15.0; 52.9 |
| 4 | 71.91; 46.5/59.1; 30.1; 15.0; 55.2 | 80.74; 19.7/20.3; 0.0; 15.0; 56.0 | 74.64; 21.1/27.3; 0.0; 15.0; 59.8 |
| 8 | 101.40; 101.7/114.2; 84.1; 15.0; 57.3 | 147.69; 23.7/30.1; 7.5; 15.0; 59.3 | 125.06; 25.4/41.8; 0.0; 15.1; 44.8 |
| 16 | 105.67; 216.9/232.4; 201.3; 15.0; 57.7 | 188.18; 58.9/70.2; 39.5; 15.4; 62.8 | 186.75; 28.1/40.3; 5.0; 15.2; 50.9 |

All 12 cohorts recorded zero health/HTTP probe errors. The slow reader completed all rows; disconnected-reader replay reported one native producer start and one reattach RPC; the cancellation workload interrupted a live operation. The reports include raw per-operation samples, trace counters, client/server CPU and RSS samples, shutdown status, and fault-proxy logs. The complete compressed reports and hashes are:

| Active limit | Run ID | SHA-256 |
| ---: | --- | --- |
| 2 | [run-20261031T233604Z-20705](evidence/phase-419-connect-mixed-active-2.json.gz) | `e654fd153616eff5b63d9c77b977a6727cfd91c2c80841fc0a0420ed504c1a01` |
| 4 | [run-20261031T234038Z-23552](evidence/phase-419-connect-mixed-active-4.json.gz) | `ca65970cd6d36befb5a487464e04fc4735ce363479bea65960d488b6fc965f3e` |
| 8 | [run-20261031T234454Z-26030](evidence/phase-419-connect-mixed-active-8.json.gz) | `5438824f901402e6b60dfcd859ccff6ebb2acc7a841a42706e1113746bb51a16` |

These runs were taken before the final alias-resolution change; that fix affects a duplicate-label Connect plan. The release build, 85-test server suite, plan suite, and public-client smoke above were rerun after the fix. The binary hash embedded in these reports is therefore a pre-fix binary hash, not the current candidate hash.

An additional run (`run-20261101T014834Z-77516`) used the exact pushed candidate commit `c1a191a4a59b800c863a6b519598a58cf45c3377`, with a clean source tree and binary SHA-256 `558628bd2b3169fabf7c41cc1067bfb71f0587244c22ea846fa3fbb183f9026a`. This binary was built with `--features connect,duckdb-source`; the v4.18.1 baseline has only the Connect feature because it predates DuckDB integration. The run completed all four 60-second cohorts at active limit 2. Results are throughput; short-job p95/p99; admission-wait p95; and peak server RSS:

| Clients | Throughput (ops/s) | Short-job p95/p99 (ms) | Admission p95 (ms) | Peak RSS (MiB) |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 20.82 | 19.6 / 20.0 | 0.0 | 55.6 |
| 4 | 71.90 | 46.0 / 54.1 | 30.1 | 58.1 |
| 8 | 102.06 | 101.8 / 118.3 | 84.0 | 59.4 |
| 16 | 103.50 | 217.0 / 232.8 | 202.0 | 60.2 |

The report records zero health/HTTP probe errors in every cohort, completion of the concurrent one-million-row slow reader, one native producer start and one reattach during injected disconnect replay, and graceful server shutdown. The exact compressed report is [phase-419-connect-mixed-candidate-c1-active-2.json.gz](evidence/phase-419-connect-mixed-candidate-c1-active-2.json.gz), SHA-256 `8e116c9ccc705e5e8ac86a8beb5e86f7b46a97427a805e32fd0a552c4aab41f6`.

A second c1 run (`run-20261101T015746Z-82450`) used the clean source revision above, binary SHA-256 `d0595d6b0afc0b3960fdd4474f0dbba293c535c2cfbfbe67449e40f8432476d4`, and `--features connect` to match the baseline's feature set. It completed all four 60-second cohorts at active limit 4. Values are throughput; short-job p95/p99; admission-wait p95; and peak server RSS:

| Clients | Throughput (ops/s) | Short-job p95/p99 (ms) | Admission p95 (ms) | Peak RSS (MiB) |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 21.08 | 19.4 / 19.6 | 0.0 | 54.2 |
| 4 | 80.78 | 19.7 / 20.5 | 0.0 | 57.2 |
| 8 | 144.39 | 23.8 / 30.7 | 7.1 | 61.3 |
| 16 | 183.83 | 62.3 / 73.5 | 42.1 | 65.4 |

All cohorts had zero health/HTTP probe errors. The slow reader completed one million rows; injected disconnect replay reported one producer start and one reattach RPC; shutdown was graceful. The compressed report is [phase-419-connect-mixed-candidate-c1-connect-active-4.json.gz](evidence/phase-419-connect-mixed-candidate-c1-connect-active-4.json.gz), SHA-256 `82a26a7e932985d9aa1e1c5c37dc4863ce1df2640b6cf3ca272d77b137a8f54f`. The active-4 v4.18.1 baseline failed during its eight-client cohort, so this is not a throughput ratio; the repeated paired performance gate remains open.

A further active-8 candidate run (`run-20261101T020701Z-88595`) used `--features connect` and server source content committed as `a0edec2d` (binary SHA-256 `92e8b76afc0547c572ba2b28316891b6c06da93a1fc3534f9766937d4dc44bb7`). The report's runner checkout was clean at c1; its server binary was rebuilt from the current branch after the test-only wrapper was gated with `#[cfg(test)]`. It completed all four 60-second cohorts:

| Clients | Throughput (ops/s) | Short-job p95/p99 (ms) | Admission p95 (ms) | Peak RSS (MiB) |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 21.33 | 19.5 / 19.9 | 0.0 | 53.5 |
| 4 | 79.57 | 19.8 / 20.3 | 0.0 | 56.8 |
| 8 | 145.26 | 20.7 / 23.2 | 0.0 | 58.1 |
| 16 | 194.48 | 44.6 / 68.6 | 3.8 | 51.0 |

The four cohorts had zero health/HTTP probe errors; the slow reader and disconnect-replay witnesses passed, and shutdown was graceful. The compressed report is [phase-419-connect-mixed-candidate-a0-connect-active-8.json.gz](evidence/phase-419-connect-mixed-candidate-a0-connect-active-8.json.gz), SHA-256 `4c9f141ae362affa22b5cf9b89e4e4d249bbd6ede806ab554e67c47942a098ce`. The v4.18.1 baseline run `run-20261101T020336Z-86275` at active limit 8 failed at eight clients with the same admission timeout. Its retained trace is [phase-419-connect-mixed-baseline-v4.18.1-active-8-failure.server.log.gz](evidence/phase-419-connect-mixed-baseline-v4.18.1-active-8-failure.server.log.gz), SHA-256 `63b347c775a1f48fd5114264086dbba1d3a0d12072bb9dee09d0e6fef0865362`. This provides candidate liveness evidence, not a baseline throughput ratio.

At 16 clients, the c1 active-2 run clears the provisional floor once: 103.50 correct ops/s, 217.0/232.8 ms short-job p95/p99, zero probe errors, and 60.2 MiB peak RSS. It is one candidate observation, not a repeated paired comparison. The baseline was built from v4.18.1 source tag commit `0ad225677ec47b20c9c6225fa9f4b73b00e54aba`, binary SHA-256 `4bcccfb2d838e3dab57ee91f2f6ee5735fb9b8c03d13361b04dd792f68c19aae`, with `--features connect`; the c1 benchmark runner and clients were copied into the detached tag worktree so both sides used the same workload. Baseline run `run-20261101T014645Z-75661` at active limit 2 completed the one-client cohort but failed at four clients with `DEADLINE_EXCEEDED: timed out waiting for Connect compute admission` (the configured timeout was 5 seconds). Run `run-20261101T015303Z-80128` at active limit 4 completed the one- and four-client cohorts but failed at eight clients with the same admission timeout. Neither run produced benchmark JSON or comparable throughput. Their retained server traces are [active 2](evidence/phase-419-connect-mixed-baseline-v4.18.1-active-2-failure.server.log.gz), SHA-256 `b475f00c488029aa98e89e96aad654ab53bb2a2ce5d936ce10765ce6c2312cf5`, and [active 4](evidence/phase-419-connect-mixed-baseline-v4.18.1-active-4-failure.server.log.gz), SHA-256 `87d87c58acfe4c73bd8ef14307830adc7bc6da85bbf928602a526fa91e2cea2e`. These are baseline liveness failures, not throughput comparisons.

The runner now supports `--queue-timeout-ms` (default `5000`) and records that value in its settings and server environment; its harness unit test covers both the default and an override. A diagnostic active-8 baseline run, `run-20261101T021439Z-93005`, used a 30,000 ms queue timeout with the same workload and 60,000 ms response timeout. It advanced through the one- and four-client cohorts, then failed during the eight-client cohort because the HTTP progress probe reported `HTTP SELECT 1 failed`. It also produced no benchmark JSON. The retained trace is [phase-419-connect-mixed-baseline-v4.18.1-active-8-q30-failure.server.log.gz](evidence/phase-419-connect-mixed-baseline-v4.18.1-active-8-q30-failure.server.log.gz), SHA-256 `fd1a74653c70e9746b1505310332a8e57ee7d945c0a224d1a453e2b4b98e48e4`.

The matched active-8 candidate diagnostic, `run-20261101T021819Z-95676`, used the same 30,000 ms queue and 60,000 ms response timeouts, Connect-only features, and four 60-second cohorts. The runner source was clean at `27c7ccdb75419d786cb9a4087ce195051c6fc72c`; the server was rebuilt from the candidate runtime source at `a0edec2d` (binary SHA-256 `92e8b76afc0547c572ba2b28316891b6c06da93a1fc3534f9766937d4dc44bb7`). It completed every cohort with zero health/HTTP probe errors:

| Clients | Throughput (ops/s) | Short-job p95/p99 (ms) | Admission p95 (ms) | Peak RSS (MiB) |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 21.24 | 19.5 / 19.7 | 0.005 | 54.2 |
| 4 | 80.68 | 19.7 / 20.1 | 0.004 | 57.3 |
| 8 | 145.19 | 20.5 / 21.2 | 0.006 | 60.4 |
| 16 | 200.22 | 31.2 / 43.1 | 4.068 | 51.1 |

The million-row slow reader completed with no probe errors; injected disconnect replay reported one producer start and one reattach RPC; shutdown was graceful. The compressed full report is [phase-419-connect-mixed-candidate-a0-connect-active-8-q30.json.gz](evidence/phase-419-connect-mixed-candidate-a0-connect-active-8-q30.json.gz), SHA-256 `462c00cb2580b3838930f1d2c24b7d1912b736396c4b5077e8b51dbda926a225`. This candidate diagnostic did not produce a baseline ratio because the baseline failed its HTTP progress probe at eight clients.

For the post-export/count candidate, the active-2/16-client cohort measured 105.67 correct ops/s, short-job p95/p99 of 216.9/232.4 ms, and 57.7 MiB server peak RSS. A **provisional** #1738 comparison floor is 100 correct ops/s at 16 clients and active limit 2, short-job p95 at most 220 ms and p99 at most 240 ms, zero health/HTTP errors, and peak RSS at most 64 MiB. This sets the requested numeric floor before any scheduler change, with roughly 5.4% throughput headroom. It is a selection from one fresh run per active limit, not a passed acceptance gate. The required three or more rotating fresh baseline/candidate pairs, 20-iteration small-job guardrails, and scheduler-only comparison against the post-export/count intermediate candidate remain outstanding. The active-8 run suggests improved tails without a clear throughput gain over active 4; no default was changed from this single observation.

## Open acceptance gates

- The optional standalone and Quack source modes, a bounded four-connection/sixteen-waiter pool, and focused source fixtures are present. The Quack fixture proves owner sharing and next-statement visibility, but is ignored unless the platform extension path is supplied and does not constitute a Spark differential corpus. The full source lifecycle/write-rejection matrix, clean platform packaging, and measured source resource budgets remain open. D14 and V35/V36 remain open.
- The helper's 17 launcher tests and public smoke now cover missing-client import failure, fork protection, startup/session/cleanup failures and retries, nested helpers, sequential reuse, unrelated active-session preservation, and public session creation. A full clean-consumer package installation/import matrix remains open.
- #1736/#1737/#1738 correctness and instrumentation work has landed on this branch. The public smoke covers the local-relation boundary and empty-result correctness cases, but the full paired export/count matrices, allocation/buffer measurements, repeated performance guardrails, and scheduler-only comparison remain open.
- No 4.19 API/native/server package version bump, clean-consumer package matrix, exact-commit CI result, or release publication check has been performed.

Accordingly, this evidence record does not mark any 4.19 slice or issue complete and does not authorize release publication.
