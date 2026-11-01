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

Results: release build passed; server library suite passed **85 tests**; focused ReleaseAll regression passed; Connect plan suite passed **19 tests**; launcher suite passed **12 tests**; the public PySpark client smoke passed helper lifecycle, range count, table join, and grouped aggregate checks. `cargo fmt --all` and `git diff --check` passed. These earlier checks cover implemented behavior only; DuckDB-source-specific evidence is recorded below.

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
cancellation watcher calls DuckDB's interrupt handle during a scan. The catalog admits at most four
source connections and queues at most sixteen waiting operations per catalog. These are
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
  quack_shared_owner_pins_read_snapshot_and_observes_writer_commit_next_statement -- --ignored
```

The fixture checks that the reader retains one row in its transaction after an independent Quack
writer commits a second row, then sees both rows in the next statement. It uses three independent
DuckDB connections in one test process and loopback HTTP; clean-platform extension installation,
separate-process/service lifecycle, TLS, cancellation, and wider race/resource cases remain open.

The source-pool unit tests passed with the crate's `sql` and `test-hooks` features enabled. They
verify the four-connection cap, sixteen-waiter cap, queue-full rejection, and cancellation removal:

```sh
CARGO_HOME=/tmp/sparkless-cargo-home cargo test --locked --release -p robin-sparkless-polars \
  --features duckdb-source,sql,test-hooks source_pool_tests
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

The 100,000-row result exceeds the initial 2× target. The report does not include the full #1736 required matrix (local relations, wide/variable-width data, LIMIT, oversized cells, allocation/buffer measurements) or the paired count measurements at 1k/1M/10M. The exact invocation and immutable binary/package hashes for both sides were not retained in this JSON, so this is preliminary evidence rather than a protocol-complete acceptance run. The compressed full report is [phase-419-export-smoke.json.gz](evidence/phase-419-export-smoke.json.gz), SHA-256 `9a551e66a977b5299a085cd869b5518ed4ef40be798522a20bbe00d5286a2587`.

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

For the post-export/count candidate, the active-2/16-client cohort measured 105.67 correct ops/s, short-job p95/p99 of 216.9/232.4 ms, and 57.7 MiB server peak RSS. A **provisional** #1738 comparison floor is 100 correct ops/s at 16 clients and active limit 2, short-job p95 at most 220 ms and p99 at most 240 ms, zero health/HTTP errors, and peak RSS at most 64 MiB. This sets the requested numeric floor before any scheduler change, with roughly 5.4% throughput headroom. It is a selection from one fresh run per active limit, not a passed acceptance gate. The required three or more rotating fresh baseline/candidate pairs, 20-iteration small-job guardrails, and scheduler-only comparison against the post-export/count intermediate candidate remain outstanding. The active-8 run suggests improved tails without a clear throughput gain over active 4; no default was changed from this single observation.

## Open acceptance gates

- The optional standalone and Quack source modes, a bounded four-connection/sixteen-waiter pool, and focused source fixtures are present. The Quack fixture proves owner sharing and next-statement visibility, but is ignored unless the platform extension path is supplied and does not constitute a Spark differential corpus. The full source lifecycle/write-rejection matrix, clean platform packaging, and measured source resource budgets remain open. D14 and V35/V36 remain open.
- The helper has focused lifecycle and public smoke coverage, but its full package-install, import-boundary, fork, startup/session/cleanup failure, nested/reentry, and unrelated-session matrix is still outstanding.
- #1736/#1737/#1738 correctness and instrumentation work has landed locally, but the complete workload and repeated performance guardrails in the release plan are not all satisfied by the evidence above.
- No 4.19 API/native/server package version bump, clean-consumer package matrix, exact-commit CI result, or release publication check has been performed.

Accordingly, this evidence record does not mark any 4.19 slice or issue complete and does not authorize release publication.
