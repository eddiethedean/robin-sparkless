---
search:
  exclude: true
---

# Phase 4.19 implementation evidence

This is an interim record for the [4.19 release plan](RELEASE_4_19.md), not a release acceptance report. The implementation branch is `codex/release-4.19`. The earliest exploratory runs used `HEAD` `1f43130c0f96c9641442953fbb941478069922d9`, a then-dirty source digest `c5174677078c1fe0683c6424cdb268d77eb0387c37aa4889e24b73f1c75b6451`, and a Connect server binary SHA-256 `e6c41781d633629904bc2966e35e0fc9487944d8b2522b1fa78b6a5cb8726278` (reported version `4.18.0`). Later controlled runs below record their exact clean source revisions and binary hashes. The early digest included `output/benchmarks`; those untracked benchmark workspace files are not part of this implementation change.

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

Both public-client modes and the four source-pool unit tests were rerun from source revision
`ddf35e94c29f6eadd85c037afb8d3f6ed92404a6`. The standalone and Quack smokes used a release
server built with `http,connect,duckdb-source` (SHA-256
`b735bedee7fb5164fd7986ce96147f0cfbe25bf4d3ba73c52beb15edd50c479b`); the Quack owner was a
release example binary (SHA-256
`f3785aecac6e5adf5a77d5a46afef0ebf38befd826471e26900d8bd74817f7b5`). Both public-client
smokes passed, and all four source-pool tests passed.

### 4.19 package and release-workflow validation (2026-11-01)

All three Python distributions were built at version `4.19.0`, installed together in a temporary
CPython 3.11 overlay, and exercised through the installed public client. The API wheel
(`sparkless-4.19.0-py3-none-any.whl`, 70,425 bytes) has SHA-256
`15ca08ada712b953804eb5a88fb1f6b4b0b524cf379ee52532ac71635b76673f`; the native wheel
(`sparkless_native-4.19.0-cp311-abi3-macosx_11_0_arm64.whl`, 44,122,687 bytes) has SHA-256
`fe396247bf297019f9d0190c07319a95729abe66d4c5c06116abec6bb287b96c`; and the bundled server
wheel (`sparkless_server_bin-4.19.0-py3-none-macosx_26_0_arm64.whl`, 48,741,610 bytes) has
SHA-256 `a9b488ee8c308645e1172b24acf483b5d83fcca3dac1b4527b239422201a76cc`. The packaged server
binary was built in release mode with `http,connect,duckdb-source`, reports version `4.19.0`, and
has SHA-256 `252e1e5d2d0c7d9461163be9864def4b67535c94c748ab47a4d0d1e9957376be`.

The installed-wheel runtime verifier passed SQL, Parquet, and Delta checks. The server-wheel
verifier passed readiness, HTTP, Connect, port, and shutdown checks. Public `SparklessConnect`
checks passed for range/Column comparison/count, and the DuckDB source smoke passed in both
standalone mode (Python DuckDB 1.5.1 fixture) and Quack mode (bundled DuckDB 2.0.0 owner). Package
metadata reported `sparkless`, `sparkless-native`, and `sparkless-server-bin` as `4.19.0`, with
`pyspark-client==4.0.5`. The local server wheel carries a `macosx_26_0_arm64` tag because the
available host is macOS 26; it does not validate the release workflow's macOS 14 deployment tag.

The release workflow now builds both supported server wheels with `http,connect,duckdb-source`.
Its Linux and macOS server-extra consumer jobs install the hash-locked CPython 3.11 DuckDB 1.5.1
fixture dependency and run the public-client source smoke against the bundled server executable.
These exact release-runner jobs have not run for this branch yet, so cross-platform package
acceptance remains pending. Source loss/replay, full DDL/DML rejection, and measured source/RSS
budgets also remain open; this evidence does not claim complete DuckDB-source acceptance.

The SQL mutation path now checks configured read-only source namespaces before resolving rows or
falling through to a local catalog. This also covers missing table targets, including
`saveAsTable`. The release server rebuilt with the guard (`http,connect,duckdb-source`, SHA-256
`820674f28d96a61c250fb93b69b16bd993021da2b02f88d6aeb654acfe24138e`) passed the focused
read-only namespace test and the public-client smoke against that server binary. The smoke now checks INSERT,
UPDATE, DELETE, CREATE/REPLACE TABLE, DROP TABLE, ALTER TABLE, TRUNCATE, append/overwrite, and
creating a missing target in the configured source namespace; it confirms the original schema and
rows remain. Additional DDL/DML syntax forms and the source-loss/replay/resource cases remain open.

The standalone public-client smoke now also analyzes a table, temporarily removes its configured
database before collection, and verifies that the request fails; after restoring the same source,
the next statement succeeds with all rows. This checks that a source unavailable between analysis
and execution does not silently resolve elsewhere and that failure releases the source lease. It
passed against a release server rebuilt from `a850b891fc63503eeef82e6a122e426b5192a35b`
(`http,connect,duckdb-source`, binary SHA-256
`1518dc4368c186bf4a769563335f0cbb4c4b17d94886c591a86aad800a7b31e8`), installed 4.19.0 API,
native and server wheels, `pyspark-client==4.0.5`, and DuckDB 1.5.1. The cross-platform workflow
has not yet run this addition. Quack source loss during production, disconnect/reattach replay,
the full DDL/DML rejection matrix, and measured source/RSS budgets remain open.

It verifies exact values/schema, filter/project, UTC timestamp instant, `count(1)`, null-excluding
`count(column)` on populated/all-null/empty data, limit zero, empty-table schema, same-owner inner
join, grouped count/sum, unsupported source type/value errors, and rejected CREATE/INSERT/UPDATE/
DELETE/append/overwrite requests with an unchanged row count. The standalone fixture file was
created by Python DuckDB 1.5.1; the
Quack fixture separately uses the bundled DuckDB 2.0.0 owner.
Neither test replaces a Spark differential corpus. Quack disconnect/replay/source-loss acceptance,
the full source DDL/DML rejection matrix, clean platform packaging, and measured source/RSS budgets
remain open. D14 and V35/V36 are still open; the release does not claim complete DuckDB-source
acceptance.

## #1736 export and #1737 range-count measurements

An earlier smoke comparison with 15 samples per case claimed an 8.53× median improvement for 100k-row `collect()`, but omitted the exact commands and binary hashes. Its compressed report remains at [phase-419-export-smoke.json.gz](evidence/phase-419-export-smoke.json.gz), SHA-256 `9a551e66a977b5299a085cd869b5518ed4ef40be798522a20bbe00d5286a2587`; the controlled measurements below supersede that unpinned comparison.

### Paired public export measurements

`tests/connect/benchmark_phase419_exports.py` measures public `collect()` and `toArrow()` exports for ranges, empty filters, a limited range, a 10k-row local relation, and variable-width values including a 300k-character cell. It validates every returned range and local-relation value. Each fresh server gets one warmup per workload and 20 timed iterations; three baseline/candidate pairs ran in rotating order (baseline/candidate, candidate/baseline, baseline/candidate) on the same Apple M4 Pro host with `pyspark-client==4.0.5` and PyArrow 26.0.0. All six reports use clean harness revision `1bb18f26ff90919e84cd24383af942e4e413d6d0`. Both binaries are Connect-only: baseline v4.18.1 source `0ad225677ec47b20c9c6225fa9f4b73b00e54aba`, SHA-256 `4bcccfb2d838e3dab57ee91f2f6ee5735fb9b8c03d13361b04dd792f68c19aae`; candidate runtime source `a0edec2d`, SHA-256 `92e8b76afc0547c572ba2b28316891b6c06da93a1fc3534f9766937d4dc44bb7`. The harness used active operations 2, pending operations 16, max range rows 10m, queue timeout 5s, and response timeout 60s.

The table pools 60 warm samples per side and reports nearest-rank p50/p95 in milliseconds; speedup is baseline p50 divided by candidate p50. p95 and p99 from these sample counts are descriptive, not stable tail acceptance evidence.

| Workload | Baseline p50/p95 | Candidate p50/p95 | p50 speedup |
| --- | ---: | ---: | ---: |
| Range 1k `collect()` | 16.212 / 19.308 | 15.927 / 19.549 | 1.02× |
| Range 1k `toArrow()` | 16.399 / 18.444 | 14.100 / 16.491 | 1.16× |
| Range 10k `collect()` | 23.477 / 34.405 | 20.404 / 31.605 | 1.15× |
| Range 10k `toArrow()` | 18.441 / 20.761 | 15.356 / 16.688 | 1.20× |
| Range 100k `collect()` | 131.750 / 165.337 | 103.809 / 132.093 | 1.27× |
| Range 100k `toArrow()` | 46.526 / 48.313 | 15.705 / 30.760 | 2.96× |
| Range 100k, LIMIT 1k `collect()` | 16.111 / 20.456 | 16.041 / 19.335 | 1.00× |
| Range 100k, LIMIT 1k `toArrow()` | 16.968 / 18.412 | 14.975 / 16.488 | 1.13× |
| Empty filter 1k `collect()` | 42.012 / 48.222 | 16.021 / 17.176 | 2.62× |
| Empty filter 1k `toArrow()` | 42.013 / 48.562 | 16.427 / 17.288 | 2.56× |
| Empty filter 10k `collect()` | 267.060 / 332.510 | 16.720 / 19.415 | 15.97× |
| Empty filter 10k `toArrow()` | 275.862 / 332.950 | 16.854 / 18.719 | 16.37× |
| Local 10k `collect()` | 119.480 / 133.854 | 24.526 / 35.819 | 4.87× |
| Local 10k `toArrow()` | 110.622 / 119.169 | 19.175 / 21.255 | 5.77× |
| Variable-width `collect()` | 16.284 / 18.782 | 16.905 / 19.122 | 0.96× |
| Variable-width `toArrow()` | 16.707 / 19.438 | 18.723 / 19.528 | 0.89× |

These measurements use bare ranges: the 100k-row `toArrow()` median improves 2.96× and row-object `collect()` improves 1.27×. The original #1736 reproduction includes `.select("id")`; the [projected-range measurements below](#projected-range-speed-target) cover that speed target. LIMIT, at-cap local relations, variable-width values, and empty filters returned exact results. In this per-export harness the client peak RSS ranged from 250.7–254.2 MB on baseline and 254.3–256.7 MB on candidate; server RSS and retained Arrow-buffer measurements are not included. A separate candidate diagnostic on the same 100k-row Arrow result reported 391 source batches for 100,000 rows, 7 output batches, 0.636 ms serialization, 0.026 ms combine time, 0.979 ms in `arrow_next`, 15.012 ms blocked-send time, and 803,244 response bytes. The full diagnostic is [phase-419-arrow-export-mechanism-100k.json.gz](evidence/phase-419-arrow-export-mechanism-100k.json.gz), SHA-256 `9aec2fc1294789eb1c14cc45502791cd41ab158fef2555a13756a102257470da`; it is one instrumentation observation, not a repeated timing.

The full raw baseline/candidate reports and SHA-256 values are:

| Pair | Baseline report | SHA-256 | Candidate report | SHA-256 |
| ---: | --- | --- | --- | --- |
| 1 | [phase-419-exports-baseline-pair-1.json.gz](evidence/phase-419-exports-baseline-pair-1.json.gz) | `d21906ec8b7b8b6d37be181c31647f05aa6791e1811b60a2f2490cefd255a84b` | [phase-419-exports-candidate-pair-1.json.gz](evidence/phase-419-exports-candidate-pair-1.json.gz) | `da321980339305e843d0d7920f9c893c65ceaa13c7b839015488526cf90e1474` |
| 2 | [phase-419-exports-baseline-pair-2.json.gz](evidence/phase-419-exports-baseline-pair-2.json.gz) | `2aa727f3a24365e985b0e21e96fd89d4cdf53d4da39214dac5b255cd4703859c` | [phase-419-exports-candidate-pair-2.json.gz](evidence/phase-419-exports-candidate-pair-2.json.gz) | `d7ddef6b363ffe7691a4fd6f3424039e7423c36f2d19dbe2da5fff70ae87221c` |
| 3 | [phase-419-exports-baseline-pair-3.json.gz](evidence/phase-419-exports-baseline-pair-3.json.gz) | `cd98e6aeb255eab2c783e9b66fb8150c026038b211cd9b352f9933b1c2f30d8d` | [phase-419-exports-candidate-pair-3.json.gz](evidence/phase-419-exports-candidate-pair-3.json.gz) | `f0ea6c9547162e6c989ed9dfb082db920a136f4bd45c49483cc0943dfbaaf1a8` |

### Projected range speed target

**The original #1736 latency target passes:** `range(100_000).select("id").collect()` is **11.39× faster** at the pooled warm median, falling from **1,005.217 ms to 88.271 ms**. Each of the three fresh runtime pairs exceeds the initial 2× target (11.46×, 11.39×, and 11.27×). The earlier export harness omitted the projection and measured a different execution path. It now retains projected and bare ranges at 1k/10k/100k rows, with distinct workload names. The bare 100k-row `collect()` control improves 1.54× in these runs.

The baseline is the published `sparkless-server-bin==4.18.0` macOS arm64 executable, SHA-256 `1d79128ea8f09c40d980a5e8907b4a967f909b4c2e38ef58097daee0e0b91b13`, reporting `4.18.0`; its release source is `adce3c34de69ab2732de7ab16c8bb4c7445023c5` (`v4.18.0`). The release workflow at that tag builds the server with `cargo build --locked --release -p sparkless-server --no-default-features --features http,connect`. The candidate uses that same release build command at source `41ec36e03cb6f8bd82a65293b0e7e302c74c7c5b`, reports `4.19.0`, and has binary SHA-256 `55b18957519c0603b4c14dbba90ac711a3cd03febf189a0ef2c042223f0f5c31`.

All six reports use the same clean harness revision `41ec36e03cb6f8bd82a65293b0e7e302c74c7c5b`, CPython 3.11.15, unchanged `pyspark-client==4.0.5`, and PyArrow 26.0.0. Host: Apple M4 Pro, 12 logical CPUs, 24 GiB, macOS 26.5.2 arm64; candidate Rust 1.93.1. Both sides use four-thread Polars/Rayon/OpenMP pools, active operations 2, pending operations 16, a 10m-row range cap, a 5-second queue timeout, and a 60-second response timeout. Each fresh server gets one warmup and 20 timed actions per workload. Pair order is baseline/candidate, candidate/baseline, baseline/candidate. Range plan construction, result validation, startup, and shutdown are outside the action timer; local-relation actions include `createDataFrame`. Every returned range value and every local-relation value is checked.

The runner command is the same on both sides, with the executable, source revision, and report path changed. The source launcher is imported from the clean harness checkout; matching installed server-package metadata is supplied for each executable.

```sh
python tests/connect/benchmark_phase419_exports.py \
  --server-bin "$SERVER_BIN" --build-features http,connect \
  --build-profile release --server-source-revision "$SERVER_REVISION" \
  --iterations 20 --output "$REPORT"
```

Values below pool 60 samples per side and use nearest-rank p50/p95 in milliseconds. All measured export medians and p95 values improve; the p95 values are descriptive with this sample count. The client peak RSS ranges are 242.4–245.8 MiB on baseline and 245.0–245.3 MiB on candidate. This harness does not measure server RSS or complete the broader #1736 resource/lifecycle acceptance matrix.

| Workload | Released p50 / p95 | Candidate p50 / p95 | p50 speedup |
| --- | ---: | ---: | ---: |
| Projected range 1k `collect()` | 24.745 / 28.599 | 0.957 / 1.092 | 25.87× |
| Projected range 1k `toArrow()` | 24.334 / 24.994 | 0.429 / 0.500 | 56.72× |
| Projected range 10k `collect()` | 109.945 / 120.876 | 5.752 / 16.652 | 19.11× |
| Projected range 10k `toArrow()` | 103.842 / 107.786 | 0.786 / 0.902 | 132.05× |
| Projected range 100k `collect()` | 1005.217 / 1040.453 | 88.271 / 121.799 | 11.39× |
| Projected range 100k `toArrow()` | 909.688 / 925.651 | 4.293 / 4.442 | 211.88× |
| Bare range 1k `collect()` | 16.119 / 20.721 | 0.988 / 1.611 | 16.31× |
| Bare range 1k `toArrow()` | 16.592 / 18.148 | 0.421 / 0.494 | 39.38× |
| Bare range 10k `collect()` | 23.458 / 35.280 | 5.380 / 16.068 | 4.36× |
| Bare range 10k `toArrow()` | 18.417 / 22.759 | 0.453 / 0.518 | 40.70× |
| Bare range 100k `collect()` | 131.494 / 163.819 | 85.514 / 118.882 | 1.54× |
| Bare range 100k `toArrow()` | 45.953 / 48.386 | 1.381 / 1.579 | 33.27× |
| Range 100k, LIMIT 1k `collect()` | 16.104 / 19.150 | 0.906 / 1.069 | 17.77× |
| Range 100k, LIMIT 1k `toArrow()` | 16.929 / 18.184 | 0.420 / 0.523 | 40.26× |
| Empty filter 1k `collect()` | 47.265 / 50.147 | 0.533 / 0.584 | 88.62× |
| Empty filter 1k `toArrow()` | 47.417 / 49.948 | 0.525 / 0.603 | 90.27× |
| Empty filter 10k `collect()` | 330.288 / 352.736 | 1.678 / 1.747 | 196.88× |
| Empty filter 10k `toArrow()` | 334.350 / 351.742 | 1.695 / 1.777 | 197.26× |
| Local relation 10k `collect()` | 118.125 / 131.457 | 9.639 / 20.932 | 12.25× |
| Local relation 10k `toArrow()` | 110.038 / 113.815 | 4.271 / 4.492 | 25.76× |
| Variable-width `collect()` | 16.483 / 19.379 | 1.327 / 1.442 | 12.42× |
| Variable-width `toArrow()` | 17.416 / 19.795 | 1.323 / 1.445 | 13.16× |

The six reports record the binary source separately from the harness source, release profile, features, environment, and all raw samples:

| Pair | Released baseline report | SHA-256 | Candidate report | SHA-256 |
| ---: | --- | --- | --- | --- |
| 1 | [phase-419-projected-release-baseline-pair-1.json.gz](evidence/phase-419-projected-release-baseline-pair-1.json.gz) | `714b31e92cecdfda060c2b31dc5172970091e8ebd8ec3b2910c770df80b6328c` | [phase-419-projected-release-candidate-pair-1.json.gz](evidence/phase-419-projected-release-candidate-pair-1.json.gz) | `dfb0ce5cd6995c240052796f2a6d107675ffc6a4eaf96baee0b6a89e7f2d0166` |
| 2 | [phase-419-projected-release-baseline-pair-2.json.gz](evidence/phase-419-projected-release-baseline-pair-2.json.gz) | `173414ccd319090465228b38e4928340a6f26ff80ff914c92f6ec5784e17b12b` | [phase-419-projected-release-candidate-pair-2.json.gz](evidence/phase-419-projected-release-candidate-pair-2.json.gz) | `012937acdedac3c5f51bebcca18b03abd5f03c36562172796a58722651316d7d` |
| 3 | [phase-419-projected-release-baseline-pair-3.json.gz](evidence/phase-419-projected-release-baseline-pair-3.json.gz) | `8cbb8e4195a03aad152f8ac5930cfd0ea2621d5992c63d121196f3f4674d72fa` | [phase-419-projected-release-candidate-pair-3.json.gz](evidence/phase-419-projected-release-candidate-pair-3.json.gz) | `27f5bdd1716634972b6ed5c2f16e9bb33f675f9bbb84a56d21b853a5c2097b5e` |

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

A follow-up pair used `--queue-timeout-ms 30000` and `--http-probe-timeout-seconds 30` on both sides to remove the earlier client-side probe timeout. The v4.18.1 baseline (`run-20261101T043019Z-9560`, Connect-only binary SHA-256 `4bcccfb2d838e3dab57ee91f2f6ee5735fb9b8c03d13361b04dd792f68c19aae`) completed the 1-, 4-, and 8-client cohorts, then failed during the 16-client cohort with `RESOURCE_EXHAUSTED: OperationHistoryCapacityExceeded: Connect operation history is full`. The baseline rejects `ReleaseAll` for non-reattachable operations, so completed query records accumulate against its 1,024-entry limit. The compressed server trace is [phase-419-connect-mixed-baseline-v4.18.1-active-8-q30-p30-operation-history-failure.server.log.gz](evidence/phase-419-connect-mixed-baseline-v4.18.1-active-8-q30-p30-operation-history-failure.server.log.gz), SHA-256 `1712716efc0498b40006a222dc65b3fbbe6f8fe384239490dc1a7b7f97711591`.

The matched 4.19 runtime candidate (`run-20261101T043413Z-12208`) used the same four 60-second cohorts and settings. Its runner source was clean at `09e99540163626a2f104c4fbaa2b296695b0ab00`; the Connect-only server binary SHA-256 was `92e8b76afc0547c572ba2b28316891b6c06da93a1fc3534f9766937d4dc44bb7` (runtime source `a0edec2d`). It completed all cohorts with zero health/HTTP probe errors:

| Clients | Throughput (ops/s) | Short-job p95/p99 (ms) | Admission p95 (ms) | Blocked-send p95 (ms) | Peak RSS (MiB) |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 21.03 | 19.5 / 20.3 | 0.005 | 15.02 | 53.7 |
| 4 | 80.53 | 19.7 / 20.1 | 0.004 | 15.02 | 57.6 |
| 8 | 147.70 | 20.3 / 21.0 | 0.006 | 15.02 | 61.9 |
| 16 | 202.53 | 27.1 / 35.8 | 3.416 | 15.35 | 55.0 |

The one-million-row slow reader, injected disconnect replay (one producer start and one reattach), cancellation, and graceful shutdown all passed. The compressed full report is [phase-419-connect-mixed-candidate-a0-connect-active-8-q30-p30.json.gz](evidence/phase-419-connect-mixed-candidate-a0-connect-active-8-q30-p30.json.gz), SHA-256 `e7f56f80e0e3c0f2b6a92a0701d58401f1ddffc9eba8af79601ba870ceed825a`. This is candidate liveness and baseline failure evidence, not a paired throughput result at 16 clients; repeated paired performance acceptance remains open.

For the post-export/count candidate, the active-2/16-client cohort measured 105.67 correct ops/s, short-job p95/p99 of 216.9/232.4 ms, and 57.7 MiB server peak RSS. A **provisional** #1738 comparison floor is 100 correct ops/s at 16 clients and active limit 2, short-job p95 at most 220 ms and p99 at most 240 ms, zero health/HTTP errors, and peak RSS at most 64 MiB. This sets the requested numeric floor before any scheduler change, with roughly 5.4% throughput headroom. It is a selection from one fresh run per active limit, not a passed acceptance gate. The required three or more rotating fresh baseline/candidate pairs, 20-iteration small-job guardrails, and scheduler-only comparison against the post-export/count intermediate candidate remain outstanding. The active-8 run suggests improved tails without a clear throughput gain over active 4; no default was changed from this single observation.

### Response-channel wakeup paired release A/B

To isolate the #1738 send-path change, three rotating pairs compared clean 4.19 revisions that differ only in `crates/sparkless-server/src/connect.rs`: the polling baseline at `2d22d4a54f9f05dd3e72d979f600bf9def521dd9` and the wakeup candidate at `f0a81cc6f368355fb01b972443691b9cb1715d66`. Both servers were built with `cargo build --locked --release -p sparkless-server --no-default-features --features http,connect`; both report version `4.19.0`. Each fresh server ran the same public `pyspark-client==4.0.5` harness on an Apple M4 Pro (macOS 26.5.2 arm64), CPython 3.11.15, and PyArrow 26.0.0. Each workload had one warmup and 20 timed samples. Pair order was baseline/candidate, candidate/baseline, baseline/candidate. The harness source worktrees were clean and match the corresponding server revisions.

The table pools 60 samples per side and reports nearest-rank p50/p95 in milliseconds. These p95 values are descriptive with this sample count. The candidate removed the roughly 15 ms send-polling floor from small `toArrow()` and empty-result calls. The bare 100k row-object `collect()` median improved 1.15× in this sender-only comparison; the original #1736 reproduction is covered by the [projected-range measurements](#projected-range-speed-target). Client peak RSS was 242.3–245.7 MiB on the baseline and 239.0–246.7 MiB on the candidate; server RSS was not measured by this harness.

| Workload | Polling p50 / p95 | Wakeup p50 / p95 | p50 speedup |
| --- | ---: | ---: | ---: |
| Range 1k `collect()` | 15.758 / 18.547 | 0.933 / 1.571 | 16.89× |
| Range 1k `toArrow()` | 15.187 / 16.074 | 0.386 / 0.485 | 39.36× |
| Range 10k `collect()` | 20.399 / 31.446 | 5.458 / 16.096 | 3.74× |
| Range 10k `toArrow()` | 15.302 / 16.877 | 0.405 / 0.515 | 37.76× |
| Range 100k `collect()` | 101.923 / 148.096 | 88.441 / 123.174 | 1.15× |
| Range 100k `toArrow()` | 15.792 / 27.945 | 1.297 / 1.454 | 12.18× |
| Range 100k, LIMIT 1k `collect()` | 16.385 / 18.430 | 0.891 / 1.138 | 18.38× |
| Range 100k, LIMIT 1k `toArrow()` | 15.482 / 16.933 | 0.398 / 0.469 | 38.92× |
| Empty filter 1k `collect()` | 15.732 / 17.384 | 0.497 / 0.576 | 31.64× |
| Empty filter 1k `toArrow()` | 15.812 / 17.731 | 0.494 / 0.606 | 31.99× |
| Empty filter 10k `collect()` | 17.055 / 19.877 | 1.485 / 2.046 | 11.49× |
| Empty filter 10k `toArrow()` | 17.692 / 20.082 | 1.483 / 1.726 | 11.93× |
| Local relation 10k `collect()` | 25.056 / 37.422 | 9.750 / 21.165 | 2.57× |
| Local relation 10k `toArrow()` | 20.060 / 21.006 | 4.051 / 4.329 | 4.95× |
| Variable-width `collect()` | 17.694 / 19.510 | 1.214 / 1.326 | 14.58× |
| Variable-width `toArrow()` | 17.824 / 19.201 | 1.207 / 1.301 | 14.77× |

The six compressed reports retain all per-operation samples, binary hashes, and environment metadata:

| Pair | Polling baseline report | SHA-256 | Wakeup candidate report | SHA-256 |
| ---: | --- | --- | --- | --- |
| 1 | [phase-419-send-wakeup-baseline-pair-1.json.gz](evidence/phase-419-send-wakeup-baseline-pair-1.json.gz) | `b5869bc973d6795e4af0bc2aad37df95a7b3001cb48d7d46f7b81e944ffa2de2` | [phase-419-send-wakeup-candidate-pair-1.json.gz](evidence/phase-419-send-wakeup-candidate-pair-1.json.gz) | `8e59ddded638fa0e8c8829fa303b80da82e3e1b9986e6847f8a8525e53e18219` |
| 2 | [phase-419-send-wakeup-baseline-pair-2.json.gz](evidence/phase-419-send-wakeup-baseline-pair-2.json.gz) | `29367f8bdf2e48ce37bf5c6736217d9edd2fbecf85808f317ebf6f5801e1c93a` | [phase-419-send-wakeup-candidate-pair-2.json.gz](evidence/phase-419-send-wakeup-candidate-pair-2.json.gz) | `29294c0485ae2aba4da1c77285ddf22d2c6a21ccfa04d8029d8a22c3fb9c720b` |
| 3 | [phase-419-send-wakeup-baseline-pair-3.json.gz](evidence/phase-419-send-wakeup-baseline-pair-3.json.gz) | `974dcf5c123b68d0e9d51f98da0f632dec47afc53e55d5dca189331bc6192078` | [phase-419-send-wakeup-candidate-pair-3.json.gz](evidence/phase-419-send-wakeup-candidate-pair-3.json.gz) | `328741dec6ee0f01f0faceab57e0309e500a1058d284e7c53fa07310185d5efb` |

## Open acceptance gates

- The optional standalone and Quack source modes, a bounded four-connection/sixteen-waiter pool, and focused source fixtures are present. The Quack fixture proves owner sharing and next-statement visibility, but is ignored unless the platform extension path is supplied and does not constitute a Spark differential corpus. The full source lifecycle/write-rejection matrix, clean platform packaging, and measured source resource budgets remain open. D14 and V35/V36 remain open.
- The helper's 17 launcher tests and public smoke cover missing-client import failure, fork protection, startup/session/cleanup failures and retries, nested helpers, sequential reuse, unrelated active-session preservation, and public session creation. A full clean-consumer package installation/import matrix remains open.
- #1736–#1738 correctness and instrumentation work has landed. The original #1736 projected-range 2× latency target passes at 11.39× across three release pairs; paired exporter/count reports and a three-pair sender-only release A/B are retained above. Allocation/buffer measurements, full small-job guardrails, and a scheduler-only mixed-load comparison against the post-export/count intermediate candidate remain open.
- Coordinated 4.19.0 API/native/server package versions and local package checks are in place. The exact-commit release workflow still needs a successful run, and release publication checks have not been performed.

Accordingly, this evidence record does not mark any 4.19 slice or issue complete and does not authorize release publication.
