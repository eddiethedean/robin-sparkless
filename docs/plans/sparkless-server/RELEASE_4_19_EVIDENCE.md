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
and either canonical local database paths or an explicit Quack endpoint/token plus a matching
locally packaged or explicitly configured extension path. It loads that exact Quack extension
without runtime downloads, exposes schema metadata
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

#### Clean consumer install matrix (2026-11-01)

A fresh CPython 3.11.14 consumer matrix was then built from the current 4.19 sources and installed
into four separate virtual environments on macOS 26.5.2 arm64. The exact API wheel SHA-256 is
`9c4b75f79864862d238a5498ccc003743c6bceb3644b9e5e81913bf093371ba9`; the native wheel is
`83c96a8f58d3275c48a61497f33ea565aa710069f06fac3a0879d815e0499380`; and the server wheel is
`954b563bf3d2430298086ee8455694c2aa208b1bf7310b8d823249399f9ccc52`. The server wheel contains
the release binary from source `43ce29b2c31a3da8d175a91cc350d09a76e8eeb4`, built with
`http,connect,duckdb-source` (binary SHA-256
`8c48df6228973930c744a491356f48e3058fa6f3f384d397c85c939d91778199`).

All four installs and `pip check` passed. The base install imported `SparklessConnect` without
installing `pyspark-client` or `sparkless-server-bin`; the server extra passed its installed-binary,
HTTP mutation, restart, and HTTP-only checks without PySpark; and both the Connect extra and
combined server/Connect extras passed the hash-locked `pyspark-client==4.0.5` dependency set and an
exact 32-row public `SparklessConnect` query. The raw matrix, installed metadata, wheel hashes, and
host limitation are in [phase-419-clean-install-matrix-macos.json](evidence/phase-419-clean-install-matrix-macos.json),
SHA-256 `333e31186428afbc79fc5e9d726a32a469a60ea2ba6837ac0e84b5c75e154c93`. This closes the
clean-consumer install/import matrix on the tested macOS arm64 host; the Linux and macOS 14-tagged
server-extra release jobs remain the cross-platform check.

The release workflow builds both supported server wheels with `http,connect,duckdb-source` and
now fetches the DuckDB 2.0.0 Quack extension for the exact wheel platform, packages it beside the
executable, and builds a matching owner fixture. The Linux and macOS server-extra consumer jobs
install the hash-locked CPython 3.11 DuckDB 1.5.1 fixture dependency and run the public-client
source smoke in standalone and packaged-Quack modes. These exact release-runner jobs have not run
for this branch yet, so cross-platform package acceptance remains pending. The standalone and
Quack source-loss/replay cases, write rejection, and macOS arm64 source/RSS budget evidence are
recorded below; broader platform/resource coverage remains open, and this evidence does not claim
complete DuckDB-source acceptance.

The SQL mutation path now checks configured read-only source namespaces before resolving rows or
falling through to a local catalog. This also covers missing table targets, including
`saveAsTable`. The release server rebuilt with the guard (`http,connect,duckdb-source`, SHA-256
`820674f28d96a61c250fb93b69b16bd993021da2b02f88d6aeb654acfe24138e`) passed the focused
read-only namespace test and the public-client smoke against that server binary. The smoke checks
INSERT and INSERT OVERWRITE, UPDATE, DELETE, CREATE/REPLACE TABLE and TABLE LIKE, CREATE/REPLACE
VIEW, DROP TABLE/VIEW, CREATE/DROP SCHEMA and DATABASE, ALTER TABLE add/rename/drop-column,
TRUNCATE, append/overwrite, and creating a missing save target in the configured source namespace;
it confirms the original schema and rows remain. A later smoke run with the same packaged server
(`http,connect,duckdb-source`, SHA-256
`252e1e5d2d0c7d9461163be9864def4b67535c94c748ab47a4d0d1e9957376be`),
`pyspark-client==4.0.5`, and Python DuckDB 1.5.1 passed these expanded checks. The public smoke
also rejects nested and non-finite values and a VARCHAR larger than 512 KiB.

The standalone public-client smoke now also analyzes a table, temporarily removes its configured
database before collection, and verifies that the request fails; after restoring the same source,
the next statement succeeds with all rows. This checks that a source unavailable between analysis
and execution does not silently resolve elsewhere and that failure releases the source lease. It
passed against a release server rebuilt from `a850b891fc63503eeef82e6a122e426b5192a35b`
(`http,connect,duckdb-source`, binary SHA-256
`1518dc4368c186bf4a769563335f0cbb4c4b17d94886c591a86aad800a7b31e8`), installed 4.19.0 API,
native and server wheels, `pyspark-client==4.0.5`, and DuckDB 1.5.1. The cross-platform workflow
has not yet run this addition.

It verifies exact values/schema, filter/project, UTC timestamp instant, `count(1)`, null-excluding
`count(column)` on populated/all-null/empty data, limit zero, empty-table schema, same-owner inner
join, grouped count/sum, unsupported source type/value errors, and rejected CREATE/INSERT/UPDATE/
DELETE/append/overwrite requests with an unchanged row count. The standalone fixture file was
created by Python DuckDB 1.5.1; the Quack fixture separately uses the bundled DuckDB 2.0.0 owner.
Neither test replaces a Spark differential corpus.

The public Quack smoke now uses a separate owner process and a separate writer process. After the
writer commits an event, the next Connect statement sees all four rows. The smoke then analyzes a
table, stops the shared owner before execution, verifies the read fails, restarts that same owner,
and verifies a subsequent query succeeds. This passed on macOS arm64 using the packaged release
server above and the DuckDB 2.0.0 `osx_arm64` Quack extension; the owner/writer fixture was built
from source revision `a857fde6`.

Both standalone and Quack public smokes now route a 50,000-row source scan through the existing
fault proxy, which drops the stream after the first ExecutePlan response. The pinned client's
normal retry returns every exact row; server tracing asserts one producer start and at least one
ReattachExecute RPC. The DuckDB-source cancellation fixture also passed against the same Quack
extension, cancelling after its first batch and observing `cancelled` on the shared owner. Public
Quack cancellation during graceful server shutdown also passed, as recorded below. Release-wheel
Quack extension packaging is implemented; exact cross-platform package validation and broader
platform/resource acceptance remain open. The Spark differential corpus is now recorded below.
D14 and V35/V36 are still open; the release does not claim complete DuckDB-source acceptance.

After the final Connect response-acknowledgment changes, the server was rebuilt at source
`43ce29b2c31a3da8d175a91cc350d09a76e8eeb4` with `http,connect,duckdb-source`. The release server
binary reports 4.19.0 and has SHA-256
`8c48df6228973930c744a491356f48e3058fa6f3f384d397c85c939d91778199`; the matching Quack owner
fixture binary has SHA-256 `ffe5dea9c2f513157ee6b3cf2efe3837dd31a4b0b81851b4fd12513faf21cfdc`. The
public DuckDB smoke passed in standalone mode with Python DuckDB 1.5.1 and in Quack mode with the
DuckDB 2.0.0 `osx_arm64` extension, using the 4.19.0 candidate client environment and pinned
`pyspark-client==4.0.5`. This validates source reads and the client retry/cancel path on the final
Connect implementation; it does not close shutdown cancellation or cross-platform extension
acceptance.

### DuckDB Connect source comparison with Spark 4.0.5 (2026-11-01)

`tests/connect/differential_phase419_duckdb.py` passed 14 row/schema comparisons against Apache
Spark 4.0.5 `local[4]` using the public Spark Connect client for the candidate and Spark's local
DataFrames for the reference. The cases cover two tables, filter/project, `LIMIT 0`, global and
nullable counts, empty table schema, all-null count, grouped count/sum, same-owner inner join with
duplicate/null keys, grouped null keys, exact `SMALLINT`/`INTEGER`/`BIGINT` extrema and non-null
schema, `DOUBLE`/`DATE`/UTC microsecond timestamp types, and exact decimal values at
`DECIMAL(1,0)`, `(9,0)`, `(18,6)`, `(38,0)`, and `(38,38)` boundaries. This closes the recorded
table/join/aggregate and selected scalar-boundary reference comparisons for the declared subset; it
does not certify general Connect SQL, other Spark versions, or every precision/scale pair.

The local rerun used test harness revision `78b608cd5ed4dde57b6eb0de277307f4d63495c9`, API
`4.19.0`, PySpark `4.0.5`, DuckDB fixture writer `1.5.1`, macOS 26.5.2 arm64, and the local
release-profile `sparkless-server 4.19.0` executable with SHA-256
`81a72f39a383c01ccf96f56136fd560cb62d27b08dc1f70feeb1f0ab2fc708b0` (built at source
`22034d3014163c72c0511d0363d3388587412ecb`). The oracle ran in Spark's bundled `local[4]` runtime
in the same process; it was not a Spark Connect reference server. The raw report is
[phase-419-duckdb-spark-differential.json.gz](evidence/phase-419-duckdb-spark-differential.json.gz),
SHA-256 `b9fee26ffb696da3db7879132c925626fb90e756e29b08bfbedd80bff5e1f053`.

### Public DuckDB cancellation during graceful shutdown (2026-11-01)

`tests/connect/smoke_phase419_duckdb_shutdown.py` passed against a live Quack owner with a public
Spark Connect client. The source query was confirmed active before the normal server shutdown
command. Shutdown completed in 0.088 seconds, the client received `Spark Connect server is
shutting down`, and the server's source trace recorded `duckdb_source_cancel_requested` followed by
`duckdb_source_remote_cancel accepted=true`. This closes the macOS arm64 public shutdown-cancel
witness; it does not establish the same extension behavior on other platforms.

The release executable was built at source `6b3b7408` with
`cargo build --locked --release -p sparkless-server --no-default-features --features
http,connect,duckdb-source`, and has SHA-256
`15c8ce1c0ab9caca28b56de8d5b6646ecbc5bc89cc665da48da7bcab0b93e92d`. The fixture used the
DuckDB 2.0.0 `osx_arm64` Quack extension and the owner example from that same source. The runner is
at source `0d6215c9`. Its compressed result is
[phase-419-duckdb-shutdown-cancel.json.gz](evidence/phase-419-duckdb-shutdown-cancel.json.gz),
SHA-256 `e246e9eda7a2f3da87e267e14b50d1bad6e37340703469ef8ca1ea9d35ac433a`.

### Concurrent source scans and slow-reader budgets (2026-11-01)

Three fresh release-server processes per cohort ran four independent `pyspark-client==4.0.5`
sessions against one standalone DuckDB source, with four active Connect operations, a 16-operation
pending limit, and a 10 ms pause after each 16,384-row group. The host was an Apple M4 Pro running
macOS 26.5.2 arm64 with CPython 3.11.14, PyArrow 26.0.0, DuckDB fixture writer 1.5.1, and the
packaged `sparkless-server 4.19.0` executable (SHA-256
`252e1e5d2d0c7d9461163be9864def4b67535c94c748ab47a4d0d1e9957376be`). Each RSS value is the
peak of that one server child after exit, not a total machine, client, or allocator measurement.

At 100,000 rows per reader with 16-byte payloads, all four readers completed on every measured
round. Three server RSS peaks were 155.5, 147.5, and 147.3 MiB; the per-reader latency p50 ranged
from 1,022.5 to 1,026.2 ms, and the descriptive p95 ranged from 1,038.1 to 1,072.1 ms. At 200,000
rows per reader, the 8 MiB Connect replay/sender cap was occasionally reached by slow consumers:
across the three fresh processes, 7, 0, and 8 of 16 total warmup/measured actions were rejected
with the expected cap error, while every completed action returned exactly 200,000 rows. RSS peaks
were 206.6, 195.6, and 198.8 MiB. At 350,000 rows per reader, all four clients hit the same
expected cap in each warmup, and the retained per-reader errors confirm prompt rejection; the three
server RSS peaks were 211.3, 213.1, and 211.3 MiB.

This selects a **provisional measured guardrail of 256 MiB server-child RSS** for the tested
four-reader 100k–350k workload range on this host. It is an observational release-candidate budget,
not an enforced process memory ceiling or a promise for other platforms. The 200k mixed
completion/rejection reports also show why result outcomes are retained per reader instead of
reducing the run to one median.

The 100k success cohort used harness source `6b3b7408` / script SHA-256
`495718bf1a9c903b848e3e6468812aee946ef5340def2157fa5e64fb1fec9546`; the mixed 200k cohort used
source `819333a5` / script SHA-256
`c47fcf95d4442f978d3e98e74c04b01b0b95325f242801f2340a8c6c333c7dcd`; the 350k cap cohort used
source `578b6ee5` / script SHA-256
`f50dfd1f5ee9ded1bdd6925881fce92b08888dd8fc06c22b010ebf5a45998c71`.

| Cohort | Run | Result report | SHA-256 |
| --- | ---: | --- | --- |
| 100k × 16 B, four readers | 1 | [report](evidence/phase-419-duckdb-concurrent-slow-readers-run-1.json.gz) | `c6f61fc2753cac2ed48d6167ec41c148e9084b4a2d9e6eea3cc06704ac7af2b3` |
| 100k × 16 B, four readers | 2 | [report](evidence/phase-419-duckdb-concurrent-slow-readers-run-2.json.gz) | `849a0c191aa063f94ef63505ebbe6e408308f79a261bf6eb9564efb276d5cdef` |
| 100k × 16 B, four readers | 3 | [report](evidence/phase-419-duckdb-concurrent-slow-readers-run-3.json.gz) | `7a262129f9be302f8cb82a40e1f2437a61b5269358844f33371fd017d500c7a8` |
| 200k × 16 B, four readers | 1 | [report](evidence/phase-419-duckdb-concurrent-slow-readers-200k-run-1.json.gz) | `a7ce3f0292b90c49d24ed4d3c8aba2ecb1ce18683b0b458135235a14ae32b3ff` |
| 200k × 16 B, four readers | 2 | [report](evidence/phase-419-duckdb-concurrent-slow-readers-200k-run-2.json.gz) | `416bf4c58bcb9f03260c07539576d8edf6f7f3390e5453b017febdfa2b5d2c19` |
| 200k × 16 B, four readers | 3 | [report](evidence/phase-419-duckdb-concurrent-slow-readers-200k-run-3.json.gz) | `0f88223558bff91d995c0351fc82945597b6b183df4ce89bcd1aed28fd1e0186` |
| 350k × 16 B, four readers | 1 | [report](evidence/phase-419-duckdb-concurrent-result-reject-350k-run-1.json.gz) | `577e247a08c3c0e960f7d605adb58c4f3206a6d2a93cd69e3c35e496e9577a0e` |
| 350k × 16 B, four readers | 2 | [report](evidence/phase-419-duckdb-concurrent-result-reject-350k-run-2.json.gz) | `909f3d36b1da002e7115b00d0e37588642af931312fc0b159730a5f903a49fe8` |
| 350k × 16 B, four readers | 3 | [report](evidence/phase-419-duckdb-concurrent-result-reject-350k-run-3.json.gz) | `a45bb99ba714b2b21edb4e178efb6d3514552423887280f0dc5bdd5fbd61f8f9` |

### One-million-row four-reader boundary (2026-11-02)

Three exploratory runs on macOS 26.5.2 arm64 used the 4.19.0 macOS server binary from release
workflow [41362360697](https://github.com/eddiethedean/robin-sparkless/actions/runs/41362360697)
(SHA-256 `eb1fec83f8018f311b85a307ef9b95f30f91ee9ee5f1fed4b18fa145ac2db4f0`; features
`http,connect,duckdb-source`). Harness revision was `831fa0ff7778e9dfb5ccc560ad2d203bf43f06cc`.
The active-limit-4 run used harness script SHA-256
`c47fcf95d4442f978d3e98e74c04b01b0b95325f242801f2340a8c6c333c7dcd`; active limits 2 and 1
used the script with explicit admission/queue options, SHA-256
`41c5f83f7d3641efb5b8c077b30d6d3b4316fe2f214edae4b1d18e7663fd9344`.
Four independent clients each consumed exactly 1,000,000 rows with 16-byte payloads through
`toLocalIterator()`, pausing 10 ms per 16,384-row batch. The one warmup and one measured round
completed for every reader without a result-limit rejection. Each source had 24 MB of logical
column data; the aggregate was 96 MB. The per-client action p50/p95 samples were 8,890.2/9,130.4
ms at active limit 4, 9,223.4/9,235.6 ms at active limit 2 (with a 30-second queue timeout),
and 9,039.3/9,122.4 ms at active limit 1 (with a 60-second queue timeout).

Server-child RSS peaked at 337,117,184 bytes (321.5 MiB) with active limit 4 and 281,526,272 bytes
(268.5 MiB) with active limit 2, and 265,961,472 bytes (253.6 MiB) with active limit 1. The first
two exceed the provisional 256 MiB guardrail selected for the 100k–350k four-reader range. The
active-1 result is one run only, with roughly 2.4 MiB of headroom, so it does not establish a stable
budget. These observations are not process memory limits, but they show that the 1M per-statement
row cap does not imply the same RSS budget under four simultaneous scans, even when compute
admission is 2. High-volume concurrent source work remains outside the measured 256 MiB range. The
compressed reports are [active limit 4](evidence/phase-419-duckdb-concurrent-resource-1m-active4-macos.json.gz),
SHA-256 `2b55d1994fb18c3143b2fd07d159cddfe812ac6f194a268ab6e6c3b895b47fbf`, and [active limit 2](evidence/phase-419-duckdb-concurrent-resource-1m-active2-macos.json.gz),
SHA-256 `ff42a1663b653451f0d6ae4b6168ddcf32d6192a90e2c6662017d10ff1fc7824`, and [active limit 1](evidence/phase-419-duckdb-concurrent-resource-1m-active1-macos.json.gz),
SHA-256 `23fccebb4fe1e469ba08fd3c44eb109a8e8287ca2d976c15540cc53583afeb51`.

### Standalone DuckDB source resource measurements (2026-11-01)

`tests/connect/benchmark_phase419_duckdb_source.py` created distinct hash-derived VARCHAR rows in
Python DuckDB 1.5.1 and issued one warmup plus three measured public-client actions through a fresh
Sparkless server for each report. The count reports use `count()`; the bounded slow-reader report
uses `toLocalIterator()`. The client stack was `pyspark-client==4.0.5`,
PyArrow 26.0.0, CPython 3.11.14. Platform: Apple M4 Pro, macOS 26.5.2 arm64. Server artifact:
release 4.19.0, features `http,connect,duckdb-source`, binary SHA-256
`252e1e5d2d0c7d9461163be9864def4b67535c94c748ab47a4d0d1e9957376be`. Harness source revision
`8977b683f0d167011cc0c452d58b6eb223fb25e3`, script SHA-256
`0127ad46725b87ec03097a204603925b35e4566224deb24120b767e224decbcc`. Count timings include the
Connect request and result action but exclude startup and shutdown. RSS is the operating system's
peak for the single managed server child (`getrusage(RUSAGE_CHILDREN)` after it exits); this is a
measured single-operation observation, not a process-enforced RSS cap.

| Rows × payload | Uncompressed columns | DuckDB file | Action p50 / p95 | Peak server RSS |
| --- | ---: | ---: | ---: | ---: |
| 100k × 16 B | 2.4 MB | 1.26 MiB | 43.94 / 44.84 ms | 73.9 MiB |
| 500k × 16 B | 12 MB | 4.76 MiB | 57.43 / 58.94 ms | 97.8 MiB |
| 1M × 16 B | 24 MB | 9.01 MiB | 74.09 / 74.29 ms | 122.4 MiB |
| 1M × 20 B | 28 MB | 11.01 MiB | 81.24 / 81.80 ms | 131.0 MiB |

The 1M × 20 B case was repeated in three fresh server processes. Peak RSS was 131.0, 138.9, and
136.0 MiB; count p50 was 81.24, 81.55, and 81.36 ms. The maximum observed 138.9 MiB is the
current single-scan measurement at the 1M-row cap with a 28 MB logical source; it is not a hard
limit and does not cover concurrent scans or replay retention.

A bounded public `toLocalIterator()` run consumed 200k × 16 B rows while pausing for 20 ms after
each 16,384-row group. All IDs and payload lengths were checked. It completed with action p50/p95
of 845.82/852.65 ms and peak server RSS of 107.2 MiB. At the 1M × 20 B size, the same public
iterator reaches the existing 8 MiB per-operation Connect replay/sender-buffer limit and fails
with `Connect replay and sender buffers exceed 8388608 bytes for one operation`; the server peak
on that rejected run was 140.8 MiB. This records the transport boundary, not a passing 1M-row slow
reader. These reports use harness revision `45b6803fa7b8bb1ce78951cc727e8bf5416b64e7`, script
SHA-256 `22a4e9621e954e0b7fa6e4dc5bc8ebca6e92795cdd701278680b3e208cd5f434`.

Two negative runs confirm operation-level enforcement. A 1M-row × 256 B source (264 MB logical,
155.8 MiB on disk) fails with `DuckDB source exceeds the 33554432-byte operation limit`; a
1,000,001-row × empty-string source fails with `DuckDB source exceeds the 1000000-row operation
limit`. The decoded-byte counter separately has a unit test that accepts exactly 64 MiB across
batches and rejects the next byte without increasing accounting. Existing source-pool tests
exercise the four-connection/sixteen-waiter limits and Quack's two-slot scan/cancel reservation.

On branch revision `f386022bc88f9a1ec9ab244a75f50cdfd469455c`, release-mode validation with
`--features sql,duckdb-source` passed all seven `duckdb_source` integration tests (six passed; the
live Quack extension test was ignored because no extension path was supplied) and all five
`source_pool_tests`. The pool is a concurrency limiter: waiting acquires occur before a source
connection is opened or any source batch is read, so queued source Arrow/decoded payload bytes are
zero. Waiters still retain plan/control state that is count-bounded but not separately
byte-accounted. Active connections belong to statement snapshots and close when those snapshots
are dropped; there is no idle connection pool or source idle TTL.

Raw reports retain counts, timings, file sizes, server/client hashes, and OS RSS:

| Case | Report | SHA-256 |
| --- | --- | --- |
| 100k × 16 B | [report](evidence/phase-419-duckdb-source-100k.json.gz) | `455566f8e85a38c3225282e66c01c63e80a876b9b07b70ce8bc19b25c545d896` |
| 500k × 16 B | [report](evidence/phase-419-duckdb-source-500k.json.gz) | `e80233504a023272b746aeef908c5246047621ad56d7b8af9c599c77eee3cd03` |
| 1M × 16 B | [report](evidence/phase-419-duckdb-source-1m.json.gz) | `0d2547fc511c13c475d063904945204d4abaef3e79d2432f2460cef4775462e9` |
| 1M × 20 B | [report](evidence/phase-419-duckdb-source-near-cap-1m.json.gz) | `f7aed52f457c549adc48c053142fa6b3290b75d3ef919e071741e5ca843622dc` |
| 1M × 20 B repeat 2 | [report](evidence/phase-419-duckdb-source-near-cap-pair-2.json.gz) | `4dc18862f76a2cf8ba15d259220413c23ff1e37f9fe5a0fca82ebe86f1314bc5` |
| 1M × 20 B repeat 3 | [report](evidence/phase-419-duckdb-source-near-cap-pair-3.json.gz) | `55986f983c0870ae6df533d564280041fc999deffef5dae59c1112a50393edc5` |
| Slow reader, 200k × 16 B | [report](evidence/phase-419-duckdb-source-slowreader-200k.json.gz) | `ec32aa64a20b224d89d1bce8490ff03e272fc77c204735be1a9b9873a3961482` |
| IPC cap rejection | [report](evidence/phase-419-duckdb-source-reject-ipc.json.gz) | `8b2b98762f8df191ef9542d63fc37154a8a0b717652e7569c553bdc1c355d85f` |
| Row cap rejection | [report](evidence/phase-419-duckdb-source-reject-rows.json.gz) | `b54bd7e5455eb16f50bde156ba7670f2933e049238e05014f78b224b5bfb4807` |
| Result-buffer cap rejection, 1M × 20 B | [report](evidence/phase-419-duckdb-source-reject-result.json.gz) | `4a8a32361118301825b4183c8dc29b5d3e89c61c8a4d52187429ab088cb5f386` |

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

The table above measures the earlier exporter candidate. After the final reattachable-response
acknowledgment and cancellation changes, the exact original `range(100_000).select("id").collect()`
target was rerun using `--projected-range-only` on the same public client. Three rotating release
pairs each collected 20 warm samples per side and 20 fresh startup samples per runtime. The
baseline was the `v4.18.1` release binary (reported version 4.18.0, effective `http,connect`, SHA-256
`4bcccfb2d838e3dab57ee91f2f6ee5735fb9b8c03d13361b04dd792f68c19aae`); the candidate was clean
source revision `43ce29b2c31a3da8d175a91cc350d09a76e8eeb4`, release profile, `http,connect`, SHA-256
`c604f85ab4856dbe3717c930ff3a47b3119b2b00f6a98a54ef9263889779f745`. The host, CPython 3.11.15,
PyArrow 26.0.0, `pyspark-client==4.0.5`, helper, and server limits matched the comparison above.
Across 60 samples, baseline p50/p95 was 1,013.776/1,052.723 ms and candidate p50/p95 was
148.010/181.050 ms, a **6.85× p50 speedup** at the final source revision. Startup p50/p95 was
9.005/10.977 ms baseline and 9.017/11.527 ms candidate. The 2× target therefore still passes at
the final cancellation-safe revision; its response acknowledgment adds latency relative to the
earlier exporter-only candidate while retaining substantial margin over the target.

Each run used `tests/connect/benchmark_phase419_exports.py --projected-range-only --iterations 20
--startup-samples 20`; the reports retain the binary and harness provenance, all raw samples, and
environment:

| Pair | Baseline report | SHA-256 | Candidate report | SHA-256 |
| ---: | --- | --- | --- | --- |
| 1 | [baseline](evidence/phase-419-projected-final-baseline-pair-1.json.gz) | `5ec07ae190c75405f51e2c3cf00cd437a23224187fe1b1bb4fdecd1dcb0579b1` | [candidate](evidence/phase-419-projected-final-candidate-pair-1.json.gz) | `5f48275562c860af1ddac997d4862525dbfce68f85fddb06640967a502f0ff45` |
| 2 | [baseline](evidence/phase-419-projected-final-baseline-pair-2.json.gz) | `3dd1c77d3edb0f95a555b249ff144ab6124caccd46400034bc20824dce1ee72e` | [candidate](evidence/phase-419-projected-final-candidate-pair-2.json.gz) | `612240604e6c1becaf142c75c7b7d46fafd5931d8d2b90c38da39f8d507f098c` |
| 3 | [baseline](evidence/phase-419-projected-final-baseline-pair-3.json.gz) | `826e270fe9ab5f2337e6dbd433dcf789c92da09db4d20aa7869ea41e3ad41efa` | [candidate](evidence/phase-419-projected-final-candidate-pair-3.json.gz) | `9946eea010866556a1a4a6de0e7b7b053ee4d14ec0b026c283268b092d6df0b9` |

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

For the post-export/count candidate, the active-2/16-client cohort measured 105.67 correct ops/s, short-job p95/p99 of 216.9/232.4 ms, and 57.7 MiB server peak RSS. A **provisional** #1738 comparison floor is 100 correct ops/s at 16 clients and active limit 2, short-job p95 at most 220 ms and p99 at most 240 ms, zero health/HTTP errors, and peak RSS at most 64 MiB. This sets the requested numeric floor before any scheduler change, with roughly 5.4% throughput headroom. The original selection was one fresh run; the three-pair scheduler-only comparison against this intermediate candidate is recorded below and passes the floor at active limit 2. The 20-iteration small-job guardrails and complete scheduler comparison against the released baseline remain outstanding. No default was changed.

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

### Scheduler-only intermediate mixed-load comparison

Three rotating pairs compared the post-export/count polling baseline at `2d22d4a54f9f05dd3e72d979f600bf9def521dd9` with the response-channel wakeup candidate at `f0a81cc6f368355fb01b972443691b9cb1715d66`. Those source revisions differ only in `crates/sparkless-server/src/connect.rs`. Both release binaries used `--no-default-features --features http,connect`, reported version `4.19.0`, and used the same `pyspark-client==4.0.5` harness. Binary SHA-256 values are baseline `6678b4dda8eb70871fd964ca4ec1ab9ff64f7d1373438ca91b6ad8fa8e0d6f92` and candidate `8d2744a4d4e5e676f7e9947496b11c4ab079de38313831a41b9d14e54f5f26b5`. The runner source revision was `325fbf4f057ddeb7dbce07c5cb153f07dd26e984`.

Each fresh server ran four 60-second steady cohorts at 1/4/8/16 clients, with active-operation limits 2/4/8, one-million-row filtered counts, 100,000-row collections, a concurrent one-million-row slow reader, health/HTTP probes, disconnect/replay fault injection, and server CPU/RSS and Connect phase sampling. Queue and HTTP probe timeouts were both 30 seconds. Pair order was baseline/candidate, candidate/baseline, baseline/candidate for every active limit. Platform: Apple M4 Pro (12 logical CPUs, 24 GiB), macOS 26.5.2 arm64, Rust 1.93.1, CPython 3.11.14, PyArrow 26.0.0, UTC.

The following throughputs are medians across the three fresh pairs, shown as polling baseline → wakeup candidate in correct ops/s. Tail percentiles and peak RSS are medians of the three per-run values for the 16-client cohort; the runner uses nearest-rank percentiles. Each cohort retained at least hundreds of short-job samples. All 18 reports recorded zero health/HTTP errors; each slow reader completed one million rows, replay used one producer start and one reattach RPC, and both server and proxy shut down gracefully.

| Active limit | Ops/s at clients 1 / 4 / 8 / 16 | 16-client short-job p95 (ms) | 16-client short-job p99 (ms) | 16-client server peak RSS (MiB) |
| ---: | --- | ---: | ---: | ---: |
| 2 | 20.2→28.1 / 69.7→100.8 / 99.4→163.5 / 103.8→192.9 (1.86×) | 216.2→42.7 | 231.9→64.0 | 60.4→59.7 |
| 4 | 20.3→28.1 / 76.4→100.4 / 137.3→162.4 / 180.6→189.3 (1.05×) | 55.9→43.9 | 65.8→68.2 | 63.8→65.0 |
| 8 | 20.3→28.2 / 76.6→100.0 / 138.3→164.0 / 186.3→190.3 (1.02×) | 38.0→40.0 | 54.9→62.5 | 65.2→64.5 |

At the provisional active-2 floor, all three candidate pairs exceed 100 correct ops/s at 16 clients and stay under 220/240 ms short-job p95/p99 and 64 MiB peak RSS. Active limit 2 shows the clear tail improvement; active limits 4/8 show modest throughput gains, with small p99 regressions. No default was changed. This closes the scheduler-only intermediate comparison. The published 4.18.0 executable initially exhausted its default 1,024-operation history during the 16-client cohort; the matched benchmark-only history override and completed released-baseline comparison are recorded below.

The 18 compressed reports retain per-operation samples, phase counters, environment and server hashes:

| Active limit | Pair | Polling baseline report | SHA-256 | Wakeup candidate report | SHA-256 |
| ---: | ---: | --- | --- | --- | --- |
| 2 | 1 | [baseline](evidence/phase-419-scheduler-intermediate-active-2-baseline-pair-1.json.gz) | `bf8b848b4f955aef9b32a23327b3cf444a6a7f5a6f3a9254d255f75019ce6401` | [candidate](evidence/phase-419-scheduler-intermediate-active-2-candidate-pair-1.json.gz) | `efa594d22a03fd013081c707480a1a357dc0d71f239ed276a79cda5f844afee2` |
| 2 | 2 | [baseline](evidence/phase-419-scheduler-intermediate-active-2-baseline-pair-2.json.gz) | `57ee105b0f4a430c3a2865b094af5bcd140b72e38f94eff1e0ab03f9338e8d12` | [candidate](evidence/phase-419-scheduler-intermediate-active-2-candidate-pair-2.json.gz) | `c8f302bbeee332a955c6f7f1fc7399692855ef387e971202943e527a3e8f0ebf` |
| 2 | 3 | [baseline](evidence/phase-419-scheduler-intermediate-active-2-baseline-pair-3.json.gz) | `5e9fcfe22c1ab0be9fcc1a495439e4e4585103c9b284743e6f768e679d8498dc` | [candidate](evidence/phase-419-scheduler-intermediate-active-2-candidate-pair-3.json.gz) | `beed1149c5cee337861e7326d6294a246b472dc98a95eeac37da449aa0f23784` |
| 4 | 1 | [baseline](evidence/phase-419-scheduler-intermediate-active-4-baseline-pair-1.json.gz) | `860c43eead6f9efc9a3dc5480e41050848c3c58971a7b7375d7ee870741fd8ea` | [candidate](evidence/phase-419-scheduler-intermediate-active-4-candidate-pair-1.json.gz) | `062f45ce0f989c345cf4f915b648d092c572dd5380b1c327eb937be37cadb3d0` |
| 4 | 2 | [baseline](evidence/phase-419-scheduler-intermediate-active-4-baseline-pair-2.json.gz) | `cd6289b6bacd2b4fd53cf7746f341b4a31f8cef117d6582c9ac12d4821a68e6c` | [candidate](evidence/phase-419-scheduler-intermediate-active-4-candidate-pair-2.json.gz) | `aef14f19a61b2e05c73f8a742930780245bfa6ac4c68573966af1236d51db6fc` |
| 4 | 3 | [baseline](evidence/phase-419-scheduler-intermediate-active-4-baseline-pair-3.json.gz) | `ba54ca213d366f658e14536eb81c5f8925c742d7975426479850dd5f3bd12f80` | [candidate](evidence/phase-419-scheduler-intermediate-active-4-candidate-pair-3.json.gz) | `a35152a4e3df86b6a7f49030bbae02bbdff0882c037c63401b64833dda0ead00` |
| 8 | 1 | [baseline](evidence/phase-419-scheduler-intermediate-active-8-baseline-pair-1.json.gz) | `226c0d915488d278e28eba991e164f522456ef705ec2a2e2cd8e82c19d08aa8c` | [candidate](evidence/phase-419-scheduler-intermediate-active-8-candidate-pair-1.json.gz) | `59f4d6a0e4653e436d324b388a258aec098285e7b499dedd9b2b7bc986e0884d` |
| 8 | 2 | [baseline](evidence/phase-419-scheduler-intermediate-active-8-baseline-pair-2.json.gz) | `2d868bd91898d2b13b875174ddd9692239118626c5b4de1ad7404c1f3ded94b9` | [candidate](evidence/phase-419-scheduler-intermediate-active-8-candidate-pair-2.json.gz) | `2789812dd23155af81d3d6a21260387b3befbaaefff91a0db87c4bf4cd6588b9` |
| 8 | 3 | [baseline](evidence/phase-419-scheduler-intermediate-active-8-baseline-pair-3.json.gz) | `b5786ca1d5339c19d01b60529018b20f2dd42928570592cd2be90b305742a81e` | [candidate](evidence/phase-419-scheduler-intermediate-active-8-candidate-pair-3.json.gz) | `48cdc384f60451a8ac590727535767bcc604e421839cd7bc88ead7e3bb2dff03` |

### Released-baseline scheduler comparison (2026-11-01–02)

Three rotating pairs compared the published `sparkless-server-bin==4.18.0` executable (the 4.18.1 API baseline) with the 4.19 scheduler candidate. The baseline macOS arm64 wheel SHA-256 is `ea4e2a7eb0ae8605e999d2f4a45bdefcb6980cb538724feefc24a5cbc2a07dfa`; its bundled server executable SHA-256 is `1d79128ea8f09c40d980a5e8907b4a967f909b4c2e38ef58097daee0e0b91b13`. The candidate release executable reports 4.19.0, was built with `--no-default-features --features http,connect` from source revision `43ce29b2c31a3da8d175a91cc350d09a76e8eeb4`, and has SHA-256 `c604f85ab4856dbe3717c930ff3a47b3119b2b00f6a98a54ef9263889779f745`. Subsequent runtime changes on the branch were confined to DuckDB-source code; release packaging, workflow, documentation, and benchmark evidence also changed. The Connect scheduler and exporter paths in that binary match the final candidate.

Both executables ran on the same Apple M4 Pro (12 logical CPUs), macOS 26.5.2 arm64 host, with CPython 3.11.15, `pyspark-client==4.0.5`, PyArrow 26.0.0, Rust 1.93.1, and UTC. The harness revision was `fa9cbd9c000c638f6a60a14e76aa9255283f3f00`; its benchmark-only dirty source digest was `e4f26ee259c24b0e9405f2a9ecf455203dd8510cd087b5d0374466605b6f3680`. Each server completed 60-second mixed-workload cohorts at 1/4/8/16 clients, with active limits 2/4/8, a one-million-row filtered count, 100,000-row `range(...).select("id").collect()` operations, a concurrent one-million-row slow reader, health and HTTP probes, disconnect/replay fault injection, and server CPU/RSS and Connect phase sampling. Pair order was baseline/candidate, candidate/baseline, baseline/candidate at each active limit. Queue and HTTP probe timeouts were both 30 seconds.

To keep the 16-client released-baseline cohort measurable, both binaries received the same finite benchmark-only operation-history limits: 32,768 records and 128 MiB. The 4.18.0 default history otherwise exhausted during that cohort. The production defaults were not changed. Each of the 18 runs exited successfully with zero health/HTTP probe errors; each slow reader completed one million rows; replay recorded one producer and one reattach RPC; and shutdown was graceful.

Values below are medians across the three fresh pairs. Throughput counts correct operations; latency is the short-job nearest-rank percentile; RSS is sampled server peak RSS for that client cohort.

| Active limit | Clients | Correct ops/s, baseline → candidate | Short-job p95 ms, baseline → candidate | Short-job p99 ms, baseline → candidate | Peak RSS MiB, baseline → candidate |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 2 | 1 | 3.37 → 21.03 | 19.9 → 4.6 | 20.1 → 4.7 | 75.4 → 46.9 |
| 2 | 4 | 4.31 → 79.28 | 1,927.3 → 4.8 | 3,065.0 → 5.1 | 78.6 → 50.6 |
| 2 | 8 | 4.34 → 143.59 | 3,805.9 → 6.0 | 6,045.7 → 7.2 | 79.0 → 53.1 |
| 2 | 16 | 4.35 → 195.23 | 7,716.8 → 29.5 | 10,108.0 → 51.7 | 80.0 → 56.2 |
| 4 | 1 | 3.36 → 21.31 | 19.7 → 4.5 | 20.1 → 4.7 | 75.8 → 46.9 |
| 4 | 4 | 5.45 → 79.13 | 306.5 → 4.8 | 2,126.0 → 5.1 | 93.5 → 51.6 |
| 4 | 8 | 5.45 → 143.08 | 2,546.1 → 6.0 | 2,688.3 → 6.9 | 134.0 → 54.0 |
| 4 | 16 | 5.40 → 197.04 | 5,373.4 → 28.6 | 7,962.5 → 54.3 | 144.2 → 59.5 |
| 8 | 1 | 3.34 → 21.35 | 19.7 → 4.4 | 19.8 → 4.6 | 77.2 → 47.3 |
| 8 | 4 | 5.49 → 79.14 | 20.2 → 4.8 | 20.4 → 5.2 | 105.1 → 51.4 |
| 8 | 8 | 5.81 → 145.64 | 514.9 → 5.6 | 3,221.8 → 6.5 | 152.5 → 55.3 |
| 8 | 16 | 5.89 → 201.75 | 4,853.4 → 24.8 | 4,896.0 → 46.2 | 160.5 → 59.4 |

At the selected active-2/16-client floor, all three candidate pairs exceeded 100 correct ops/s and stayed below 220 ms p95, 240 ms p99, and 64 MiB cohort peak RSS. The medians were 195.23 ops/s, 29.5 ms p95, 51.7 ms p99, and 56.2 MiB. Candidate server RSS remained below 64 MiB in every cohort and slow-reader sample. For the active-2/16-client candidate, median phase p95 values were 1.728 ms admission wait, 0.366 ms planning, 12.483 ms Arrow-next, 0.059 ms replay accounting, and 0 ms blocked-send. The published 4.18.0 binary predates these phase counters; its missing values are unavailable, not measured zero. This closes the released-baseline scheduler comparison at the selected floor; the independent intermediate-candidate comparison above remains part of the acceptance record.

The 18 deterministic gzip archives retain per-operation samples, environment, settings, phases, and binary hashes. SHA-256 values below are for the compressed report files.

| Active limit | Pair | Baseline report | SHA-256 | Candidate report | SHA-256 |
| ---: | ---: | --- | --- | --- | --- |
| 2 | 1 | [baseline](evidence/phase-419-scheduler-released-active-2-baseline-pair-1.json.gz) | `f8068a01671e3c5d2ccfaeb01e903e68735d05df8a137eb1c336d38d45c53006` | [candidate](evidence/phase-419-scheduler-released-active-2-candidate-pair-1.json.gz) | `436c0a174c6a67672ad644716c0cd40fd9f55d176efeb24eab40064e1f61e7e2` |
| 2 | 2 | [baseline](evidence/phase-419-scheduler-released-active-2-baseline-pair-2.json.gz) | `8d358e4378d8f325d25c6a12c4e28c5dcd815252b435d5589a7d9ebfd26b67ed` | [candidate](evidence/phase-419-scheduler-released-active-2-candidate-pair-2.json.gz) | `9ecfda8090e171a82bf90da02ab2ddbf2d21a8aa1579f9ad9e87cd61ea1b98d7` |
| 2 | 3 | [baseline](evidence/phase-419-scheduler-released-active-2-baseline-pair-3.json.gz) | `aa59ab9e41d8982e693571e85aca0ee5d2db1208e4c52b1ced47d01709e97c99` | [candidate](evidence/phase-419-scheduler-released-active-2-candidate-pair-3.json.gz) | `85dea44e0a33e306a639be4e2c9f5cdf0345c17bad0317ba962071a8068d24f0` |
| 4 | 1 | [baseline](evidence/phase-419-scheduler-released-active-4-baseline-pair-1.json.gz) | `03ef538aec6d4db13f066b2397622d1f8e11c201435a79bd3d3940f3ff2f4fad` | [candidate](evidence/phase-419-scheduler-released-active-4-candidate-pair-1.json.gz) | `a201103467ac3adae5e85cef546943be5cec34794deed784eb0d892586db7b1b` |
| 4 | 2 | [baseline](evidence/phase-419-scheduler-released-active-4-baseline-pair-2.json.gz) | `51ffb905c9beedc7db978a3b41ea00f36440bdae483571caca74388b8584e2a0` | [candidate](evidence/phase-419-scheduler-released-active-4-candidate-pair-2.json.gz) | `3c1043dcb66c242cd5e8b2609afb3376dbe65fcc0dedb16192dcbb00457026d3` |
| 4 | 3 | [baseline](evidence/phase-419-scheduler-released-active-4-baseline-pair-3.json.gz) | `cd40999f0542c06f0cd8f1827fbb884aa3b1ae30fb817807655ca3b25eb758a0` | [candidate](evidence/phase-419-scheduler-released-active-4-candidate-pair-3.json.gz) | `dbee3f9a707bee9667b4c5ef33882b2df65bc83fe7bf9714684b8853564841ae` |
| 8 | 1 | [baseline](evidence/phase-419-scheduler-released-active-8-baseline-pair-1.json.gz) | `411e67a3180e35364cbe1c523bb65ef8685cbd383750dd40259fd4a2a59d6e9e` | [candidate](evidence/phase-419-scheduler-released-active-8-candidate-pair-1.json.gz) | `3bc4ce0803daef006917a47e331c7fc3c0bf2659a9a1dc1630c9d220ad4798f7` |
| 8 | 2 | [baseline](evidence/phase-419-scheduler-released-active-8-baseline-pair-2.json.gz) | `79833abe6e61116979a700d06d7d460c3e21c460f943e85c544ad47c55da54ad` | [candidate](evidence/phase-419-scheduler-released-active-8-candidate-pair-2.json.gz) | `8709fa647598e928ad95fdcd57e4ac70daba20c45985bed41a72d56695e4fcd3` |
| 8 | 3 | [baseline](evidence/phase-419-scheduler-released-active-8-baseline-pair-3.json.gz) | `8486d421f8c99f1b8010805babd23a43dc51753e8ceed20df02235694af5f115` | [candidate](evidence/phase-419-scheduler-released-active-8-candidate-pair-3.json.gz) | `32234a1fef3fbccc793467dea244cb734113338b431fa88b4c99e5ee97775922` |

### Optional Rust allocator trace witness (2026-11-02)

One unscored active-2 witness used the opt-in `allocator-trace` release binary (4.19.0, SHA-256 `ec88206c3ea80db2ab3ece9b53a475fb94f82dbdb1858d4e40bf105fe9394e18`) from harness revision `fa9cbd9c000c638f6a60a14e76aa9255283f3f00`; the dirty-source digest was `3453e69ba5d251b13ad5f7c979d1e3ea7badead51f07f013b1401a38aeec3f10`. It ran four 60-second cohorts at 1/4/8/16 clients on the same macOS arm64 host and with the same finite benchmark-only history limits. All four cohorts had zero health/HTTP probe errors, the one-million-row slow reader completed, replay recorded one producer and one reattach, and shutdown was graceful.

The shutdown snapshot reported 12,690,847 live Rust-global-allocator bytes and 19,746,917 peak live bytes (about 12.1 and 18.8 MiB); cumulative allocation accounting was 173,041,014,343 bytes over the full process run. The cumulative counter includes successful Rust `alloc`/`alloc_zeroed` sizes and positive `realloc` growth deltas. The largest sampled cohort RSS was 55,083,008 bytes (52.5 MiB). These counters cover Rust allocations only; DuckDB and other direct native C/C++ allocations are excluded, and the single run does not establish portable per-operation memory limits. The complete report is [phase-419-allocator-trace-active-2.json.gz](evidence/phase-419-allocator-trace-active-2.json.gz), SHA-256 `4f91ca5c33dbe20c3ae58a01246a6584055ef4479b27e02ac3077c1b405f0d9f`.

### Small-job latency guardrails and Arrow buffer instrumentation (2026-11-01)

Three rotating release pairs measured the public-client small-job cases at 20 timed iterations
after one warmup per runtime. Each of the three fresh Python runtimes per side also recorded 20
fresh server/readiness/session starts. Pair order was baseline/candidate, candidate/baseline,
baseline/candidate. The harness source is `bc068ff4f6e4f1ce92a489f4547fce645f2bee0e` and uses the
same `pyspark-client==4.0.5`, CPython 3.11.15, and PyArrow 26.0.0 on both sides, with local
`SparklessConnect` launcher code from that source. The host was an Apple M4 Pro, macOS 26.5.2
arm64.

The published baseline executable is the `v4.18.1` server build from source
`0ad225677ec47b20c9c6225fa9f4b73b00e54aba`, reports server version 4.18.0, and has SHA-256
`4bcccfb2d838e3dab57ee91f2f6ee5735fb9b8c03d13361b04dd792f68c19aae`. The release candidate is
the `http,connect` release executable built from source `43f43589` (full source revision
`43f43589f0124fe250327bea4f5af61745406aaa`), reports version 4.19.0, and has SHA-256
`207dac093924892e9a08b8d23e2b685faeea2812e2ec2df97e2888a471aebef6`. The baseline build uses
the `connect` feature with default `http`; the candidate explicitly enables both, so the effective
server feature sets match. Both used the same Python API helper source, native package, and
Connect client dependencies; the installed server-binary version metadata matched the executable
on each side for the launcher's compatibility check.

The table pools 60 timed actions per side and uses nearest-rank p50/p95. The acceptance limits
are a median regression no greater than `max(5% of baseline, 1 ms)` and p95 regression no greater
than `max(10% of baseline, 2 ms)`. Every action stayed within those limits; all but schema analysis
improved by several milliseconds, while schema analysis was effectively unchanged. Startup p50
increased by 0.313 ms, below the 1 ms absolute allowance, and startup p95 improved. Startup p95
uses 60 fresh starts and is descriptive rather than a high-confidence extreme-tail estimate.

| Workload | Baseline p50 / p95 (ms) | Candidate p50 / p95 (ms) |
| --- | ---: | ---: |
| Tuple ingest/filter/count, 10 rows | 15.933 / 18.610 | 0.833 / 1.085 |
| Tuple ingest/filter/count, 100 rows | 16.643 / 18.852 | 0.779 / 1.017 |
| Tuple ingest/filter/count, 1,000 rows | 16.807 / 19.363 | 1.009 / 1.165 |
| Tuple ingest/filter/count, 10,000 rows | 18.487 / 23.239 | 3.298 / 3.412 |
| Nullable preview, 25 of 1,000 rows | 17.418 / 19.644 | 0.952 / 1.145 |
| Nullable/mixed collect, 256 rows | 19.283 / 20.758 | 1.012 / 1.187 |
| Range schema analysis, 1,000 rows | 0.156 / 0.328 | 0.125 / 0.193 |
| Fresh server/readiness/session start | 8.437 / 10.740 | 8.750 / 10.548 |

Client-process peak RSS ranged from 133.5–134.7 MiB on the baseline and 133.5–133.7 MiB on the
candidate. This harness does not sample server RSS or server CPU and does not measure first-batch
or cancellation latency; those resource and lifecycle comparisons remain separate acceptance
work.

The six compressed reports preserve each fresh runtime and its action/startup samples:

| Pair | Baseline report | SHA-256 | Candidate report | SHA-256 |
| ---: | --- | --- | --- | --- |
| 1 | [baseline](evidence/phase-419-small-jobs-baseline-pair-1.json.gz) | `10ea515ab50ae02887cc292205d1842e178436b90794a25f4f59d5eb3fd93737` | [candidate](evidence/phase-419-small-jobs-candidate-pair-1.json.gz) | `89298dc6e7163d27f1b8d0e673121d0cb334d5e3045fd9bb08f9b363d77aaff3` |
| 2 | [baseline](evidence/phase-419-small-jobs-baseline-pair-2.json.gz) | `ac37e53f35c68e5e02969b75f4377eb8f4d5f019a7a379092e2f236ca25a6506` | [candidate](evidence/phase-419-small-jobs-candidate-pair-2.json.gz) | `47cf50a5f3a547df9f63b05ffc6b72a9988f626bd44741f7d9e8de110bdb63a0` |
| 3 | [baseline](evidence/phase-419-small-jobs-baseline-pair-3.json.gz) | `7961112f99ac7cb2982670ca81c8c738306119dfc70c50807bd0836990a7dc4c` | [candidate](evidence/phase-419-small-jobs-candidate-pair-3.json.gz) | `73528d8b14a3889e9224a4787a052ea320e4d8955efb51f08a8d2706490ba679` |

The exporter now emits opt-in `SPARKLESS_ARROW_TRACE=1` records split into source size, buffered
frame size estimates, IPC buffer sizes, rechunk/IPC-write/schema-rewrite/combine time, and
serialized versus emitted bytes. In the single measured `range(100_000).select("id")` trace,
both `collect()` records processed 100,000 rows in 391 source chunks and seven output batches.
The largest source batch estimate was 2,048 bytes; peak buffered and combined frame estimates
were 131,072 bytes; the largest serialized IPC batch was 131,344 bytes; and the estimated raw plus
rewritten schema buffer high-water was 262,688 bytes. Each stream serialized and emitted 801,904
IPC bytes. Across the warmup and timed `collect()` traces, serialization took 0.095–0.112 ms,
including 0.021–0.023 ms rechunking, 0.053–0.065 ms IPC writing, and 0.020–0.022 ms schema
rewriting. These are reported buffer-length/data-size estimates, not allocator-wide accounting or
server RSS measurements. The full one-iteration trace report is
[phase-419-arrow-buffer-trace.json.gz](evidence/phase-419-arrow-buffer-trace.json.gz), SHA-256
`f407eb57c61d63f74c40ca311ce0c9065ea144c7c22e53126c278a9c8fcf4285`.

The trace implementation passed `cargo check --locked --release -p sparkless-server
--no-default-features --features http,connect` and the matching release build at source
`43f43589f0124fe250327bea4f5af61745406aaa`. The bounded IPC regressions passed with
`cargo test --locked --release -p robin-sparkless-polars --features sql arrow_ipc_batches_`
(3 passed). `test_slice8_harness.py` passed all 7 parser/harness tests, the benchmark scripts passed
Ruff and Python compilation, `scripts/check_documentation.py` checked all 187 documentation files,
and `smoke_phase419_client.py` passed against the same 4.19 release server.

### Small-job server resource and cancellation comparison (2026-11-01)

After the response-acknowledgment and cancellation changes at source `43ce29b2c31a3da8d175a91cc350d09a76e8eeb4`, three rotating release pairs measured 20 timed actions per case and 20 fresh readiness/session starts per runtime. Each side therefore has 60 timed samples per case and 60 startup samples. Pair order was baseline/candidate, candidate/baseline, baseline/candidate. Both sides used the same local Python helper, `pyspark-client==4.0.5`, CPython 3.11.15, PyArrow 26.0.0, and Apple M4 Pro / macOS 26.5.2 arm64. The baseline is the released `v4.18.1` server, version `4.18.0`, release profile, effective `http,connect` features, and binary SHA-256 `4bcccfb2d838e3dab57ee91f2f6ee5735fb9b8c03d13361b04dd792f68c19aae`. The candidate is the clean 4.19.0 release binary with `http,connect`, built at the source revision above, SHA-256 `c604f85ab4856dbe3717c930ff3a47b3119b2b00f6a98a54ef9263889779f745`.

Pooled nearest-rank action and startup percentiles are:

| Workload | Baseline p50 / p95 (ms) | Candidate p50 / p95 (ms) |
| --- | ---: | ---: |
| Tuple ingest/filter/count, 10 rows | 16.476 / 16.856 | 1.772 / 2.115 |
| Tuple ingest/filter/count, 100 rows | 16.467 / 16.874 | 1.725 / 2.005 |
| Tuple ingest/filter/count, 1,000 rows | 16.768 / 16.955 | 1.987 / 2.340 |
| Tuple ingest/filter/count, 10,000 rows | 19.233 / 19.573 | 4.484 / 4.976 |
| Nullable preview, 25 of 1,000 rows | 16.783 / 17.304 | 1.953 / 2.152 |
| Nullable/mixed collect, 256 rows | 18.891 / 19.361 | 2.024 / 2.484 |
| Range schema analysis, 1,000 rows | 0.489 / 0.650 | 0.524 / 0.682 |
| Range(10M) first-batch-and-cancel action | 168.205 / 175.354 | 21.319 / 23.301 |
| Fresh server/readiness/session start | 8.526 / 9.329 | 8.701 / 9.363 |

The cancellation case consumed exactly one 16,384-row batch on both sides. Candidate first-batch
latency was 18.802/20.504 ms p50/p95 versus 165.616/172.766 ms for the baseline. The interrupt
RPC took 0.291/0.359 ms versus 0.299/0.383 ms; after that RPC, cancellation surfaced to the client
in 1.999/2.393 ms versus 2.043/2.373 ms. Across the three sampled server processes, peak observed
RSS was 47.36 MiB for the candidate and 58.42 MiB for the baseline. Summed `ps` process-CPU deltas
over these workloads were 0.37 s and 10.39 s respectively; macOS `ps` reports CPU time at coarse
resolution, so these totals are descriptive samples rather than allocator-wide or per-operation
CPU accounting. The server CPU/RSS/first-batch/cancellation comparison is now recorded for this
host and workload set; it does not establish cross-platform limits or close allocator-wide
accounting.

The six compressed reports preserve raw action, startup, cancellation, server-process, and
environment samples:

| Pair | Baseline report | SHA-256 | Candidate report | SHA-256 |
| ---: | --- | --- | --- | --- |
| 1 | [baseline](evidence/phase-419-small-jobs-cancel-ack-baseline-pair-1.json.gz) | `ff05f805a1903054ca70da2c6fd320326f4e05a82a7b812855dc7fd3584046da` | [candidate](evidence/phase-419-small-jobs-cancel-ack-candidate-pair-1.json.gz) | `bfc9008abbea465b9123deb9c539eb92d901d1e3dbb1a1845897080ea010a4d7` |
| 2 | [baseline](evidence/phase-419-small-jobs-cancel-ack-baseline-pair-2.json.gz) | `7a50db2c8f8334b0f37f96118cac033230c0962fbe62e0fb8cb1f05dcaca09f1` | [candidate](evidence/phase-419-small-jobs-cancel-ack-candidate-pair-2.json.gz) | `002dc79ddcd358ea6ce5fa79cdf16635b5e6ba4b8f79145db16b3805e9d3ff94` |
| 3 | [baseline](evidence/phase-419-small-jobs-cancel-ack-baseline-pair-3.json.gz) | `9d4bf2274f668c61c523087e3a9cec0b3246cadf98b537bd5a32b3b6a8a0d094` | [candidate](evidence/phase-419-small-jobs-cancel-ack-candidate-pair-3.json.gz) | `270e63c9888d2b83eae1446b40504d2634d5573ba7e5e0b70fd94159bed06ba4` |

### Four-worker pytest Connect comparison (2026-11-02)

The retained runner `tests/connect/benchmark_phase419_xdist.py` launches one shared Connect
server per engine, then runs three rotating baseline/candidate pairs. Each server handles one
warmup suite and one timed suite. Four real `pytest-xdist` workers each create and stop their own
session on that server. The 38-case suite validates four jobs from #1738's described mix: 10
1,000-row local-relation ingest/filter/count cases, 10 nullable local-relation collections, 10
25-row previews, and 8 10,000-row range collections. All results are checked. Each engine has 228
passing executions; 456 executions passed in total.

Both sides used `pyspark-client==4.0.5`, PyArrow 26.0.0, pytest 9.2.0, pytest-xdist 3.8.0, and
CPython 3.11.15 on an Apple M4 Pro (12 logical CPUs), macOS 26.5.2 arm64. The JVM service is Spark
Connect 4.0.5 with `local[4]`, FAIR scheduling, four shuffle/default partitions, and
`ActiveProcessorCount=4`. Sparkless used a 4.19.0 release-profile server with `http,connect,
duckdb-source`, four active operations, and four-thread Polars/Rayon/OpenMP pools. The Sparkless
binary came from release workflow [41362360697](https://github.com/eddiethedean/robin-sparkless/actions/runs/41362360697),
SHA-256 `eb1fec83f8018f311b85a307ef9b95f30f91ee9ee5f1fed4b18fa145ac2db4f0`; its Connect
scheduler/exporter paths match the final candidate. The Spark distribution archive SHA-256 is
`a0d44b0e7faf0458ad83b7aa664e7ed8b96beed8a55d66ce3b0dfac2309dfca6`, and the Connect JAR SHA-256
is `f762b877f468efbcc2302f62bac2426fd3e0f67ea7d469952efe465bf5587192`.

| Metric | Sparkless 4.19 | JVM Spark Connect 4.0.5 |
| --- | ---: | ---: |
| Timed suite p50 / p95 (3 samples) | 726.4 / 727.8 ms | 1,067.9 / 1,092.0 ms |
| Server start + readiness action + first suite p50 (3 samples) | 833.0 ms | 4,541.9 ms |

These three-run percentiles describe this checked-in four-job suite only. The earlier 2.699 s
Sparkless and 1.378 s JVM warm-suite figures in the #1738 issue body refer to a 38-test suite, but
the issue does not retain that suite's case list or source revision. This comparison therefore
provides fresh shared-server evidence for the described four-job mix without claiming to reproduce
those earlier figures. The JVM server process was stopped with SIGINT after each pair; the raw
report retains its exit status and server-log digest.

The deterministic compressed report is
[phase-419-four-worker-pytest-macos.json.gz](evidence/phase-419-four-worker-pytest-macos.json.gz),
SHA-256 `aec45e42cc838634e3d34dfe213e5758c1243b732ffd7b58f48b6430ff0cbd12`. It contains all
six suite runs, per-test JUnit durations/statuses, startup/readiness timings, rotation, client and
server artifact hashes, and exact configuration. The benchmark source revision was
`936645fd088c46056a7a3e834838964152d94fc5`; the script SHA-256 is
`84a3f6fc337006094f32c00d6c5e048c9b65c3af81edb8a6a2ebac7a04bc4205`.

## Exact-commit Linux and macOS source-extra resource checks (2026-11-01)

Release workflow run [41351811918](https://github.com/eddiethedean/robin-sparkless/actions/runs/41351811918) completed successfully at branch commit `7361d16850f8e76a84d70b4a88c6ee6c39da7b58`: 33 jobs passed and five publication jobs were skipped for the branch run. The `Server extra (ubuntu-24.04)` and `Server extra (macos-15)` jobs installed the platform-matched 4.19.0 server wheels, passed standalone and Quack public-client source smokes, and completed the four-reader slow-consumer case. The native server container build and smoke and the release validation acceptance gate also passed.

Each resource run used the release-profile server with `http,connect,duckdb-source`, `pyspark-client==4.0.5`, PyArrow 26.0.0, four concurrent readers, 100,000 rows per reader, 16-byte payloads, and a 10 ms slow-reader delay. Each platform had one warmup round and one measured round. All four measured readers returned 100,000 rows, with zero result-limit rejections. The four action samples are shown as nearest-rank p50/p95; with four samples the p95 is descriptive. RSS is the server child peak after exit.

| Platform | Server binary SHA-256 | Action p50 / p95 (ms) | Server child peak RSS | Raw report SHA-256 |
| --- | --- | ---: | ---: | --- |
| Linux x86_64, Ubuntu 24.04 | `7d4df36b6af25f733c0ce96882d24c2fbc1d3f21b1e2b022cbf8c6170b397d35` | 3,113.5 / 3,213.5 | 192.9 MiB | [phase419-source-resource-manylinux-x86_64.json](evidence/phase419-source-resource-manylinux-x86_64.json), `ed70ed4143eefe2cfbf596d518b86329d5e17f9cc59a92b4d808af1261eb09c3` |
| macOS arm64, macOS 15 | `eb1fec83f8018f311b85a307ef9b95f30f91ee9ee5f1fed4b18fa145ac2db4f0` | 1,942.1 / 1,979.2 | 137.8 MiB | [phase419-source-resource-macos-aarch64.json](evidence/phase419-source-resource-macos-aarch64.json), `3979de0c692b24761cb22b904189be2814d4a7799fa2b6fbe7b562f43dcfe18e` |

The enforced source limits were 1,000,000 rows, 32 MiB IPC, and 64 MiB decoded data per statement; 16,384 rows and 1 MiB IPC per batch; four source connections and sixteen queued source operations per catalog. The server Connect result cap was 8 MiB, with four active operations and sixteen pending operations. The workload used a 1,323,008-byte DuckDB source file and produced 2.4 MB of logical column data per reader (9.6 MB across the four readers).

This closes the exact-commit packaged Linux/macOS source-smoke and single-cohort resource-run item. It is one measured round per platform, not a stable tail estimate or a cross-platform hard RSS limit; broader source/resource coverage remains open. These results do not close D14 or V35/V36 by themselves.

## Local macOS source-build and packaged-wheel checks (2026-11-02)

To keep iteration local until release evidence is needed, release workflow
[41371241798](https://github.com/eddiethedean/robin-sparkless/actions/runs/41371241798) was
canceled during its two native compile jobs and produced no source-resource artifacts. Local
checks included a fresh local release build from current branch revision
`22034d3014163c72c0511d0363d3388587412ecb`, using Rust 1.93.1 and
the command:

```sh
CARGO_BUILD_JOBS=4 CMAKE_BUILD_PARALLEL_LEVEL=4 \
  cargo build --locked --release -p sparkless-server \
  --no-default-features --features http,connect,duckdb-source
```

The binary reports
`sparkless-server 4.19.0`; its SHA-256 is
`81a72f39a383c01ccf96f56136fd560cb62d27b08dc1f70feeb1f0ab2fc708b0`. The Python harness ran at
the same revision, SHA-256
`41c5f83f7d3641efb5b8c077b30d6d3b4316fe2f214edae4b1d18e7663fd9344`.

The host was an Apple M4 Pro, macOS 26.5.2 arm64, with CPython 3.11.14,
`pyspark-client==4.0.5`, PyArrow 26.0.0, and DuckDB 1.5.1. Both runs used four readers, four active
operations, four-thread Polars/Rayon/OpenMP pools, 16-byte payloads, a 10 ms slow-reader delay,
one warmup round, and one measured round. Every measured reader returned all rows and no reader
hit the 8 MiB result cap. The API and native wheels were installed from artifacts in workflow
[41362360697](https://github.com/eddiethedean/robin-sparkless/actions/runs/41362360697), and the
Quack 2.0.0 extension and independent owner fixture came from its successful macOS wheel job. The
server-wheel verifier passed HTTP, Connect-client, and DuckDB-source checks on that packaged wheel;
the public source smoke also passed with the current local server build for standalone and Quack
shared-owner modes.

| Rows per reader | Source file | Logical data per reader / across four | Action p50 / p95 (ms) | Server peak RSS | Raw report SHA-256 |
| ---: | ---: | ---: | ---: | ---: | --- |
| 100,000 | 1,323,008 bytes | 2.4 / 9.6 MB | 1,023.9 / 1,058.9 | 141,524,992 bytes (135.0 MiB) | [report](evidence/phase419-source-resource-local-macos-aarch64-100k.json.gz), `b2cb3d9aa548672c60e275e621fa0d627ce6cca10f5236c6bd71b9672a89eb1d` |
| 350,000 | 3,682,304 bytes | 8.4 / 33.6 MB | 3,496.5 / 3,577.4 | 204,505,088 bytes (195.0 MiB) | [report](evidence/phase419-source-resource-local-macos-aarch64-350k.json.gz), `8ff77608d96df491ed9c98015a0abda5cdb4e58af1094d6fba30ab50cb5ef972` |

Each case used one warmup and three measured rounds, for 12 measured reader actions. Nearest-rank
p95 is the maximum of those 12 samples. Both local cohorts remain within the provisional 256 MiB
macOS guardrail, but one fresh server per size does not establish stable tails or a portable RSS
bound. `tests/connect/smoke_phase419_duckdb_client.py` passed locally for both the standalone source
and the separate-process Quack shared owner. `scripts/verify_server_bin_wheel.py` also passed against
the packaged wheel with HTTP, Connect-client, and DuckDB-source checks enabled.

Three current-source boundary runs used four readers, 1,000,000 rows per reader, one warmup, and
one measured round. Active limit 1 used a 60-second queue timeout; limits 2 and 4 used 30 seconds.
All four readers returned one million rows with zero result-cap rejections in all three runs. The
server binary SHA-256 was
`81a72f39a383c01ccf96f56136fd560cb62d27b08dc1f70feeb1f0ab2fc708b0`.

| Active operations | Action p50 / p95 (ms) | Server peak RSS | Raw report SHA-256 |
| ---: | ---: | ---: | --- |
| 1 | 9,957.3 / 10,019.4 | 263,782,400 bytes (251.6 MiB) | [report](evidence/phase419-source-resource-local-macos-aarch64-1m-active1.json.gz), `9c3f36b41c9329a30730c4c20eb4591e79bb0fffaffa564e1e67c594fa9634e8` |
| 2 | 9,709.8 / 9,973.3 | 277,610,496 bytes (264.8 MiB) | [report](evidence/phase419-source-resource-local-macos-aarch64-1m-active2.json.gz), `d77a72e6516bca68f2dc470f4c4e725b54fe99e0fc0c9265d7dd9d838f4f1c8c` |
| 4 | 9,741.7 / 9,912.4 | 325,484,544 bytes (310.4 MiB) | [report](evidence/phase419-source-resource-local-macos-aarch64-1m-active4.json.gz), `5e5eade8add148f2303f263b48697b4c423067d327030cf636f92fa42b88c2d5` |

Each p95 is descriptive, with four reader actions in one measured round. Active limit 1 remained
4.4 MiB below the provisional guardrail; active limits 2 and 4 exceeded it by 8.8 and 54.4 MiB.
These initial single-round observations confirm the concurrency boundary. The repeated current-source
results below supersede their latency and RSS estimates.

### Repeated current-source one-million-row slow-reader runs (2026-11-01)

Each active-operation setting was measured across three fresh server processes. The first process
ran one warmup and three measured rounds; each of the next two processes ran one warmup and one
measured round. Each round ran four independent Connect readers against the same one-million-row,
16-byte-payload DuckDB table while delaying `toLocalIterator()` by 10 ms after each row group. This
produced twenty measured reader actions per setting. All readers returned one million rows in every
round, with zero 8 MiB result-cap rejections. The server used four-thread Polars/Rayon/OpenMP pools;
the client used CPython 3.11.14, `pyspark-client==4.0.5`, PyArrow 26.0.0, and DuckDB 1.5.1 on an Apple
M4 Pro / macOS 26.5.2 arm64. The local release-profile server reports 4.19.0, has SHA-256
`81a72f39a383c01ccf96f56136fd560cb62d27b08dc1f70feeb1f0ab2fc708b0`, and was built at source
`22034d3014163c72c0511d0363d3388587412ecb`. The benchmark harness revision was
`65fec52fb53c5d685a2f010dfbba407d325932b9`.

Action p50/p95 use nearest-rank percentiles over the twenty measured reader actions. Peak RSS is
`resource.getrusage(RUSAGE_CHILDREN)` for each server child after exit; the table shows each
setting's measured range and maximum.

| Active operations | Action p50 / p95 (ms) | Server peak RSS range (max) | Reports (max RSS report first) |
| ---: | ---: | ---: | --- |
| 1 | 10,072.1 / 10,181.2 | 247.7–257.8 MiB (270,368,768 bytes) | [three-process report 1](evidence/phase419-source-resource-local-macos-aarch64-1m-active1-3rounds.json.gz), SHA-256 `f1943a26f633e5879174b0836a43f4a89ee19f1a6f506cd2b7f8c862a871852e`; [2](evidence/phase419-source-resource-local-macos-aarch64-1m-active1-fresh2.json.gz), `c1966af42c2c5643b9acb79a9205f0899002b84bbbd8a12ff7e6285a6931233d`; [3](evidence/phase419-source-resource-local-macos-aarch64-1m-active1-fresh3.json.gz), `3c57e3a59859e344337348f6798deb47358f6cb04064071c2b4d8b5b9b38d92f` |
| 2 | 9,910.3 / 10,034.2 | 264.8–281.6 MiB (295,305,216 bytes) | [three-process report 1](evidence/phase419-source-resource-local-macos-aarch64-1m-active2-3rounds.json.gz), SHA-256 `75e70ff1e7078acf86a18d0dd2b44df5fd08d361acf3c21d26926d674f9cbe9f`; [2](evidence/phase419-source-resource-local-macos-aarch64-1m-active2-fresh2.json.gz), `403f7aefa9e5bec4697cb563c1c2b8893b92b54614c3d691345992eb58708413`; [3](evidence/phase419-source-resource-local-macos-aarch64-1m-active2-fresh3.json.gz), `c3e5156064ef0b7d6bdfdc20932e3b8dcafccb4acf009d2bbcdbdaaeaee35859` |
| 4 | 9,979.5 / 10,070.5 | 299.0–325.5 MiB (341,278,720 bytes) | [three-process report 1](evidence/phase419-source-resource-local-macos-aarch64-1m-active4-3rounds.json.gz), SHA-256 `f89c543663fe0430c0bf7cf2200840a8d3dc72976990f928ce296d4fb0615848`; [2](evidence/phase419-source-resource-local-macos-aarch64-1m-active4-fresh2.json.gz), `013236de24f2e94bf6a3c895723acdfd6853893e90cfa2b2363680d556322591`; [3](evidence/phase419-source-resource-local-macos-aarch64-1m-active4-fresh3.json.gz), `cbf9b572ff4d03fb3574bcd0cc31d6e033f7914e3ee1db8a552ac1df72c79179` |

The measured 100k–350k guardrail remains 256 MiB. For the 1M-row, four-reader workload on this
macOS arm64 host, all three active settings fit under a separate provisional 384 MiB RSS guardrail;
the maximum observed peak was 325.5 MiB, leaving 58.5 MiB of measured headroom. The budget is a
workload/platform observation, not an enforced process ceiling or a portable guarantee. Linux and
other-host resource coverage remains open.

## Open acceptance gates

- The optional standalone and Quack source modes, a bounded four-connection/sixteen-waiter limiter, and focused source fixtures are present. Waiting acquires retain no source Arrow batches; statement snapshots own and close their connection rather than retaining it idle. The public Quack fixture proves a separate-process writer commit, owner loss between analysis/execution, recovery after owner restart, and one-producer disconnect/reattach replay; native and public Quack fixtures confirm remote cancellation after a batch and during graceful server shutdown. Standalone and Quack public smokes exercise unsupported types/values, the documented write-rejection forms, integer extrema, microsecond timestamps, and exact decimal boundaries through `DECIMAL(38,38)`. The declared table/join/group/aggregate subset has a 14-case Spark 4.0.5 differential report. The provisional 256 MiB observed-RSS guardrail covers four-reader 100k–350k slow-reader cohorts on macOS arm64; three measured current-source rounds at both 100k and 350k stayed below it. A previous branch workflow recorded one 100k-row round per platform; that historical result is not used as a current implementation gate. Repeated current-source 1M-row cohorts completed all readers at active limits 1/2/4 with 20 measured actions over three fresh server processes per setting and zero result-cap rejections; peak RSS ranged up to 257.8/281.6/325.5 MiB. A separate provisional 384 MiB macOS 1M-row guardrail contains every measured process peak, but is not enforced or portable. Local Linux source/resource acceptance and other-host coverage remain open. D14 and V35/V36 remain open.
- An earlier branch workflow's consumer jobs passed on Linux x86_64 and macOS arm64 and retained source-resource reports above. Each measured reader completed. These runs are historical context only and do not replace local current-candidate checks or establish a cross-platform RSS limit.
- The helper's 17 launcher tests and public smoke cover missing-client import failure, fork protection, startup/session/cleanup failures and retries, nested helpers, sequential reuse, unrelated active-session preservation, and public session creation. The four-mode clean-consumer install/import matrix passes on macOS arm64; the cross-platform server-extra source package smokes also pass, while broader helper/platform acceptance remains open.
- #1736–#1738 correctness and instrumentation work has landed. The original #1736 projected-range 2× latency target passes at 6.85× on the final source revision; the earlier exporter-only candidate measured 11.39× across three release pairs. Paired exporter/count reports, three-pair small-job action/startup guardrails and server CPU/RSS/first-batch/cancellation comparisons, a three-pair sender-only release A/B, and three-pair scheduler comparisons at active limits 2/4/8 against both the post-export/count intermediate candidate and the published 4.18.0 server are retained above. The optional Rust-global allocator witness records one process-wide trace, while excluding allocations made directly by DuckDB and other native C/C++ code. A new three-pair four-worker comparison covers the described four-job mix; the exact 38-test suite behind #1738's earlier timings is not retained, so reproducing that comparison remains open. Source-specific broader platform/resource coverage and the complete support acceptance gates remain open.
- Coordinated 4.19.0 API/native/server package versions and package build definitions are in place. Local current-candidate package and platform checks remain part of implementation acceptance. The earlier branch workflow succeeded with 33 passing jobs and five publication jobs skipped; it is retained as historical context and does not substitute for local checks of the current candidate. Release publication checks are deferred until the release-evidence stage.

Accordingly, this evidence record does not mark the 4.19 phase or its GitHub issues complete and does not authorize release publication.
