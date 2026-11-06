---
search:
  exclude: true
---

> **Archive / historical record:** this page describes a past snapshot or a plan.
> Use the [current support contract](../../SUPPORT_MATRIX.md) for release behavior.

# SS-15: Optional DuckDB execution backend for Spark Connect

> **Status: source implementation and local candidate evidence available; release gates remain open.** The pinned Spark 4.0.5 differential passes 44 result/schema cases and rejects 20 unsupported forms on macOS arm64. The original V38 matrix found admission saturation and a non-reattachable response-stall failure with a 5-second send watchdog. The candidate now allows 30 seconds for HTTP/2 send-credit replenishment and uses one serialized write/receipt connection plus a bounded pool of four read connections and 16 queued reads. In final release-profile cohorts, local and Quack owners completed paced one-million-row streams at eight clients/active-16 with no capacity rejections, clean health probes, and graceful shutdown. Quack small-count p99 fell from 27.75 seconds to 167 ms; throughput rose from 12.4 to 79.2 ops/s. An active-8 run rejects the ninth simultaneous operation at the configured compute queue timeout. Local package wheels also passed isolated install checks, but still carry 4.19.0 metadata. One local POSIX WAL-limit fault reopened with zero committed rows and zero receipts. V38 remains open for capacity/resource-budget acceptance, cross-platform and other partial-I/O evidence, full cleanup, and exact 4.20.0 artifacts.
> [Index](README.md) · [Roadmap](ROADMAP.md) · [Epic](EPIC.md) · [Decisions](DECISIONS.md) · [Validation](VALIDATION.md)

## Outcome and distinction from Phase 3

[4.19.0 shipped](../../RELEASE_EVIDENCE.md#4190-public-artifacts-2026-11-05-utc) the [separate DuckDB product](23-product-and-package-separation.md), with read-only DuckDB sources and Polars execution. Reuse its product, source driver, and launcher boundaries. D15 and V37/V38 govern the new execution mode. Use the 4.19.0 artifacts as the regression baseline and reconcile outstanding source acceptance against the exact artifacts used by 4.20.

Expose an optional DuckDB execution backend through Sparkless Server's existing Spark Connect endpoint. The same unmodified Connect client can submit the declared SQL/DataFrame subset, but DuckDB now executes the supported plans and owns persistent table writes. Phase 3 only supplied DuckDB table batches to Polars; this phase performs the query inside DuckDB.

Polars remains the default engine. Choose an execution engine explicitly when configuring a server instance, bind that choice for its sessions/operations, and expose a truthful capability matrix. Do not silently switch engines for unsupported operations or after a statement may have taken effect. Backend selection uses Sparkless server configuration and the standard Connect contract.

This is a native single-node backend, not Apache Spark or general Spark compatibility. The server and enabled image remain Python/JVM-free. Existing DuckDB data is usable only through the database-owner modes certified in Phase 3; a Connect URL does not grant safe access to another process's database files.

## Prerequisites and decisions

- Phase 2 SS-09/10 Connect translation, analysis, Arrow, replay, session, interruption, and release conformance.
- Phase 3 SS-14/D14 native DuckDB dependencies, ownership, names, snapshots, types, and measured resource behavior; V35/V36 completed for advertised shared data.
- The source implementation records the current D15 choices below. D15 is not considered evidence-frozen until the owner, client, reference, transaction, and resource decisions are exercised and their reports are linked. Phase 3 source artifacts remain a separate prerequisite and do not establish execution parity.

## Current implementation status

The candidate source now includes fixed-process backend selection, the DuckDB
execution owner, Connect request translation and analysis, bounded Arrow
streaming, typed `CREATE TABLE` and literal `INSERT ... VALUES` (including
explicit timestamp and decimal casts), transactional operation receipts,
launcher identity negotiation, and capability reporting.
It also includes source changes for 420-07–420-11 and #1746. These are code
implementation statements, not V37/V38 or performance results. No candidate
release artifact or public capability claim has been approved.

Implementation details and operator setup are summarized in the
[DuckDB execution candidate guide](../../DUCKDB_EXECUTION.md). The package and
server behavior remain documented as 4.19.0 until 4.20 artifacts pass the
acceptance gates below.

### Local source verification (2026-11-06)

On the local macOS arm64 workspace, these checks passed against the current
source candidate:

- DuckDB server and base Polars server compile with HTTP and Connect enabled.
- `cargo build --locked -p sparkless-server --no-default-features --features http,connect`: passed. The source launcher probe matrix passed for base default Polars, DuckDB-product default Polars, and DuckDB execution with a local owner. It also verified readiness/capabilities identity agreement and pre-readiness rejection for a wrong product, missing owner, and missing Connect listener. Binary hashes and cases are in [the 420-01 local report](evidence/phase-420-backend-startup-local.json), SHA-256 `3fe652cefc113e39eb4b6bcb220fe87bcb56b22f7acbce74e8e6cbe8f07f2dff`; the built base server SHA-256 is `dd2e96a267a048956ba98edb4040624f72472949a8bccebf1fc4227c86ca9e01`.
- `cargo test --locked -p robin-sparkless-duckdb`: 19 passed, one manual fault test ignored. Transaction tests cover commit and receipt replay after reopening the owner, rollback of earlier DDL/data when a later statement fails, reuse of an identity after the failed attempt, escaped receipt-key literals, eight concurrent retries applying one write, pre-admission cancellation with no data or receipt effect, and a test-triggered rollback that makes DuckDB return an actual error from the `COMMIT` command before allowing retry. Read-pool cleanup tests verify that a cancelled waiter releases its queue slot and that an invalidated checked-out connection closes on return. A separate lost-ack test commits successfully, injects a lost acknowledgement, verifies the committed row and receipt, and proves retry does not apply the write twice. The ignored `storage_write_failure_during_commit_returns_unknown` test was rerun alone against the current source with `SIGXFSZ` ignored and `RLIMIT_FSIZE=4 MiB`; DuckDB attempted to write a 32 MiB transaction WAL at COMMIT, received `File too large`, and the adapter returned an unknown-outcome error. Reopening after this injected fault found zero rows, zero receipts, and no partial payload. See the [local fault report](evidence/phase-420-duckdb-storage-commit-fault-local.json); other partial-I/O outcomes remain unverified. Query streaming also rejects non-finite DOUBLE and infinite DATE/TIMESTAMP values before Arrow encoding.
- The public local-owner write recovery smoke passed on macOS 26.5.2 arm64 with Python 3.11.15 / `pyspark-client==4.0.5` / `duckdb==1.5.1`. It retains the live `ReattachExecute` check, then drops a second committed write response, hard-kills the server, verifies the row and exact durable receipt before retry, restarts with a new server instance ID, and resends the unchanged request. The new process returns completion without applying the insert again; the database contains one copy. The final release-profile report is [here](evidence/phase-420-duckdb-write-reattach-final-release-local.json), with server SHA-256 `06286564b7b10890b6a6d98fad6cb8276beb8292431efcb8387f1f62f764ec6d`, runner SHA-256 `193e9bfa95d03c30a88394a0d9aac3bc41813c72dec59f128818a9516498e831`, and report SHA-256 `95b60bf2414e8e7ec4fa337c0911cc7a2c5b9778f08c9e1a6a9af13def15b17d`. This proves receipt recovery after a known commit on the local owner; it does not simulate a failure reported by DuckDB during its actual COMMIT operation.
- The earlier source-build Quack execution smoke verified shared table visibility, analysis-to-execution catalog changes, write reattachment, cancellation, owner restart, and durable receipt replay. Its report remains [here](evidence/phase-420-quack-execution-local.json), SHA-256 `836de62ffadced6887c49c13fc31ac8968bd9371e125fcf9ffa97144df528638`.
- The current release-profile Quack smoke also covered mixed-load owner loss. While a paced 1M-row reattachable stream was active, four independent Connect clients submitted reads; all four received expected Quack connection errors after owner loss, the stream completed 1M rows, HTTP health stayed ready, and reads recovered after restart. The report includes the restart/replay, cancellation, and conflict checks: [current report](evidence/phase-420-quack-execution-final-release-local.json), SHA-256 `5b651d014b498e7f670ea7482a399e7c62c87a7afe343e334878b3e6859978b3`. It records server SHA-256 `06286564b7b10890b6a6d98fad6cb8276beb8292431efcb8387f1f62f764ec6d`, runner SHA-256 `bfb1e88584d9c2377d28e834acd95191b1c503476c80619601679df7c181e488`, fixture owner SHA-256 `448df3700db8a0a9b8d348064bee3da2811fb679e0a25d3542c35f4624bf105f`, and extension SHA-256 `ef547f01cb30412aa6dbade59bdd5254ed897ba4f113a1700e3e01e00a747935`. This does not inject a failure at the transaction commit boundary or establish behavior on other platforms.
- `cargo test --locked -p sparkless-server-polars --no-default-features --features http,connect,duckdb-source --lib`: 101 passed, including the public HTTP execution path, SQL/plan translation, local DuckDB commands, output schemas, Arrow streaming, admission, replay, and cancellation. The prepared SQL route arms the deterministic catalog-race test hooks; both HTTP race cases pass.
- `cargo test --locked -p robin-sparkless-polars --features sql --lib`: 414 passed, 2 manual measurements ignored. This reruns the broad Polars SQL regression matrix required alongside the V37 execution checks.
- `PYTHONPATH=python/server-duckdb:python/connect /tmp/sparkless-phase420-client-py311/bin/python tests/connect/smoke_phase420_duckdb_execution.py --server-bin target/debug/sparkless-server-duckdb`: passed with the locked `pyspark-client==4.0.5`, `duckdb==1.5.1`, and PyArrow 26.0.0 environment. It covers public range/`selectExpr`, lazy SQL reads, `.collect()` on typed `CREATE TABLE`/`INSERT`, sequential visibility through the standalone DuckDB client, and read-after-restart.
- The final release-profile differential passed 44 Spark 4.0.5 result/schema comparisons and rejected 20 boundary forms. It recorded 51 DuckDB query witnesses and six committed-write witnesses using the unmodified `pyspark-client==4.0.5`, `duckdb==1.5.1`, PyArrow 26.0.0, and Python 3.11.15 on macOS 26.5.2 arm64. The 11-key configuration profile matched the pinned reference and all six public RuntimeConfig operations passed. The report records one intentional difference: Spark 4.0.5 returns NaN, while Sparkless keeps its finite-DOUBLE boundary. The server binary SHA-256 is `06286564b7b10890b6a6d98fad6cb8276beb8292431efcb8387f1f62f764ec6d`; the runner SHA-256 is `e0c40330e96788d18a408bfe841fa0de90d3e7daaad038b2396c40b48c9fa4a6`. The deterministic-gzip report is [here](evidence/phase-420-duckdb-connect-differential-final-release-local.json.gz), SHA-256 `b8afb47422fa6a577e7efa2c71daf10777a925d76fe8fc0fdd2a40537d9e27d5`. Exact 4.20.0 artifact and cross-platform confirmation remain release gates.
- The original V38 matrix (pre-fix 5-second send watchdog) used 60-second 1/4/8/16-client cohorts, one-million-row counts/slow readers, 100,000-row collects, and a 5-second compute queue timeout. Reattachable runs covered active limits 2/4/8 against local and shared Quack owners and recorded 25/19/10 admission rejections at limits 2/4/8 per owner mode. The non-reattachable cohorts at those limits stalled under 100 ms pacing; retain them as pre-fix evidence in the [12-run summary](evidence/phase-420-duckdb-v38-load-local-summary.json). The send-stall deadline fix allows 30 seconds for HTTP/2 credit replenishment and retains a 5-second reattachable response-ACK deadline. A bounded pool then removed the observed short-query stall behind a paced reader. Final release-profile 8-client/active-16 cohorts for both owners passed with zero capacity rejections; concurrent and final readers each validated 1,000,000 rows, health probes were clean, and both processes shut down gracefully. With the pool, local small-count p99 was 163 ms at 80.3 ops/s; Quack small-count p99 was 167 ms at 79.2 ops/s, versus 27.75 seconds and 12.4 ops/s before the pool. The Quack server and fixture owner peaked at 116 MB and 66.8 MB RSS. These are one-host observations with no accepted latency or memory budget. See the [read-pool summary](evidence/phase-420-duckdb-v38-read-pool-local-summary.json), its linked reports, and the separate [send-stall fix report](evidence/phase-420-duckdb-v38-backpressure-release-local-summary.json). Cross-platform repetition, arbitrary partial-I/O recovery, resource-budget acceptance, and exact release gates remain open.
- `PYTHONPATH=python/testing .venv/bin/python -m pytest tests/unit/test_sparkless_testing.py -q`: 126 passed in Sparkless mode, including comparator regression tests for nested types, metadata, decimal scale, and missing schema details.
- `SPARKLESS_TEST_MODE=pyspark ... pytest tests/unit/test_sparkless_testing.py::TestComparisonDataFrames -q`: 23 passed against the pinned Apache Spark 4.0.5 Python distribution. A local `sparkless-testing` wheel also built and imported in an isolated Python 3.11.15 environment where PySpark is absent; its declared dependency list contains only `pytest>=7`. See [the 420-06 local report](evidence/phase-420-sparkless-testing-local.json). The corrected wheel has not been published.
- Ruff format and lint checks passed for the changed Python helper and tests.
- The current local Python workspace suite passed 157 tests across `sparkless-testing`, the benchmark harness, and server launcher, with seven subtests. System pytest plugin autoload was disabled because the installed `pytest_cases` plugin is incompatible with the host pytest version.
- `cargo fmt --all -- --check` and strict Clippy passed for the DuckDB owner and the HTTP/Connect/DuckDB-source adapter (`-D warnings`).
- Final release-profile builds passed for the base server, DuckDB server, and Quack fixture owner. Startup selection, public DuckDB execution, local write reattachment/restart replay, Quack owner-loss/restart replay, and the refreshed 44-case Spark 4.0.5 differential with 20 explicit rejections pass against DuckDB server SHA-256 `06286564b7b10890b6a6d98fad6cb8276beb8292431efcb8387f1f62f764ec6d`. The differential runner writes deterministic gzip for `.json.gz` reports. Both server products were built into local macOS wheels and passed the isolated product-wheel verifier; the DuckDB wheel's embedded binary also passed the public DuckDB execution smoke. The current `sparkless-testing` wheel imported with PySpark imports blocked. The [package smoke report](evidence/phase-420-local-package-smokes.json) records wheel and binary hashes. These wheels retain 4.19.0 metadata and do not stand in for 4.20.0 artifacts. The refreshed [release-profile summary](evidence/phase-420-release-profile-local-summary.json), SHA-256 `57a84add0dad6d312a336f7641006975450b0c1d9aee9c56672e460cd703f294`, links current reports, including isolated performance attribution and active-query shutdown cancellation. A final three-pair HTTP comparison against the published 4.19.0 binary was rerun against the current base-server SHA-256 `fc7533b0887c9be002c0716cff005923e302c4c49e4b648746b9bd3d6a220b0c`; all paired small, wide mixed, and near-limit reads matched schemas and response lengths with no p50/p95 budget breaches ([report](evidence/phase-420-http-benchmarks-final-release-local.json), SHA-256 `32eaad7af4248de5c738197ff47d6dfda7863b754033107786fcd2af95c6e3ec). With the bounded query pool, local and Quack active-16 cohorts pass concurrent/final paced reads without capacity rejections; local small-count p99 is 163 ms at 80.3 ops/s, and Quack small-count p99 is 167 ms at 79.2 ops/s. The Quack server/owner peak RSS is 116/66.8 MB. These one-host observations have no accepted latency or memory budget. Cross-platform/package coverage, the supported capacity envelope, other partial-I/O recovery, full multi-operation cleanup acceptance, CI, and exact release-artifact checks remain open. The [pre-fix release summary](evidence/phase-420-release-profile-pre-fix-local-summary.json) and 5-second stall reports remain retained for comparison.

These are local source and release-profile checks. The expanded differential closes the declared
V37 matrix on one host, including result schemas, typed boundaries, fixed
configuration, analysis/execution behavior, and explicit unsupported forms.
The Quack report extends V38 identity, restart, owner-loss, and cancellation
evidence. Pre-fix load reports recorded queue rejections and non-reattachable stalls. The post-fix release runs at active_operations=16 completed paced readers against both owners, but the V38 capacity gate remains open until the supported resource envelope and broader acceptance are approved. One local WAL-limit fault also confirmed that a failed COMMIT reopened with no row and no receipt. Mixed-load owner-loss and active-query shutdown cancellation have now been measured locally; cross-platform repetition, other partial-I/O recovery, accepted capacity/resource budgets, full multi-operation cleanup, packaging, and exact release-artifact gates remain open. Performance
comparisons cover the local cohorts described below; per-change attribution is
recorded in its own section. The focused Arrow export comparison
had one median budget breach in its first three pairs; a fourth candidate-first
pair did not repeat it, so the local run did not establish a repeatable regression.

## D15 decision checklist

The following table records the choices currently implemented in source.
They remain candidate choices until the listed probes and validation reports
pass; source presence alone does not freeze D15.

| Decision | Current candidate implementation | Evidence required to freeze |
| --- | --- | --- |
| Product, backend, and listeners | `sparkless-server` remains Polars-only. `sparkless-server-duckdb` defaults to Polars; `SPARKLESS_EXECUTION_BACKEND=duckdb` requires that product and `SPARKLESS_DUCKDB_EXECUTION_OWNER`. DuckDB mode requires a Spark Connect listener and disables HTTP SQL statements. | Local source-launcher probes pass for default and selected modes, wrong product, missing owner/listener, and readiness/capabilities identity ([report](evidence/phase-420-backend-startup-local.json)). Repeat with installed release packages and exact artifacts before publication. |
| Backend boundary | The shared `ConnectExecutionPort` dispatches to Polars or DuckDB; session/replay/admission remain shared. DuckDB HTTP statements are rejected explicitly. | Trace lifecycle, analysis, command, stream, cancellation, and error paths with both real adapters. Verify no unsupported plan falls back to Polars. |
| Database owner | Choose either one persistent local database owned by one server process or one shared Quack owner. Quack Connect tables use a fixed execution catalog; 4.19 source aliases remain read-only and independent. | Verify bound queries, transactions, streams, interruption, credentials, snapshot scope, platform support, owner loss/restart, and independent-client visibility against the actual driver/extension. Never use two writable processes on one local file. |
| Client and semantics | The adapter targets the pinned Spark Connect 4.0.5 protocol. Candidate relations, SQL expressions, result types, and commands are enumerated below; unsupported forms are rejected. | Capture public-client request shapes and Spark 4.0.5 results/schemas for every supported row. Record settings, semantic differences, and boundary rejections. |
| Mutations and retries | Typed `CREATE TABLE` and literal `INSERT INTO ... VALUES` use a durable receipt table committed in the same transaction. Receipt capacity is 1,000,000; changed content for an existing identity is rejected. | Inject failures around statement/receipt/commit. Verify known and unknown outcomes, retry/reconcile behavior, restart persistence, and identity scope through the supported client/status path. |
| Resource budgets | One serialized write/receipt connection plus up to four concurrent read connections and at most 16 queued reads; DuckDB `threads=1` and `memory_limit=256MB` per connection; range cap 1,000,000; LocalRelation caps 10,000 rows, 1 MiB IPC, and 8 MiB decoded; Arrow output batches cap at 16,384 rows and 1 MiB. Existing Connect admission/response/replay bounds also apply. | Measure queue/deadline behavior, owner and server RSS, throughput, and latency on declared platforms/workloads. Distinguish enforced limits from observed process RSS. |

The 4.19 products default to Polars. The candidate reports the selected runtime
backend in readiness and capabilities, and the Python launcher negotiates its
expected value from the child environment. Test both products in default mode,
the DuckDB product in DuckDB mode, wrong product/backend, missing owner,
missing Connect listener, and readiness/capabilities disagreement. An
unsupported selection or failed startup probe must fail before exposing an
endpoint.

## Delivery order and dependencies

Start 420-01 with a 4.19 baseline and a minimal path from an unmodified client to DuckDB. Then land read execution and analysis (420-02), followed by writes (420-03) and the full lifecycle/shared-owner matrix (420-04). Begin cancellation and ownership tests with the first read and write slices. Run 420-06 and the performance slices independently of DuckDB enablement. Run 420-05 on the integrated candidate after the backend and parallel work pass their gates. Slice IDs remain stable.

| Slice | Deliverable | Exit evidence |
| --- | --- | --- |
| 420-01 | Backend selection and owner composition implemented; local source launcher matrix passed | Startup rejects the wrong product/backend/owner combination; default and selected-mode readiness and capabilities agree. Exact installed-artifact probes and D15 freeze remain pending. See the [local report](evidence/phase-420-backend-startup-local.json). |
| 420-02 | Read translation, analysis, bounded Arrow streaming, and pre-IPC scalar validation implemented; 44 local Spark-reference cases and 20 explicit rejection cases pass, including integer-width extrema and the complete fixed configuration profile; Quack analysis-to-execution catalog update passes | Source-level V37 matrix passes on macOS arm64. Repeat with exact release artifacts on supported platforms and rerun the Polars/source regression set. See the [expanded Spark report](evidence/phase-420-duckdb-connect-differential-expanded-local.json.gz) and [Quack report](evidence/phase-420-quack-execution-local.json). |
| 420-03 | Typed DDL/append subset and durable transactional receipts implemented; live reattachment, local and Quack replay after hard server restarts, synthetic pre-commit rollback/retry, simulated lost-COMMIT-ack replay, and local POSIX WAL write failure during COMMIT return unknown. The injected failure was followed by reopen and confirmed 0 rows, 0 receipts, and no partial payload. | Repeat storage-failure outcome checks on supported platforms and for other partial-I/O cases; retain the duplicate and conflicting identity checks through the Quack owner. See the [local storage-fault report](evidence/phase-420-duckdb-storage-commit-fault-local.json). |
| 420-04 | Local-owner and Quack durable replay, conflict rejection, owner recovery, cancellation, and a 1M-row reattachable owner-loss stream pass. A release-profile mixed-load probe kept that stream active while four independent Connect reads failed against the lost owner; health stayed ready and reads recovered after restart. A new backend-mode shutdown smoke confirms a live billion-row Quack-backed query is cancelled during graceful Sparkless shutdown, leaves no active Quack query, and exits both processes successfully. | Accept the supported capacity/resource envelope and its latency/RSS budgets; complete cleanup beyond the active-query shutdown witness, plus cross-platform and other partial-I/O recovery checks. Repeat mixed-load owner-loss acceptance on supported platforms and exact release artifacts. See the [pre-fix matrix](evidence/phase-420-duckdb-v38-load-local-summary.json), [read-pool summary](evidence/phase-420-duckdb-v38-read-pool-local-summary.json), send-stall fix report, Quack lifecycle report, [backend shutdown report](evidence/phase-420-duckdb-backend-shutdown-local.json), local-owner replay report, and storage-fault report. |
| 420-05 | Integrated HTTP/export/scheduler comparisons and isolated #1739–#1743 attribution are retained locally; release gate not complete | Finish exact-distribution candidate checks, cross-platform package/image checks, supported resource-envelope acceptance, and the release inventory. Repeat public checks after publication. See the [per-change evidence](#isolated-performance-attribution). |
| 420-06 | Comparator implementation, docs, and local candidate-wheel checks complete for [#1746](https://github.com/eddiethedean/robin-sparkless/issues/1746) | Sparkless (126) and PySpark 4.0.5 comparator (23) regression cases pass; isolated wheel import succeeds without PySpark installed. Publishing a coordinated 4.20 artifact remains a release gate. See the [local report](evidence/phase-420-sparkless-testing-local.json). |

## Performance follow-ups

These slices improve the existing engine, HTTP, and Connect paths independently of enabling DuckDB execution. The slice IDs identify the work and allow the optimizations to be measured separately.

The source changes for 420-07–420-11 are present in the candidate. The local
comparison used the published macOS arm64 4.19.0 base server and the integrated
candidate's release build with HTTP and Connect enabled. The reports retain
binary hashes, raw timings, sample counts, and resource measurements.

| Slice | Issue | Required outcome and evidence |
| --- | --- | --- |
| 420-07 | [#1739: Precompute positional JSON column resolution and paths](https://github.com/eddiethedean/robin-sparkless/issues/1739) | Resolve column positions and escape JSON paths once per result. Preserve duplicate labels, aliases, value conversion, limits, and cancellation. Compare narrow and wide results with mixed and nullable values. |
| 420-08 | [#1740: Reuse encoded HTTP response bytes](https://github.com/eddiethedean/robin-sparkless/issues/1740) | Encode the final read response on the blocking worker and reuse the validated buffer as the HTTP body. Preserve the response contract, exact byte limits, and committed mutation outcomes. Measure near-limit and small responses while probing health and short-query progress. |
| 420-09 | [#1741: Avoid discarded Connect clones and share retained Arrow payloads](https://github.com/eddiethedean/robin-sparkless/issues/1741) | Skip unused replay clones and share immutable Arrow payloads where possible. Verify unchanged client decoding, bounded byte accounting, and replay, release, and interrupt behavior. Measure skipped clones and shared payloads separately. |
| 420-10 | [#1742: Yield Connect compute admission during blocked sends](https://github.com/eddiethedean/robin-sparkless/issues/1742) | Release the compute permit while waiting for channel capacity and reacquire it before computation resumes. Preserve session ordering, response reservations, and cancellation. Verify stalled-reader progress and benchmark 1/4/8/16 clients at active limits 2/4/8. |
| 420-11 | [#1743: Reuse validated SQL ASTs for execution and schema metadata](https://github.com/eddiethedean/robin-sparkless/issues/1743) | Parse HTTP SELECT once and reuse the validated AST for execution and result metadata. Preserve SQL validation, session and snapshot semantics, mutation outcomes, and the string-based API. Measure parse, planning, execution, and total HTTP latency. |

Use the [4.19 performance protocol](RELEASE_4_19.md#performance-protocol-and-completion-criteria) for measurement methods. Record the published 4.19.0 server version, artifact hash, feature set, host, client lock, commands, and limits before changes. Measure each optimization against the immediately preceding comparable release build. Capture #1739 before #1740 and #1741 before #1742 if their changes are stacked. Finally, compare the integrated candidate with the published baseline under matching conditions.

Use three fresh rotating pairs and at least 20 warm samples per small workload. Record raw timings, result checks, CPU, RSS, and failed or rejected operations. Keep the small-job median regression limit at `max(5%, 1 ms)` and p95 at `max(10%, 2 ms)`. Investigate repeatable breaches before accepting a slice. For #1742, sustain each 1/4/8/16-client cohort for at least 60 measured seconds at active limits 2/4/8, with both reattachable and non-reattachable readers. Record first-batch and cancellation latency, blocked send, compute readmission, health progress, and p50/p95/p99 with sample counts. Select a throughput floor from the measured baseline before changing scheduling. Keep admission and byte limits fixed across pairs.

### Local performance evidence

- Connect exports passed all 66 paired workload comparisons across 22 workloads and three rotating pairs. All 24 paired small-job comparisons and the separately timed startup checks stayed within budget. One median in the focused 100,000-row Arrow export comparison exceeded budget across the three 60-sample pairs. A fourth candidate-first pair did not repeat that slowdown, and all four p95 comparisons stayed within budget. The compressed raw report SHA-256 is `977023ec54e8bde7990510c419be9681188527f4142b1ac74b014ac824fcc2b0`. [Raw Connect export and startup reports](evidence/phase-420-connect-exports-local.json.gz)
- HTTP retained the exact schema and response byte length for the small SELECT, 32-column mixed projection with duplicate labels, and 8,000,525-byte result. The large read's median fell from 9.9–10.4 ms to 7.2–7.3 ms across three pairs of 20 warm samples. Both binaries returned the same byte-limit error for the larger result, and every health and SQL progress probe succeeded. All HTTP workload medians and p95s stayed within the paired regression budget. The raw report SHA-256 is `5d934ad88a93bff25c9a7d0db465c273b45fea3407f7d8a41fd379a1fead9908`. [Raw HTTP comparison](evidence/phase-420-http-benchmarks-local.json)
- The non-reattachable scheduler sweeps completed at active limits 2, 4, and 8 with 1, 4, 8, and 16 clients. Every slow reader validated 1,000,000 rows and every progress probe succeeded. One active-8 candidate run overlapped another benchmark and was excluded. A clean sweep showed variable 16-client short-query tails, so the harness repeated that cohort in three fresh alternating pairs with a 95% throughput floor. The median throughput ratio was 99.3%, median paired short-query p95 improved by 4.1 ms, and no pair exceeded the p50 or p95 budget. The candidate trace records about 3.3 seconds of blocked send followed by compute readmission for the measured slow reader. [Raw scheduler reports and traces](evidence/phase-420-connect-scheduler-local.json.gz)
- Reattachable 1-, 4-, and 8-client cohorts completed three rotating 60-second pairs at all active limits 2, 4, and 8. Every cohort passed the 95% throughput floor and p50/p95 budgets in all pairs; median throughput ratios ranged from 99.4% to 100.2%. Reattachable 16-client cohorts also completed three pairs at active limits 2, 4, and 8. Their median throughput ratios were 99.2%, 100.1%, and 98.6%, all above the preselected floor. Median short-query p50 changes were +0.29, −0.08, and +0.20 ms; median p95 changes were +1.09, −0.10, and +0.56 ms. Each active limit had one p95 pair above budget, below the two-pair repeatability threshold; p50 had no breaches. Each slow reader validated 1,000,000 rows, 62 Arrow responses, and 63 release RPCs. Health, HTTP progress, resource sampling, graceful shutdown, and operation-ID-matched traces passed in every run.

The scheduler evidence archive SHA-256 is `c21565d95bb23acf3c3c127dbe36bcd1900e8a6ae3d516200a89afce01703a97`.

The scheduler harness now locks out overlapping local benchmark runs and
excludes client report collection from the throughput interval. The
local 24-cell matrix (two reader modes, active limits 2/4/8, and 1/4/8/16
clients) now has at least one 60-second baseline/candidate pair per cell.
Reattachable cohorts have three rotating pairs in every cell. All nine
reattachable 1/4/8-client cells passed every pair's throughput and latency
budgets. Non-reattachable cohorts have one pair per cell, with three additional
pairs for the saturated active-8, 16-client cohort. The three-pair 16-client
reattachable runs at active limits 2/4/8 had one p95 breach each, below the
repeatability threshold.

### Isolated performance attribution

These one-host release-profile comparisons isolate each change from an otherwise
matching candidate build. They complete local attribution for 420-07–420-11;
the variant binary hashes and raw timings are retained in the linked reports.
The variant executables are not archived. The current integrated base-server
binary is `fc7533b0887c9be002c0716cff005923e302c4c49e4b648746b9bd3d6a220b0c`.

| Slice | Local result | Evidence |
| --- | --- | --- |
| 420-07 / #1739 | Six rotating pairs with 20 warm samples per HTTP workload. The near-limit result had one p50 pair over its 1 ms/5% budget in the first three-pair run; it did not recur in the repeated three-pair run. No p95 pair exceeded budget. | [JSON column-resolution attribution](evidence/phase-420-http-attribution-1739-local.json) |
| 420-08 / #1740 | Three rotating pairs; no p50 or p95 budget breaches. Near-limit p50/p95 medians improved by 2.37/2.17 ms. Small and wide reads stayed effectively flat. | [Encoded response reuse attribution](evidence/phase-420-http-attribution-1740-local.json) |
| 420-09 / #1741, skip unused replay clone | Three 60-second 8-client/active-16 pairs passed the 95% throughput floor; median ratio 99.3%, with no short-job p50/p95 budget breaches. | [Replay-clone scheduler attribution](evidence/phase-420-scheduler-attribution-1741-local.json.gz) |
| 420-09 / #1741, share Arrow payloads | Three public 100,000-row Arrow pairs had median p50 +0.665 ms, p95 −0.547 ms, and peak RSS −49,152 bytes. One p50 pair exceeded the relative budget; this run does not show a repeatable p50 improvement. Existing decode, replay, accounting, and cancellation checks remain separate correctness evidence. | [Arrow payload-sharing attribution and raw samples](evidence/phase-420-arrow-sharing-attribution-local.json.gz) |
| 420-10 / #1742 | Against the no-yield variant, three 60-second 8-client pairs at active limits 16 and 8 passed the 95% throughput floor (median ratios 98.5% and 98.8%) with no short-job p50/p95 breaches. | [Active-16 comparison](evidence/phase-420-scheduler-attribution-1742-active16-local.json.gz), [active-8 comparison](evidence/phase-420-scheduler-attribution-1742-active8-local.json.gz) |
| 420-11 / #1743 | Three rotating HTTP pairs had no p50/p95 budget breaches. The measured median p50 changes were −0.0015 ms (small), −0.1165 ms (wide), and −0.0723 ms (near-limit); this is within measurement noise. A separate 200-observation stage probe measured parse p50/p95 at 0.0098/0.0248 ms, lowering at 0.0041/0.0133 ms, plan-to-lazy-frame at 0.3361/0.3672 ms, and total parse/plan/materialize/encode at 0.5722/0.6861 ms. | [SQL AST reuse HTTP attribution](evidence/phase-420-http-attribution-1743-local.json), [SQL stage timings](evidence/phase-420-sql-stage-attribution-local.json) |

The #1739 and Arrow-sharing observations include one isolated p50 budget breach
each; neither report shows a repeatable breach. These attribution runs explain
the measured changes on macOS arm64 but do not establish a production benefit or
the supported multi-client resource envelope. Cross-platform and exact-artifact
measurements remain release gates.

## SQL, plans, and type compatibility

Translate the frozen Connect relations/expressions and supported Spark SQL into engine-neutral operations or validated DuckDB plans/SQL. A `spark.sql(...)` query and a DataFrame chain must both obey the advertised semantic profile. Passing arbitrary Spark SQL straight to DuckDB is not evidence of compatibility.

Use this starting matrix for D15. List exact operators, request forms, literal/parameter types, and applicable configuration in the frozen matrix. Every advertised row needs a client fixture and a rejection case at its boundary.

| Surface | Proposed first support | V37 witness |
| --- | --- | --- |
| DataFrame reads | `range`, bounded scalar local relations, and named tables in the execution catalog. Projection, supported filters, aliases, limit, inner equijoins, and count/grouped count or sum. | Exact values and logical schemas against Spark. Include empty input, duplicate labels, aliased join keys, nulls, overflow, and multipart identifiers. Compare unordered rows as a multiset unless ordering is part of the query. |
| SQL reads | One parsed, allowlisted `SELECT` using the same expression and relation subset. Define any supported sort and cast variants explicitly. | Match the equivalent DataFrame query. Capture the actual client SQL request and reject malformed, unsupported, and multi-statement SQL without effects. |
| Types and analysis | Begin with the scalar Connect types already shipped, then admit only the types that round trip in the DuckDB adapter. | Compare AnalyzePlan schema with actual Arrow results for zero rows and multiple batches. Exercise nullability, duplicate labels, signed bounds, decimal precision/scale, and UTC microsecond timestamps. Define behavior when the catalog changes between analysis and execution. |
| Writes | Evaluate typed `CREATE TABLE` and append with `INSERT INTO ... VALUES` as the first Connect SQL commands. DataFrame writers and other commands need their own acceptance before enablement. | Verify public request shapes and resulting schema/data through both clients. Cover type or constraint failure before commit and explicit rejection of writes to existing read-only sources. |

Reject commands, plan nodes, settings, and types outside the frozen matrix before effects. Record the intentional semantic differences. Use bound parameters and validated identifiers when translating SQL. Validate the plan against a statement snapshot during execution so an earlier AnalyzePlan call cannot lend it stale schema or catalog state.

Keep catalog/table and session namespace ownership explicit. Temporary views/configuration belong to their session; shared saved tables have the selected database's persistence and transaction behavior. Mixed Polars/DuckDB execution within one plan is outside the initial backend and must not appear as an automatic fallback.

## Transactions and operation ownership

Enumerate the initial DDL/append/write commands and prove atomicity before enabling them. Reuse the server's operation identity and retained-result lifecycle, while binding each operation to its backend, database scope, session, and semantic context. A stable replay must not perform another write.

DuckDB tables may survive a Sparkless restart, unlike the original ephemeral Polars server catalog. Document that difference explicitly. Commit acknowledgement, cancellation, and response delivery remain separate events. Reserve outcome capacity before effects; obtain authoritative commit outcomes from the database owner. Do not claim a database rollback when only the client connection or response was lost.

D15 must choose and prove whether receipts are recorded transactionally with the data or whether some disconnect/crash cases remain unresolved and block automatic retry. Record the following outcomes through the supported client/status path:

| Injected condition | Required evidence |
| --- | --- |
| Rejection or cancellation before commit | No visible effects. Release reservations and report the documented rejection or aborted outcome. |
| Commit confirmed, delivery lost or cancelled | Data remains committed. Retry or reattach under the original identity returns its retained outcome without a second write. |
| Owner or connection lost before commit can be determined | Report the documented unknown outcome. Prevent automatic resubmission and define how an operator can reconcile the data. |
| Same identity with changed request or session | Reject the conflicting request or isolate it according to the frozen identity scope. Verify no replay or effects cross that scope. |
| Server restart | HTTP mutations require the current `server_instance_id` and reject stale instance IDs. A Connect retry with the same user, client session, operation ID, and database owner must resolve through the durable receipt rather than rerun a committed write. Local-owner replay after server restart and Quack replay after both Quack-owner and Sparkless restarts now pass in local smokes. Independently verify table persistence for each owner mode against release artifacts. |

Cancel/interrupt requests must reach the actual DuckDB producer. Keep its connection, transaction, compute permit, and buffers owned until it terminates. Once data commits, retain the committed outcome regardless of a later cancel, close, timeout, or disconnect. Reattachment replays retained Arrow results/receipts without rerunning a query. Source/service failures must not fall back to a local database or a second engine.

## Independent client interoperability

Use an in-repository fixture with a standard DuckDB client and Sparkless Connect client. Prove that both paths use the same DuckDB owner, canonical catalogs, and transaction visibility. Create/append through one interface and read through the other in both directions. Cover concurrent reads/writes, SQL-created tables, schema changes, temporary-state isolation, errors, restart, and canceled or disconnected writes. Use supported generic database connection settings. No external application change or maintainer acceptance is a prerequisite. A copy, export, or periodic synchronization cannot serve as evidence of shared live tables.

## Acceptance and release gates

### Before publication

- [ ] Freeze D15 and record the exact artifact evidence for the Phase 3 source prerequisites. A published version alone does not close V35/V36 or the public stage of V59.
- [ ] **V37:** Run the frozen matrix through the unmodified Connect client. Compare results and schemas with the pinned Spark reference and record an engine-side DuckDB execution witness for supported plans. Rerun the Polars and read-only source regression suites.
- [ ] **V38:** Pass the mutation and interoperability matrices, including owner loss, reconnect without reexecution, and capacity/cleanup. Apply V19–V24 to both enabled backends. Exercise short queries, large results, slow readers, and cancellation under concurrent load. Check D15's numeric budgets and report server and owner RSS, queue behavior, health progress, and correct throughput. V25 applies only to separately advertised proxy routes.
- [ ] Complete 420-06 and [420-07–420-11](#performance-follow-ups) on the integrated candidate. Retain baseline and candidate revisions, commands, settings, raw samples, failures, and artifact hashes with the evidence.
- [ ] Verify exact-commit CI and the [candidate release inventory](../../RELEASING.md). Run clean installs for local API, client-only, base server, and DuckDB server in both backend modes, with the supported launcher paths. Exercise server wheels on native Linux x86_64 and macOS arm64 and smoke-test both release images. Verify the base package and image exclude DuckDB/Quack and server execution requires neither Python nor a JVM. Publish the capability matrix and operating limits with the candidate docs.

### After publication

- [ ] Verify the coordinated 4.20.0 public inventory and hashes. Repeat fresh public installs, backend identity checks, and representative read/write, replay, and helper smoke tests on the shipped artifacts.
- [ ] Link the final reports and known limitations from the release evidence. Update stable install instructions and the testing API version notes after the public checks pass.

## Touchpoints

- [Core engine interfaces](https://github.com/eddiethedean/robin-sparkless/blob/main/crates/robin-sparkless-core/src/engine/mod.rs)
- [Shared Polars-backed server runtime and construction](https://github.com/eddiethedean/robin-sparkless/blob/main/crates/sparkless-server-polars/src/lib.rs)
- [Connect translation](09-connect-plans-and-analysis.md) and [lifecycle conformance](10-arrow-replay-and-connect-conformance.md)
- [DuckDB source ownership and schema decisions](14-duckdb-data-source.md)

Distributed execution, arbitrary Python workers, universal Spark compatibility, cross-engine federation, and native Delta/UC writes outside the frozen backend matrix remain separate work.
