---
search:
  exclude: true
---

# 4.19.0 implementation and release plan

**Status: planned; refined 2026-10-31.** This plan schedules optional DuckDB table sources, the `SparklessConnect` context manager, and performance issues [#1736](https://github.com/eddiethedean/robin-sparkless/issues/1736), [#1737](https://github.com/eddiethedean/robin-sparkless/issues/1737), and [#1738](https://github.com/eddiethedean/robin-sparkless/issues/1738). No slice or benchmark is complete by virtue of this document. Current release behavior remains defined by the [support matrix](../../SUPPORT_MATRIX.md).

[Roadmap](ROADMAP.md) · [Epic](EPIC.md) · [Decisions](DECISIONS.md) · [Validation](VALIDATION.md) · [Release procedure](../../RELEASING.md)

## Outcome and implementation baseline

An unmodified `pyspark-client==4.0.5` can read configured DuckDB tables and run the declared transformations through Polars. `with SparklessConnect() as spark:` owns a real Connect session and native server, with deterministic cleanup. Existing range/local-relation workloads gain measured collection, count, and concurrency improvements while retaining small-query latency and bounded transport/replay accounting.

Retain the 4.18 Spark 4.0.5 protocol/client locks, CPython 3.11 fixture environment, UTC/ANSI/case profile, and checked Polars Arrow boundary. Broader client/Python/platform compatibility requires separate evidence. The [current release procedure](../../RELEASING.md) governs component versions, candidate checks, publication, and optional diagnostics; older coordinated-publication prose does not override it. Plan an API and engine/server 4.19.0 release because this phase changes both components; keep exact native/server dependency pins and verify the split API/native/server distributions.

Source review at `fd9bcb849614a703f6d7ce023f478d3b82405c53` identifies these implementation inputs:

| Area | Existing behavior | Required 4.19 change |
| --- | --- | --- |
| External catalog | `ExternalTableFormat` describes Delta; external Arrow streams primarily serve writes | Add an engine-neutral typed read source with snapshot/lease ownership; preserve existing providers |
| Connect translation | Range/local relations, filter/project/limit, and global count; named-table reads, joins, and grouped aggregates are absent | Add and fixture the explicit table/transformation subset below; native engine support alone does not enable Connect |
| Arrow export | Range/lazy sources use one-row pulls and frames are assembled per row | Implement bounded columnar consumption and serialization in 419-09 |
| Range count | `integer_literal` extracts scalar literals; ordinary core I64 literals can be dynamic | Prove and repair eligibility through the public core-IR/Connect path in 419-10 |
| Scheduling | Compute permits span production/sending; full response channels are polled with sleeps up to 10 ms | Instrument first; change backpressure/admission only with evidence in 419-11 |
| Python launcher | `SparklessServer` owns a process; no `SparklessConnect` helper or connect extra | Compose the existing launcher with an independently owned real client session |

Review the current files when implementation starts; these observations are a dated baseline, not a substitute for profiling. Local work can begin against settled interfaces. Shipping requires the earlier supported native/HTTP/Connect behavior and fresh package verification; record any outstanding 4.18 publication recovery using [release evidence](../../RELEASE_EVIDENCE.md).

## Delivery order and dependencies

IDs 419-01–419-11 retain their existing meanings. Execution order follows dependencies rather than numeric order. Assign an owner, reviewable change, exact tested revision, and retained evidence to every slice.

| Slice | Deliverable | Dependencies | Exit evidence |
| --- | --- | --- | --- |
| **419-01** | Freeze D14: native driver, standalone and live-owner modes, namespaces, schemas, snapshots, limits, packaging | Existing source/catalog interfaces; self-contained DuckDB feasibility fixture | Exact versions/licenses and supported platforms; standalone lock/error behavior and independent writer sharing one owner; numeric resource budgets; no Python/JVM server dependency |
| **419-02** | Engine-neutral source resolver and Connect named-table/schema analysis | 419-01 | Canonical identity, qualified-name/case/collision tests, no rows collected during analysis, operation-scoped snapshot lease, source-disabled errors |
| **419-03** | Typed, cancellable source batches consumed by Polars | 419-02; agreed batch-budget interface with 419-09 | Empty/nonempty exact schemas/values, bounded buffering, early termination, source loss, and lease cleanup |
| **419-04** | Implement the declared Connect source transformations | 419-03; public-client fixtures for the new nodes | Spark differential corpus for table/filter/project/limit/count/join/grouped aggregate; explicit rejection before source writes/effects |
| **419-05** | V35/V36 source acceptance, optional packaging, capability/docs | 419-01–419-04 | Standalone/live-owner races, snapshots, replay, measured budgets, default/enabled native builds and fresh installs |
| **419-06** | Freeze helper API, optional dependencies, import behavior | Existing launcher/control contract; locked 4.0.5 client | Documented constructor, exact dependency policy, client/Python/platform matrix, no import-time PySpark import/process launch |
| **419-07** | Implement owned helper lifecycle | 419-06 | Independent session, defined reentry/error behavior, all cleanup attempted, unrelated session unaffected |
| **419-08** | Helper acceptance and packaged docs/examples | 419-07 | Real public-client success and failure matrix; base/server/connect installs; no owned child/session left behind |
| **419-09** | Bounded columnar Arrow export, #1736 | Shared performance protocol frozen; existing IPC/lifecycle contracts | Mechanism counters, result/type/byte/cancel regressions, paired collection/empty-result measurements |
| **419-10** | Exact dynamic-integer range counts, #1737 | Shared performance protocol frozen; existing core IR | Direct eligibility assertion, public-client results, checked boundaries/fallback, paired count measurements |
| **419-11** | Measured mixed-job scheduling/backpressure, #1738 | 419-09 and 419-10 measured together; phase instrumentation | Sustained 1/4/8/16-client reports, short-job tails, slow-reader/cancel/replay/accounting matrix; default changes justified separately |

Start by capturing the performance baseline and proving 419-01 feasibility. The helper and range-count work can proceed independently of DuckDB. Agree source/output batch ownership and byte budgets before implementing 419-03/419-09. Remeasure the combined exporter/count candidate before isolating any scheduler change. Integrate all tracks before final candidate validation; neither standalone file scans nor helper/performance completion satisfy the live-owner source gate. If feasibility fails, record the blocker and revise the phase scope explicitly before shipping a reduced release.

## DuckDB source contract

### Ownership and snapshots

D14 remains open until 419-01 selects and measures a native implementation. Certify two modes: a standalone read-only source owned by Sparkless, and independent writer/reader clients reaching one database owner. A proposed service/driver needs native clients, compatible exact pins, transaction support, deployment instructions, and platform builds. A second writable process opening the same file is insufficient. Use in-repository writer/reader fixtures; another application's contribution or release cannot gate this phase.

Trusted server configuration defines source aliases and connection settings. Clients resolve named tables through those aliases; they cannot supply database paths, arbitrary connection strings, extension-loading statements, or credentials. Freeze one-, two-, and three-part names, quoting/case rules, default namespace, collision behavior with existing catalogs, and missing/disabled-source errors in D14.

Analysis reads metadata without scanning table rows. A standalone schema request describes its own observed version; it cannot pin a transaction indefinitely waiting for a later execution. Execution resolves metadata and all source tables in one statement snapshot, reanalyzes the plan against that snapshot, and makes execution result descriptors agree with Arrow payloads. A schema changed since an earlier analysis must produce current consistent metadata or a clear analysis error, never a silently mismatched payload.

The operation owns source connection/transaction leases until actual source/Polars production stops. Deadline/disconnect requests cancellation; they do not release resources still in use. Reattachment reads retained responses from the original producer. Source failure must not reconnect and replay the plan against newer data. Explicit release, expiry, session close, and shutdown must retain accounting until cleanup completes. Independent writer commits become visible to a subsequent statement; an active statement retains its original multi-table snapshot.

### Minimum public-client matrix

Implement the following narrow additions in 419-02/419-04. Capture the actual unmodified 4.0.5 requests, including aliases/plan IDs, before extending the translator. These additions support the source use case; the wider API expansion remains in 4.23.

| Area | Required positive cases | Boundary and negative cases |
| --- | --- | --- |
| Table and analysis | `spark.table` with configured qualified names; exact schema on empty/nonempty tables | Unknown/ambiguous names, disabled source, drop/replace between analysis and execute; no row action during analysis |
| Filter/project/limit | Existing captured comparisons and projection expressions over source columns; alias, limit zero, limit before/after filter | Nulls, duplicate labels, missing/ambiguous columns, early stop; preserve plan order |
| Count | Global `count()` and `count(column)`; null exclusion for the latter | Empty input, all-null column, BIGINT count schema; no metadata shortcut without snapshot-valid proof |
| Join | Two configured tables in one source owner; inner equijoin on same-type integer keys with explicit aliases/projection | Null/duplicate keys, empty side, qualified resolution, exact nullability; unsupported join kinds/conditions fail clearly |
| Grouped aggregate | One integer/string key with `count()` and nullable BIGINT `sum` | Null keys, empty/all-null input, overflow and output nullability under the pinned Spark profile; unsupported aggregates fail clearly |
| Read-only enforcement | Existing Sparkless-owned tables retain their behavior | All DuckDB-targeted DDL/DML, save/append/overwrite and unsupported descendants rejected before effects; Connect SQL commands remain outside this phase |

Join/group/aggregate translator additions must preserve existing range/local-relation behavior. Advertise support per enabled source/type/expression capability; do not promise a new node generally merely because one source fixture passed. Cross-owner and DuckDB-to-managed-table joins remain unsupported until a statement consistency policy and separate evidence are added to D14. The mandatory source join uses two tables under the same owner.

Map the existing remote scalar set exactly: BOOLEAN, SMALLINT, INT, BIGINT, finite DOUBLE, STRING, DATE, UTC microsecond TIMESTAMP, and DECIMAL(1–38, valid scale). Test integer extrema, precision/scale, nullability, field order/metadata, and schema-bearing empty batches. DuckDB-specific, nested, unsigned/out-of-range, non-finite, or incompatible temporal values fail explicitly until an exact mapping is selected and tested. Start without filter pushdown; enable each pushdown only after Spark/Polars semantic equivalence evidence.

D14 must record finite values and enforcement points for source connections, active snapshots and their lifetime, scan queue length/bytes, decoded batch bytes, maximum scalar bytes, cancellation/deadline handling, and idle/cleanup limits. Account source buffers alongside output/replay buffers and DuckDB/Polars intermediates. Reuse current transport caps unless a measured change is recorded. State separately which bounds are enforced and which CPU/RSS/intermediate budgets are cooperative or measured. Increasing table sizes and slow consumers must demonstrate the chosen budgets; batched output alone does not prove bounded total execution memory.

## SparklessConnect helper contract

Proposed public API: `SparklessConnect(*, server_options=None)` in `sparkless.server`, forwarding a copied mapping of existing `SparklessServer` keyword arguments (including `executable_path`, `startup_timeout`, `shutdown_timeout`, and `kill_timeout`). Reject `connect_port=None` before launching. Keep top-level export optional; the documented import is `from sparkless.server import SparklessConnect`. The helper owns a new server and session; attaching an external server and async helper support remain later scope.

On entry, validate options and lazily import the pinned public Connect client before launching; report a missing dependency with an install command. Start the existing launcher, wait for validated readiness, and create an independent real session with `SparkSession.builder.remote(server.spark_connect_url).create()`. Verify this client path against the pinned release; avoid reusing an unrelated active/default session. Return the actual Connect `SparkSession`.

Lifecycle states are idle, entering, active, exiting, then idle after complete cleanup. Same-instance nested entry fails before side effects. Sequential reuse creates a fresh process/session only after previous cleanup succeeds; separate instances can nest. Reject use after fork consistently with the launcher. Manual session stop inside the block is safe. Always attempt session stop followed by server shutdown/reap, including failed session creation and stop failures. Preserve the original body/startup error and attach/report cleanup errors; with no earlier error, raise the first cleanup failure and retain others. Never suppress a body exception or claim idle cleanup success while an owned child remains.

The proposed `sparkless[connect]` extra includes `pyspark-client==4.0.5` and the matching engine-version server binary. Base and `sparkless[server]` installs remain free of a required PySpark client. Install/resolve base, server, connect, and combined extras in clean environments; retain the package's existing Python minimum and the server platform matrix unless expanded with evidence. Importing the helper starts no process and imports no PySpark. The existing parent `sparkless` import loads the native extension; this phase does not promise an engine-free parent-package import.

## Performance protocol and completion criteria

First preserve a reproducible baseline: published API 4.18.1 with native/server 4.18.0, exact artifact hashes and build features, locked 4.0.5 client, machine/OS, CPU/thread settings, all limits, and workload commands. Add reproducible mixed-workload and session-fixture/four-worker cases to the repository; issue prose alone is insufficient. Reuse the existing slice-8 runner, fault proxy, and V26 verifier where applicable, extending their corpus/reporting rather than inventing a parallel benchmark framework. Keep the historical V26 runner budgets intact; they do not replace the stricter 4.19 small-job guardrails.

Compare released baseline and candidate on the same host in rotating order across at least three fresh server pairs. Keep client code, data, client/reference versions, thread/admission limits, and semantic settings identical. Separate cold startup plus readiness and first suite from warm query timings. Exclude shutdown from query latency and record shutdown/cleanup separately. Store raw per-operation samples and failed/rejected/cancelled operations, not just aggregate medians. For Spark comparisons use one shared server per engine and the identical Connect client; record configured limits without claiming hard CPU equivalence. Baseline/candidate Sparkless comparisons determine regression acceptance; beating Spark on every workload is not a requirement.

| Workload | Required evidence / acceptance |
| --- | --- |
| #1736 export | Correct 1k/10k/100k range results and local relations within the existing 10k-row/byte caps, empty filters at 1k/10k inputs, wide/variable-width data, LIMIT and oversized-cell cases. Oversized inline local relations retain explicit limit errors. Record source chunks/native executions, serialization/rechunk costs, first-batch/total latency, and peak buffers/RSS. Optimized range path must avoid per-source-row native execution. Initial target: at least 2× faster 100k-row range collection than baseline. |
| #1737 count | Prove fast-path selection through core IR and public Connect string `>` / Column `>=`; check typed/dynamic thresholds, reversed comparisons, integer bounds, positive/negative steps, empty ranges, unsupported/fractional/out-of-range fallback, and cancellation. Measure 1k/1M/10M counts: no eligible row materialization, growth within the small-job absolute/relative latency budget, and initial target of at least 2× faster 10M count. Keep native boundary tests even where the public translator rejects a predicate. |
| #1738 scheduling | After measuring combined exporter/count changes, isolate scheduler changes against that intermediate candidate as well as the released baseline. Run 1/4/8/16 independent clients with the same mix of short jobs, large collections/counts, and slow readers at active limits 2/4/8. Start with at least 60 seconds of measured steady load per cohort and extend until tails are stable. Report throughput, short-job p50/p95/p99, admission wait, compute/encode/replay/blocked-send time, queue/rejections, CPU/RSS, cancellation, and health progress. Select a numeric throughput acceptance floor from the measured post-export baseline before changing scheduler code; record it with the workload/configuration. |
| Small-job guardrails | At least 20 measured warm iterations per workload per fresh runtime. Cover 10/100/1k/10k tuple ingestion/filter/count, preview 25, nullable/mixed values, schema analysis, startup, and session-fixture/four-worker pytest. Fail repeatable median increases above `max(5% of baseline, 1 ms)` or p95 increases above `max(10% of baseline, 2 ms)`, including matched contention cases. |

Twenty samples are a minimum for basic timings, not sufficient evidence for p99. Report tail sample counts and percentile method, retain enough sustained observations to stabilize tails, and rerun noisy pairs before accepting or rejecting a change. Performance targets remain open until measured; a missed target requires recorded evidence and explicit scope/target revision before declaring the issue complete.

Compute admission and response-byte accounting remain separate responsibilities. A blocked producer can release/yield compute only when it has stopped computing, and must reacquire admission before resuming. Preserve per-session serialization, original operation identity, reserved terminal delivery, deadline/cancel wakeups, and byte reservations across detach/reattach. Shared response buffers and notification-based sending are implementation candidates to measure; no buffer, concurrency, or default-limit increase is accepted solely to improve a benchmark. Rerun existing HTTP/Connect slow-reader, cancellation, expiry/release, saturation and shutdown tests after each relevant change.

## Integration and release acceptance

Use existing CI and the release workflow described in [RELEASING.md](../../RELEASING.md). Put deterministic correctness/lifecycle/limit regressions in ordinary CI. Record reproducible performance and source-resource diagnostics on the final candidate; these are evidence needed to accept this phase and its issues, not a claim that new workflow jobs already exist or that all historical optional diagnostics gate publication.

- [ ] Every 419-01–419-11 slice has an owner, exact revision, passing evidence, and documented limitations; D14 selects supported modes/types/platforms and numeric budgets.
- [ ] V35/V36 pass for real standalone and live-owner sources; table/join/grouped-aggregate client fixtures and reference comparisons cover the declared additions, races, source-write rejection, and snapshot cleanup.
- [ ] The helper passes real packaged success, body/startup/import/session/stop/shutdown failure, repeated/nested/fork/manual-stop, and unrelated-session isolation cases; base/server/connect dependency boundaries pass.
- [ ] #1736–#1738 meet their issue acceptance and this protocol, including small-job guardrails, selected scheduling throughput floor, mechanism counters, resource/lifecycle regressions, and retained raw reports. Scheduling-only evidence uses the post-export/count intermediate baseline too.
- [ ] Earlier native/HTTP/Connect regressions pass on the integrated candidate; DuckDB-disabled and enabled builds, existing Delta providers, supported server wheels/container, and clean consumer installs retain their contracts.
- [ ] Capability/support/install documentation lists only the tested source/type/client/platform matrix. Default Polars and optional DuckDB behavior are distinguishable; later DuckDB execution, writes, general Connect SQL, artifacts, UDFs, and streaming remain outside this release.
- [ ] Exact-commit CI and package checks pass before publication under the current release procedure. Verify API/native/server dependencies, source archives, wheel size/storage inventory, native library loading, and the tested container executable; no source checkout is required by an installed consumer.
- [ ] Fresh public installs/hashes and container digest/HTTP/Connect/helper/source smoke checks verify the final published inventory. Preserve the candidate and public evidence separately; partial publication leaves release completion open.

Record final evidence as commands plus immutable revisions/artifact hashes, feature/platform matrix, pass/fail/skipped status, measured limits, and known deviations. Update [DECISIONS](DECISIONS.md), [VALIDATION](VALIDATION.md), and the support docs when implementation evidence changes their status. This planning change neither bumps versions nor publishes artifacts.
