---
search:
  exclude: true
---

> **Archive / historical record:** this page describes a past snapshot or a plan.
> Use the [current support contract](../../SUPPORT_MATRIX.md) for release behavior.

# SS-15: Optional DuckDB execution backend for Spark Connect

> **Status: implementation present in the current 4.20 candidate; not release-ready.** Target release: 4.20.0; optional capability. V37 Spark semantic and schema evidence, V38 transaction/owner lifecycle evidence, performance reports, and exact-artifact release gates remain open.
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
streaming, typed `CREATE TABLE` and literal `INSERT ... VALUES`, transactional
operation receipts, launcher identity negotiation, and capability reporting.
It also includes source changes for 420-07–420-11 and #1746. These are code
implementation statements, not V37/V38 or performance results. No candidate
release artifact or public capability claim has been approved.

Implementation details and operator setup are summarized in the
[DuckDB execution candidate guide](../../DUCKDB_EXECUTION.md). The package and
server behavior remain documented as 4.19.0 until 4.20 artifacts pass the
acceptance gates below.

### Local source verification (2026-11-05)

On the local macOS arm64 workspace, these checks passed against the current
source candidate:

- DuckDB server and base Polars server compile with HTTP and Connect enabled.
- `cargo test -p robin-sparkless-duckdb`: 8 passed.
- `cargo test -p sparkless-server-polars --no-default-features --features connect,duckdb-source`: 62 passed, including local DuckDB command, SQL query, output schema, and Arrow stream coverage.
- `PYTHONPATH=python/testing .venv/bin/python -m pytest tests/unit/test_sparkless_testing.py::TestComparisonDataFrames -q`: 16 passed.
- Ruff format and lint checks passed for the changed Python helper and tests.

These are local source checks. They do not close V37 Spark result/schema
comparison, V38 owner fault and interoperability coverage, performance
reports, cross-platform packaging, or exact-artifact release gates.

## D15 decision checklist

The following table records the choices currently implemented in source.
They remain candidate choices until the listed probes and validation reports
pass; source presence alone does not freeze D15.

| Decision | Current candidate implementation | Evidence required to freeze |
| --- | --- | --- |
| Product, backend, and listeners | `sparkless-server` remains Polars-only. `sparkless-server-duckdb` defaults to Polars; `SPARKLESS_EXECUTION_BACKEND=duckdb` requires that product and `SPARKLESS_DUCKDB_EXECUTION_OWNER`. DuckDB mode requires a Spark Connect listener and disables HTTP SQL statements. | Probe default and DuckDB modes through installed launchers. Verify wrong product, missing owner, and missing Connect listener fail before readiness. Compare readiness, capabilities, and launcher identity. |
| Backend boundary | The shared `ConnectExecutionPort` dispatches to Polars or DuckDB; session/replay/admission remain shared. DuckDB HTTP statements are rejected explicitly. | Trace lifecycle, analysis, command, stream, cancellation, and error paths with both real adapters. Verify no unsupported plan falls back to Polars. |
| Database owner | Choose either one persistent local database owned by one server process or one shared Quack owner. Quack Connect tables use a fixed execution catalog; 4.19 source aliases remain read-only and independent. | Verify bound queries, transactions, streams, interruption, credentials, snapshot scope, platform support, owner loss/restart, and independent-client visibility against the actual driver/extension. Never use two writable processes on one local file. |
| Client and semantics | The adapter targets the pinned Spark Connect 4.0.5 protocol. Candidate relations, SQL expressions, result types, and commands are enumerated below; unsupported forms are rejected. | Capture public-client request shapes and Spark 4.0.5 results/schemas for every supported row. Record settings, semantic differences, and boundary rejections. |
| Mutations and retries | Typed `CREATE TABLE` and literal `INSERT INTO ... VALUES` use a durable receipt table committed in the same transaction. Receipt capacity is 1,000,000; changed content for an existing identity is rejected. | Inject failures around statement/receipt/commit. Verify known and unknown outcomes, retry/reconcile behavior, restart persistence, and identity scope through the supported client/status path. |
| Resource budgets | One serialized owner connection; DuckDB `threads=1` and `memory_limit=256MB`; range cap 1,000,000; LocalRelation caps 10,000 rows, 1 MiB IPC, and 8 MiB decoded; Arrow output batches cap at 16,384 rows and 1 MiB. Existing Connect admission/response/replay bounds also apply. | Measure queue/deadline behavior, owner and server RSS, throughput, and latency on declared platforms/workloads. Distinguish enforced limits from observed process RSS. |

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
| 420-01 | Backend selection and owner composition implemented | Startup rejects the wrong product/backend/owner combination and readiness/capabilities expose the backend. Public default/selected-mode launcher probes and the frozen D15 evidence record remain pending. |
| 420-02 | Read translation, analysis, and bounded Arrow streaming implemented | Run V37 against the pinned client and Spark reference, including empty results and exact logical schemas. |
| 420-03 | Typed DDL/append command subset and durable transaction receipts implemented | Inject faults before and after commit; establish aborted, committed, and unknown outcomes and verify duplicate/conflicting operation identities. |
| 420-04 | Local and shared owner paths, cancellation, and transactional receipts implemented; restart recovery path unproven | Run V38 bidirectional reads/writes, cancellation, reattach, release, saturation, owner loss, and restart against the same owner. Verify which retries can reach a durable receipt after a server restart; persistence alone is not replay evidence. |
| 420-05 | Not complete | Finish exact-distribution candidate checks, cross-platform package/image checks, benchmark reports, and the release inventory. Repeat public checks after publication. |
| 420-06 | Comparator source and documentation updated for [#1746](https://github.com/eddiethedean/robin-sparkless/issues/1746) | Run Sparkless/PySpark regression cases for nested types, nullability, metadata, decimal parameters, and empty mapping expectations. Verify the installed helper has no PySpark runtime dependency and publish the corrected artifact. |

## Performance follow-ups

These slices improve the existing engine, HTTP, and Connect paths independently of enabling DuckDB execution. The slice IDs identify the work and allow the optimizations to be measured separately.

The source changes for 420-07–420-11 are present in the candidate. None of
their latency, throughput, RSS, scheduling, or small-job acceptance claims is
complete until the prescribed reports are retained.

| Slice | Issue | Required outcome and evidence |
| --- | --- | --- |
| 420-07 | [#1739: Precompute positional JSON column resolution and paths](https://github.com/eddiethedean/robin-sparkless/issues/1739) | Resolve column positions and escape JSON paths once per result. Preserve duplicate labels, aliases, value conversion, limits, and cancellation. Compare narrow and wide results with mixed and nullable values. |
| 420-08 | [#1740: Reuse encoded HTTP response bytes](https://github.com/eddiethedean/robin-sparkless/issues/1740) | Encode the final read response on the blocking worker and reuse the validated buffer as the HTTP body. Preserve the response contract, exact byte limits, and committed mutation outcomes. Measure near-limit and small responses while probing health and short-query progress. |
| 420-09 | [#1741: Avoid discarded Connect clones and share retained Arrow payloads](https://github.com/eddiethedean/robin-sparkless/issues/1741) | Skip unused replay clones and share immutable Arrow payloads where possible. Verify unchanged client decoding, bounded byte accounting, and replay, release, and interrupt behavior. Measure skipped clones and shared payloads separately. |
| 420-10 | [#1742: Yield Connect compute admission during blocked sends](https://github.com/eddiethedean/robin-sparkless/issues/1742) | Release the compute permit while waiting for channel capacity and reacquire it before computation resumes. Preserve session ordering, response reservations, and cancellation. Verify stalled-reader progress and benchmark 1/4/8/16 clients at active limits 2/4/8. |
| 420-11 | [#1743: Reuse validated SQL ASTs for execution and schema metadata](https://github.com/eddiethedean/robin-sparkless/issues/1743) | Parse HTTP SELECT once and reuse the validated AST for execution and result metadata. Preserve SQL validation, session and snapshot semantics, mutation outcomes, and the string-based API. Measure parse, planning, execution, and total HTTP latency. |

Use the [4.19 performance protocol](RELEASE_4_19.md#performance-protocol-and-completion-criteria) for measurement methods. Record the published 4.19.0 server version, artifact hash, feature set, host, client lock, commands, and limits before changes. Measure each optimization against the immediately preceding comparable release build. Capture #1739 before #1740 and #1741 before #1742 if their changes are stacked. Finally, compare the integrated candidate with the published baseline under matching conditions.

Use three fresh rotating pairs and at least 20 warm samples per small workload. Record raw timings, result checks, CPU, RSS, and failed or rejected operations. Keep the small-job median regression limit at `max(5%, 1 ms)` and p95 at `max(10%, 2 ms)`. Investigate repeatable breaches before accepting a slice. For #1742, sustain each 1/4/8/16-client cohort for at least 60 measured seconds at active limits 2/4/8, with both reattachable and non-reattachable readers. Record first-batch and cancellation latency, blocked send, compute readmission, health progress, and p50/p95/p99 with sample counts. Select a throughput floor from the measured baseline before changing scheduling. Keep admission and byte limits fixed across pairs.

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
| Server restart | A new instance rejects old-instance retries. Independently verify that committed tables persist. Durable receipt replay requires its own transactional ledger and recovery proof. |

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
