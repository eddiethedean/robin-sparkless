---
search:
  exclude: true
---

> **Archive / historical record:** this page describes a past snapshot or a plan.
> Use the [current support contract](../../SUPPORT_MATRIX.md) for release behavior.

# SS-15: Optional DuckDB execution backend for Spark Connect

> **Status: planned; owner unassigned.** Phase: 4, after the supported Connect server and DuckDB data-source phase. Target release: 4.20.0; optional capability.
> [Index](README.md) · [Roadmap](ROADMAP.md) · [Epic](EPIC.md) · [Decisions](DECISIONS.md) · [Validation](VALIDATION.md)

## Outcome and distinction from Phase 3

[4.19.0 shipped](../../RELEASE_EVIDENCE.md#4190-public-artifacts-2026-11-05-utc) the [separate DuckDB product](23-product-and-package-separation.md), with read-only DuckDB sources and Polars execution. Reuse its product, source driver, and launcher boundaries. D15 and V37/V38 govern the new execution mode. Use the 4.19.0 artifacts as the regression baseline and reconcile outstanding source acceptance against the exact artifacts used by 4.20.

Expose an optional DuckDB execution backend through Sparkless Server's existing Spark Connect endpoint. The same unmodified Connect client can submit the declared SQL/DataFrame subset, but DuckDB now executes the supported plans and owns persistent table writes. Phase 3 only supplied DuckDB table batches to Polars; this phase performs the query inside DuckDB.

Polars remains the default engine. Choose an execution engine explicitly when configuring a server instance, bind that choice for its sessions/operations, and expose a truthful capability matrix. Do not silently switch engines for unsupported operations or after a statement may have taken effect. Backend selection uses Sparkless server configuration and the standard Connect contract.

This is a native single-node backend, not Apache Spark or general Spark compatibility. The server and enabled image remain Python/JVM-free. Existing DuckDB data is usable only through the database-owner modes certified in Phase 3; a Connect URL does not grant safe access to another process's database files.

## Prerequisites and decisions

- Phase 2 SS-09/10 Connect translation, analysis, Arrow, replay, session, interruption, and release conformance.
- Phase 3 SS-14/D14 native DuckDB dependencies, ownership, names, snapshots, types, and measured resource behavior; V35/V36 completed for advertised shared data.
- Freeze [D15](DECISIONS.md#decisions-required-by-milestones) in 420-01 before implementing the supported DuckDB surface. Short feasibility probes can inform the decision. Record the outcome and evidence for each choice below.

## D15 decision checklist

The following defaults and query surface are proposals for 420-01. Freeze only the combinations backed by a working probe or a documented validation plan.

| Decision | Proposed starting point | Evidence required to freeze |
| --- | --- | --- |
| Product, backend, and listeners | Keep `sparkless-server` on Polars. Default `sparkless-server-duckdb` to Polars with its existing source behavior. Enable DuckDB execution with trusted startup configuration, fixed for the process. Scope the new query surface to Connect. | Specify configuration names, legal product/backend/listener combinations, and what HTTP statement/status routes do in DuckDB mode. Reject invalid combinations before accepting work. Show consistent product, backend, and capabilities in readiness, `/capabilities` where HTTP is enabled, and launcher checks. |
| Backend boundary | Reuse the server core's `EngineSessionHandle`, `ExecutionPort`, and `ConnectExecutionPort`, extending them only where the real adapters require it. Keep session identity, admission, and response/replay ownership in shared server code. | Trace session creation and close, Connect AnalyzePlan and ExecutePlan, SQL commands, streaming output, cancellation, and error mapping. Identify the Polars-specific composition and analysis paths still in use. Prove that the default Polars adapter and the DuckDB adapter can both use the same request lifecycle. |
| Database owner | Start with one configured writable database and one shared owner for the bidirectional fixture. Evaluate the shipped Quack owner path with the matching native DuckDB/Quack versions. Keep 4.19 read-only aliases separate from the execution catalog. | Prove that the selected owner supports bound queries, transactions, streaming results, and interruption through the actual driver. Record connection/session lifetimes, credentials, snapshot scope, supported platforms, and owner loss/restart behavior. Never use separate writable processes opening the same file as a substitute. |
| Client and semantics | Start from the 4.19 public client and Spark reference, both 4.0.5. Freeze a CPython environment, proto/dependency lock, Spark settings, and the operation/type matrix below. | Capture client request forms and reference schemas/results for representative reads and writes. List enabled session settings, intentional deviations, and rejection status for each unsupported request class. |
| Mutations and retries | Restrict writes to the frozen allowlist. Retain same-instance outcomes; require a separately proven ledger for cross-restart recovery. | Specify the public operation identity and supported status/retry path, commit witness, when an outcome is known or unknown, how unknown outcomes block unsafe retry, and how persisted tables survive owner/server restart. |
| Resource budgets | Start with existing Connect admission, response, and replay limits. Set explicit DuckDB connection/thread and query memory policies separately. | Record numeric queue and connection limits, wait/deadline behavior, statement and batch limits, memory/latency guardrails, and the workload and platform for each. Distinguish enforced limits from observed process RSS, and include the owner process in measurements. |

The 4.19 code reports `polars` from `ServerProduct::execution_backend()` and the Python launcher requires it in both readiness and capabilities. Make backend identity an explicit handshake expectation. Test both products in default mode, the DuckDB product in DuckDB mode, wrong product/backend, and readiness/capabilities disagreement. An unsupported selection or a failed startup probe must fail before exposing an endpoint.

## Delivery order and dependencies

Start 420-01 with a 4.19 baseline and a minimal path from an unmodified client to DuckDB. Then land read execution and analysis (420-02), followed by writes (420-03) and the full lifecycle/shared-owner matrix (420-04). Begin cancellation and ownership tests with the first read and write slices. Run 420-06 and the performance slices independently of DuckDB enablement. Run 420-05 on the integrated candidate after the backend and parallel work pass their gates. Slice IDs remain stable.

| Slice | Deliverable | Exit evidence |
| --- | --- | --- |
| 420-01 | Freeze D15 and compose backend selection | Record the 4.19 baseline. Show installed default Polars startup and a public Connect read in DuckDB mode that produces a backend execution witness and passes launcher negotiation. |
| 420-02 | Deliver read translation, analysis, and bounded Arrow results | V37 read matrix against the pinned client and reference, including empty results and exact logical schemas. Unsupported requests fail explicitly. |
| 420-03 | Add enumerated transactional writes and operation outcomes | Fault injection before and after commit distinguishes aborted, committed, and unknown outcomes. Duplicate or conflicting operation identities cannot cause a second effect. |
| 420-04 | Complete lifecycle and shared database interoperability | V38 bidirectional reads/writes through one owner, cancellation, reattach, release, saturation, and restart evidence. |
| 420-05 | Integrate and prepare coordinated publication | Complete the candidate checks below against the exact distributions. Repeat the public checks after publication. |
| 420-06 | Release the `sparkless-testing` schema comparison fix ([#1746](https://github.com/eddiethedean/robin-sparkless/issues/1746)) | Verify the existing source fix on Sparkless and PySpark, including matching and mismatching nested types, nullability, metadata when present, decimal parameters, and empty mapping expectations. Verify the installed helper works without a PySpark dependency; update the testing API docs when the corrected artifact is public. |

## Performance follow-ups

These slices improve the existing engine, HTTP, and Connect paths independently of enabling DuckDB execution. The slice IDs identify the work and allow the optimizations to be measured separately.

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
