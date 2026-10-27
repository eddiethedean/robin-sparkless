# SS-15: Optional DuckDB execution backend for Spark Connect

> **Status: planned; owner unassigned.** Phase: 4, after the supported Connect server and DuckDB data-source phase. Target release: 4.20.0; optional capability.
> [Index](README.md) · [Roadmap](ROADMAP.md) · [Epic](EPIC.md) · [Decisions](DECISIONS.md) · [Validation](VALIDATION.md)

## Outcome and distinction from Phase 3

Expose an optional DuckDB execution backend through Sparkless Server's existing Spark Connect endpoint. The same unmodified Connect client can submit the declared SQL/DataFrame subset, but DuckDB now executes the supported plans and owns persistent table writes. Phase 3 only supplied DuckDB table batches to Polars; this phase performs the query inside DuckDB.

Polars remains the default engine. Choose an execution engine explicitly when configuring a server instance, bind that choice for its sessions/operations, and expose a truthful capability matrix. Do not silently switch engines for unsupported operations or after a statement may have taken effect. Backend selection uses Sparkless server configuration and the standard Connect contract.

This is a native single-node backend, not Apache Spark or general Spark compatibility. The server and enabled image remain Python/JVM-free. Existing DuckDB data is usable only through the database-owner modes certified in Phase 3; a Connect URL does not grant safe access to another process's database files.

## Prerequisites and decisions

- Phase 2 SS-09/10 Connect translation, analysis, Arrow, replay, session, interruption, and release conformance.
- Phase 3 SS-14/D14 native DuckDB dependencies, ownership, names, snapshots, types, and measured resource behavior; V33/V34 completed for advertised shared data.
- Resolve D15 before implementation: server/backend selection, engine-neutral plan interfaces, Spark SQL translation, supported expressions/commands, analysis/execution schema agreement, transaction/outcome persistence, and backend-specific resource accounting.
- Existing engine traits provide a boundary to assess, not proof that the server is backend-neutral. Audit server construction, shared catalog assumptions, Polars-only APIs, logical schema sidecars, and cancellation hooks before reusing them.

## Ordered delivery

| Slice | Deliverable | Exit evidence |
| --- | --- | --- |
| 420-01 | Freeze D15 and introduce explicit backend selection/lifecycle | Default Polars path still passes; optional DuckDB build/install/start; engine identity is stable |
| 420-02 | Implement DuckDB plan translation, analysis, and bounded Arrow results | Same declared SQL/DataFrame corpus and schemas through the pinned Connect client |
| 420-03 | Add enumerated transactional writes and truthful operation outcomes | Atomic effects/receipts, duplicate/conflict/deadline/disconnect/restart evidence; unsupported writes fail before effects |
| 420-04 | Complete Connect lifecycle and shared-database interoperability | Cancellation/reattach/release/saturation plus independent DuckDB client read/write visibility through one owner |
| 420-05 | Complete V35/V36, backend documentation, benchmarks, and distribution | Numeric budgets and capability matrix; default regressions; fresh enabled/disabled installs |

## SQL, plans, and type compatibility

Translate the frozen Connect relations/expressions and supported Spark SQL into engine-neutral operations or validated DuckDB plans/SQL. A `spark.sql(...)` query and a DataFrame chain must both obey the advertised semantic profile. Passing arbitrary Spark SQL straight to DuckDB is not evidence of compatibility.

Cover identifier/case resolution, aliases and duplicate labels, filters, projections, joins, aggregates, sort/limit, nulls, checked casts/overflow, decimals, and date/time behavior for the enabled subset. Use explicit type mappings and compare AnalyzePlan schemas with actual Arrow results, including zero rows. Unsupported functions, configurations, UDFs, RDD APIs, or storage operations must fail clearly. Pin a reference Spark version and document every intentional deviation; DuckDB dialect support must not be mislabeled Spark semantics.

Keep catalog/table and session namespace ownership explicit. Temporary views/configuration belong to their session; shared saved tables have the selected database's persistence and transaction behavior. Mixed Polars/DuckDB execution within one plan is outside the initial backend and must not appear as an automatic fallback.

## Transactions and operation ownership

Enumerate the initial DDL/append/write commands and prove atomicity before enabling them. Reuse the server's operation identity and retained-result lifecycle, while binding each operation to its backend, database scope, session, and semantic context. A stable replay must not perform another write.

DuckDB tables may survive a Sparkless restart, unlike the original ephemeral Polars server catalog. Document that difference explicitly. Commit acknowledgement, cancellation, and response delivery remain separate events. Reserve outcome capacity before effects; obtain authoritative commit outcomes from the database owner. Do not claim a database rollback when only the client connection or response was lost.

D15 must choose and prove whether receipts are recorded transactionally with the data or whether some disconnect/crash cases remain unresolved and block automatic retry. Same-instance deduplication does not imply a durable exactly-once guarantee. A new instance rejects old-instance retries even when durable table data survives. Cross-restart replay requires a separately specified transactional receipt ledger and recovery proof; it is not assumed by this phase.

Cancel/interrupt requests must reach the actual DuckDB producer. Keep its connection, transaction, compute permit, and buffers owned until it terminates. Once data commits, retain the committed outcome regardless of a later cancel, close, timeout, or disconnect. Reattachment replays retained Arrow results/receipts without rerunning a query. Source/service failures must not fall back to a local database or a second engine.

## Independent client interoperability

Use an in-repository fixture with a standard DuckDB client and Sparkless Connect client. Prove that both paths use the same DuckDB owner, canonical catalogs, and transaction visibility. Create/append through one interface and read through the other in both directions. Cover concurrent reads/writes, SQL-created tables, schema changes, temporary-state isolation, errors, restart, and canceled or disconnected writes. Use supported generic database connection settings. No external application change or maintainer acceptance is a prerequisite. A copy, export, or periodic synchronization cannot serve as evidence of shared live tables.

## Acceptance and release gates

- **V35:** the declared SQL and DataFrame matrix runs through the unmodified Connect client using DuckDB computation, with reference-semantic/value/schema checks, truthful unsupported behavior, and optional/default installation evidence. The Polars backend's supported surface continues to pass.
- **V36:** transactional effects and receipts, repeated/changed operation identities, source close/loss, deadline/commit races, cancellation, result replay, session release, and restart match documented guarantees. Independent client interoperability uses one database owner; multi-client benchmarks validate results and numeric RSS/latency/queue bounds while health progresses.
- Apply all applicable V19–V24 Connect lifecycle requirements to the enabled backend, not only the original Polars implementation. Any separately advertised third-party route requires supplemental V25 certification and does not gate the standalone backend release.
- Record backend/client/reference revisions, settings, commands, platform dependencies, artifact hashes, and known deviations. Shared database service processes belong in end-to-end resource measurements.
- Publish all first-party artifacts at coordinated 4.20.0 only after the complete phase gates, previous regressions, isolated installs, and public-artifact checks pass. Optional DuckDB dependencies remain absent from default builds/images.

## Touchpoints

- [Core engine interfaces](../../../crates/robin-sparkless-core/src/engine/mod.rs)
- [Server runtime and construction](../../../crates/sparkless-server/src/lib.rs)
- [Connect translation](09-connect-plans-and-analysis.md) and [lifecycle conformance](10-arrow-replay-and-connect-conformance.md)
- [DuckDB source ownership and schema decisions](14-duckdb-data-source.md)

Distributed execution, arbitrary Python workers, universal Spark compatibility, cross-engine federation, and native Delta/UC writes outside the frozen backend matrix remain separate work.
