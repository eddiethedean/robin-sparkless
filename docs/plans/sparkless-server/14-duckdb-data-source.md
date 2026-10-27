# SS-14: DuckDB data source for Sparkless execution

> **Status: planned; owner unassigned.** Phase: 3, after supported Spark Connect. Target release: 4.19.0; optional capability.
> [Index](README.md) · [Roadmap](ROADMAP.md) · [Epic](EPIC.md) · [Decisions](DECISIONS.md) · [Validation](VALIDATION.md)

## Outcome and boundary

Allow the existing Polars execution backend to read named DuckDB tables through a native, bounded Arrow batch source. An unmodified Connect client can resolve a configured DuckDB table and run the supported Sparkless transformations over it. No Parquet export or separately synchronized table copy is required. Arrow transfer may allocate or copy data; no zero-copy or zero-transfer claim follows.

This phase adds a data source, not a DuckDB query execution backend. Sparkless/Polars still owns transformations and Spark semantics. DuckDB owns its storage and source transaction. Begin with read-only access: reject DuckDB-targeted DDL, appends, updates, deletes, overwrite, and table-save operations explicitly before effects. DuckDB execution and writes belong to [Phase 4 / SS-15](15-duckdb-connect-backend.md). Existing Sparkless-owned tables retain their current behavior.

## Prerequisites

- Phase 2 standalone Connect conformance in SS-09/10, supported endpoint lifecycle in SS-06, and applicable SS-04/07/08 resource/catalog/type contracts.
- Resolve D14 before implementation: pinned DuckDB library/driver versions, deployment ownership modes, relation naming, source snapshots, schema/Arrow types, and scan/resource limits.
- Current `ExternalCatalogProvider` is a starting point, not a complete scanner: `ExternalTableFormat` currently only describes Delta, while existing Arrow batch interfaces primarily handle writes. Extend the engine-neutral contract with typed read batches and source lifetime ownership; do not invent a DuckDB filesystem path that Polars can scan directly.

## Supported database ownership modes

Choose and verify an explicit mode for each advertised deployment:

1. **Standalone access:** Sparkless owns a configured DuckDB connection to a database with no independent writable process. Initially open source databases read-only. Report locked/incompatible files clearly; do not bypass locks or silently copy them.
2. **Live shared access:** An independent application can keep writing only through a proven shared database owner. Evaluate a native shared-process connection or a standard DuckDB database service/client, with compatible pinned versions and snapshot support. Both applications must reach the same owner. Merely mounting a writable `.duckdb` file into two processes does not establish a supported mode.

The native Sparkless process must remain Python/JVM-free. A shared-process approach cannot depend on Python callbacks in the native execution path. Use a self-contained writer/reader fixture to certify live sharing; support must not depend on a contribution to another application. A standalone scan does not certify concurrent writer interoperability. These modes are candidates until D14 records measured feasibility and selects the supported implementation.

## Ordered delivery

| Slice | Deliverable | Exit evidence |
| --- | --- | --- |
| 419-01 | Freeze D14 and prove standalone plus live-owner access feasibility | Exact versions, connection ownership, lock behavior, transaction/snapshot lifetime, failure policy |
| 419-02 | Resolve configured DuckDB catalogs/schemas/tables and analysis schemas | Stable table identity and snapshot; case/qualification rules; no name collision or silent fallback |
| 419-03 | Stream typed, cancellable Arrow batches into Polars scans | Empty/nonempty schemas and values; bounded batch buffers; no full-table collection during resolution |
| 419-04 | Run the supported Connect transformations against real DuckDB sources | Filters/projections/joins/aggregates/count/limit; truthful source errors; read-only enforcement |
| 419-05 | Complete V33/V34, opt-in packaging, capabilities, docs, and clean installs | Default Polars regressions; native image; source dependencies and supported platform matrix |

## Required semantics

- Resolve table metadata and begin a stable source snapshot together. Schema analysis and execution must not silently read incompatible versions. Hold the source transaction/lease until the actual producer stops, including after a response timeout or disconnect.
- Define one statement's snapshot across multiple DuckDB tables. Treat replacement/drop and concurrent append/update as controlled races. Cross-engine joins need an explicit statement-snapshot policy; reject combinations for which the promised consistency cannot be maintained.
- Freeze exact integer widths, BIGINT boundaries, decimal precision/scale, nullability, timestamps/time zones, and empty-result schemas. Unsupported DuckDB types fail before a misleading success. Nested types require separate enabled-type evidence.
- Start with conservative scans. Add projection/filter pushdown only where it preserves Sparkless semantics, especially casts, null comparison, decimal arithmetic, and collations. A DuckDB scan predicate must not change the result relative to Polars evaluation.
- Keep source transaction identity in the operation/plan lifetime. Reattachment replays retained results rather than rereading a newer snapshot. On source loss, fail truthfully; do not reconnect and silently rerun against changed data.
- Bound scan queues, batch bytes, connection count, source timeouts, and retained snapshots. Measure DuckDB and Polars memory together, including intermediates and replay; Arrow batches alone do not prove bounded total memory. Health, cancellation, and shutdown must progress during a scan.
- Configure sources through trusted server/deployment settings. Client SQL must not choose arbitrary database files, extensions, credentials, or remote destinations. Use structured identifiers and bound values when interacting with DuckDB.

## Acceptance and evidence

- **V33:** a pinned unmodified Connect client reads real DuckDB tables, computes the advertised transformation corpus through Polars, preserves values/schema, and receives explicit errors for unsupported types and source writes. Test empty tables and joins as well as scalar smoke queries.
- **V34:** controlled concurrent source writes/replacements demonstrate snapshots; cancellation, disconnect, reattach, source loss, and shutdown preserve ownership. Increasing source sizes demonstrate numeric allocation/RSS/queue budgets recorded in D14. An independent DuckDB writer and a Connect reader share one database owner and observe the declared commit visibility without export or synchronization.
- Record source/library/client revisions, schemas, commands, source sizes, platforms, latency/RSS, and limits. If live sharing fails feasibility, keep its gate open and revise advertised scope explicitly instead of claiming live shared-table support from a standalone scan.
- Keep the feature optional and verify a default installation without DuckDB dependencies. Package/install the enabled native binary and image on each advertised platform. Apply the coordinated 4.19.0 publication policy only after all phase exits and previous regressions pass.

## Touchpoints and references

- [External catalog contract](../../../crates/robin-sparkless-core/src/engine/external_catalog.rs)
- [Backend interfaces](../../../crates/robin-sparkless-core/src/engine/mod.rs)
- [Polars sessions](../../../crates/robin-sparkless-polars/src/session/mod.rs)
- [Server runtime](../../../crates/sparkless-server/src/lib.rs)
- [DuckDB concurrency](https://duckdb.org/docs/stable/connect/concurrency): verify the selected version's embedded/service ownership rules; documentation is not a compatibility pin.

Python workers, distributed query execution, application-specific integration protocols, durable source caches, and a DuckDB execution backend are outside this phase.
