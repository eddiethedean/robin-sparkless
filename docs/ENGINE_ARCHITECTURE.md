# Engine architecture

The local Rust API is Polars-backed. Engine-neutral schemas, expressions, execution
context, errors, and storage capability contracts live in `robin-sparkless-core`;
portable DataFrame operation plans live in `robin-sparkless-plan`; shared SQL parsing
and relational/command lowering live in `robin-sparkless-sql`; Polars execution,
physical query compilation, and type conversion live in `robin-sparkless-polars`. The
root `robin-sparkless` crate composes those layers and exposes the familiar
`SparkSession` and `DataFrame` API.

## Crate ownership

| Crate | Owns |
| --- | --- |
| `robin-sparkless-core` | Schemas, expression IR, cancellation, `ExecutionContext`, structured `EngineError`, typed frame/session/reader contracts, catalog ownership/read/write ports |
| `robin-sparkless-plan` | Portable DataFrame operations, structural JSON decoding, expression validation, shared name resolution, numeric promotion |
| `robin-sparkless-sql` | Shared SQL parser entry points, relational and command plans, statement effects, identifier rules |
| `robin-sparkless-polars` | Concrete Polars frames and readers, Polars query compilation/execution and conversion, Polars error conversion, source composition |
| `robin-sparkless-delta` | Delta path normalization, snapshot loading, table creation, and transaction commits; no Polars dependency |
| `robin-sparkless-jdbc` | Driver-neutral JDBC option parsing, URL matching, SQL identifier/dialect policy, and credential redaction |
| `robin-sparkless-jdbc-polars` | Feature-gated database connections, JDBC reads/writes, and database-to-Polars conversion |
| `robin-sparkless-duckdb` | Configured DuckDB source reads, the optional Connect execution owner, bounded Arrow streams, transactional writes, and operation receipts |
| `robin-sparkless` | Concrete Polars-backed Rust facade and core/Polars exports |
| `sparkless-native` | Python ABI, conversion, Python callback ownership, and Python exception mapping |
| `sparkless-server-core` | Engine-neutral server configuration and SQL/Connect execution-port contracts |
| `sparkless-server-runtime` | Execution-port dispatch, connection/admission permits, operation registry, and generic session registry |
| `sparkless-server-http` | HTTP/1 listener, route dispatch, JSON header validation, and bounded body reads |
| `sparkless-server-connect` | Protobuf build, bounded wire decoding, generated gRPC service dispatch/listener, replay/session history, sender accounting, and drain state |
| `sparkless-server-polars` | Product-specific HTTP and Connect handlers, Polars execution adapters, schema conversion, and server composition |
| `sparkless-server`, `sparkless-server-duckdb` | Product binaries and product-specific composition |

HTTP and Connect requests reach the same engine-neutral runtime; it owns admission,
operation accounting, and the generic session registry while product composition
supplies the session adapter and product-specific RPC handlers. Connect's protocol
build, wire limits, generated service dispatch, listener, replay records, session history, and transport budgets
live in the Connect crate. The DuckDB server product includes the read-only source
capability and can opt into its DuckDB execution adapter through trusted startup
configuration. Source aliases and the writable execution database remain separate;
the base product does not compile or ship DuckDB/Quack.

The Connect execution port accepts a typed `ConnectExecutionPlan` and returns a
logical schema with Arrow IPC batches. The Polars adapter converts the incoming wire
plan to that core type before dispatch. It currently reconstructs adapter-private
protobuf nodes to reuse the checked translator.

## Core engine contracts

The core frame contract is separated by responsibility:

- `FrameTransform` builds lazy query operations and keeps associated concrete frame
  types for transformations and joins.
- `FrameActions` materializes rows/counts and accepts cancellation or an
  `ExecutionContext` at action boundaries.
- `FrameSchema` exposes metadata without collecting rows.
- `GroupedData` owns aggregation operations for its associated frame type.
- `DataFrameReaderBackend` and `SparkSessionBackend` associate their reader/frame
  types with the selected backend instead of erasing and downcasting them.

`ExecutionContext` carries an operation ID, immutable settings snapshot, cancellation
token, and optional catalog source snapshot. Session convenience APIs build this
context automatically; embedders can supply one with `sql_with_context`.
Only the owning session reads an active context's settings, cancellation token, and
source snapshot. SQL translation scopes thread settings and restores them when it
returns, so provider callbacks can run SQL in another session.

`EngineError` is a structured value with category, stable code, retryability, operation,
metadata, and cause. Rust callers branch on `category()` and `code()` rather than
matching enum variants or parsing messages. The Python and HTTP boundaries retain an
`error_detail` object when the incoming error has structured detail. Connect maps error
categories to gRPC status codes and includes the stable engine code in the status
message.

## Catalog ownership and capabilities

`CatalogResolver` resolves an identifier's owner before metadata. `Local` permits
Sparkless' local catalog to handle the identifier; `External` pins an owner and its
read/write/batch-write capabilities. An external miss or provider error never falls
through to a local name with the same suffix.

`CatalogReader` starts a snapshot; the snapshot resolves metadata and scans under that
view. `CatalogBatchWriter` is the optional write port and accepts bounded typed Arrow
IPC batches only. Writes prepare before commit and return `Committed`, `NotCommitted`,
or `OutcomeUnknown`, so callers do not treat a lost response as rollback. Python
callbacks use the same operation ID and outcome contract. DuckDB source owners
advertise read only.

See [external catalogs](EXTERNAL_CATALOGS.md), [4.19 migration](MIGRATION_4_19.md),
and the [4.19 SOLID plan](plans/solid-refactor-4.19.md) for implementation and
breaking-change details.

## Extension rules

- Keep schemas, errors, expression IR, cancellation, and capability contracts free of
  concrete Polars types.
- Keep physical optimization and Polars `Expr` behavior in the Polars crate.
- Add a new provider capability only when a concrete consumer uses it; a reader does
  not imply a writer, and a writer does not imply a query engine.
- Preserve capability absence, namespace ownership, snapshot identity, cancellation
  safe points, and write outcomes in provider implementations.
- Add backend behavior to the core only when it has a stable, engine-neutral meaning.

## Phase status

P0 inventory, P1/P2, and local P4 contract checks passed on 2026-11-02. Live JDBC
integration remains open. See the
[4.19 SOLID plan](plans/solid-refactor-4.19.md#phase-419-review-follow-up)
for the current Connect boundary and local review evidence.
Phase 3's targeted corpus verifies matching values, schemas, and missing-column errors
across JSON, local SQL, and Connect, plus rejection of malformed plans before Connect
source scans. Phase 5's HTTP-only, Connect-only, and combined feature matrix has been
checked. The [release evidence](plans/sparkless-server/RELEASE_4_19_EVIDENCE.md)
tracks product acceptance and records which source revisions and artifacts were tested.
