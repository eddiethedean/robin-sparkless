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
| `robin-sparkless-polars` | Concrete Polars frames and readers, Polars query compilation/execution, Delta/JDBC access, Polars error conversion, source composition |
| `robin-sparkless-duckdb` | Configured DuckDB source resolution, read snapshots, bounded scans, source connection lifecycle; reads only |
| `robin-sparkless` | Concrete Polars-backed Rust facade and core/Polars exports |
| `sparkless-native` | Python ABI, conversion, Python callback ownership, and Python exception mapping |
| `sparkless-server-core` | Transport-independent server contracts and state records |
| `sparkless-server-polars` | Existing runtime and HTTP/Connect composition over the Polars session adapter |
| `sparkless-server`, `sparkless-server-duckdb` | Product binaries and product-specific composition |

The server runtime and transport extraction remains Phase 5 work. The DuckDB server
product currently adds the read-only source capability; a source provider does not
imply DuckDB writes or DuckDB query execution.

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
view. `CatalogWriter` and `CatalogBatchWriter` are optional, separate ports. Writes
prepare before commit and return `Committed`, `NotCommitted`, or `OutcomeUnknown`, so
callers do not treat a lost response as rollback. Python callbacks use the same
operation ID and outcome contract. DuckDB source owners advertise read only.

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

Phase 1 module decomposition and Phase 2 contract source changes are implemented;
their targeted behavioral gates and the P0 inventory closeout remain pending. Phase 3
adds the portable plan and shared SQL crates. Its targeted corpus verifies matching
values, schemas, and missing-column errors across JSON, local SQL, and Connect, plus
rejection of malformed plans before Connect source scans. Runtime extraction and final
release acceptance remain later phases of the 4.19 plan.
