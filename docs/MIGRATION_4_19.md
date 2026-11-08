# Migrating to Sparkless 4.19

Sparkless 4.19 is a breaking release. This guide covers the Phase 2 core-contract,
Phase 3 plan/SQL, Phase 4 storage, Phase 5 server, and Phase 6 Python package changes.
For Python application imports and installation examples, see the fuller
[Python migration guide](python_migration.md).

## Rust engine contracts

`DataFrameBackend` and its boxed, `as_any`-based frame handoff have been removed.
Implement the narrow `FrameTransform`, `FrameActions`, `FrameSchema`, and
`GroupedData` capabilities for a backend frame. Transformations and binary
operations now preserve the associated concrete frame type. `DataFrameReaderBackend`
and `SparkSessionBackend` use associated `Frame` and `Reader` types as well.

`SparkSessionBackend::create_dataframe(Vec<(i64, i64, String)>, names)` has been
removed. Use the general row constructor, which accepts JSON-compatible values and
an explicit schema:

```rust
session.create_dataframe_from_rows(
    vec![vec![serde_json::json!(7), serde_json::json!("Ada")]],
    vec![("id".into(), "long".into()), ("name".into(), "string".into())],
    true,
    false,
)?;
```

`set_external_catalog_provider` on the Rust session contract is replaced with
`set_external_catalog(Option<Arc<ExternalCatalog>>)`. Python keeps the familiar
`set_external_catalog_provider` method name.

## Catalog ownership and capabilities

The combined `ExternalCatalogProvider` trait and materialized writer are removed.
Compose an ownership/table `CatalogResolver` with optional `CatalogReader` and
`CatalogBatchWriter` capabilities. Every write capability uses bounded Arrow IPC
batches:

```rust
let resolver: Arc<dyn CatalogResolver> = Arc::new(my_resolver);
let reader: Option<Arc<dyn CatalogReader>> = Some(Arc::new(my_reader));
let writer: Option<Arc<dyn CatalogBatchWriter>> = Some(Arc::new(my_writer));
let catalog = Arc::new(ExternalCatalog::new(resolver, reader, writer));
session.set_external_catalog(Some(catalog))?;
```

`CatalogResolver::resolve_ownership` is called before table lookup. Return
`CatalogOwnership::Local` only when the local catalog may handle the identifier;
return `CatalogOwnership::external(owner_id, capabilities)` for every identifier
inside a provider-owned namespace, even when that particular table does not exist.
The latter prevents a local table from shadowing a missing external table. Missing
tables use `Ok(None)`; provider failures remain errors. Read and write capability
absence is represented explicitly. `write=true` requires `batch_write=true`, and
write preparation now returns a
`PreparedCatalogWrite` whose commit reports `Committed`, `NotCommitted`, or
`OutcomeUnknown`.

Existing DuckDB source registrations remain read-only. DuckDB write support is a
future capability and is not enabled by this contract migration.

## Storage crate boundaries

Delta path handling, snapshot loading, table creation, and transaction commits now live
in `robin-sparkless-delta`; Polars continues to own schema and Arrow/Polars conversion.
JDBC options and SQL identifier policy live in `robin-sparkless-jdbc`, while concrete
database connections and Polars conversion live in `robin-sparkless-jdbc-polars`.
The existing JDBC feature names remain on `robin-sparkless-polars` and forward to the
driver adapter. Direct users of the new adapter crate select its matching database
feature (`postgres`, `sqlite`, `mysql`, `mariadb`, `mssql`, `oracle`, or `db2`).

## Python catalog providers

Python providers must now implement `resolve_ownership` as well as `resolve_table`.
Return `None` to delegate an identifier to the local catalog. Return
`ExternalCatalogOwnership` to claim it and advertise capabilities. A writable owner
sets both `write=True` and `batch_write=True`:

```python
from sparkless.external_catalog import ExternalCatalogOwnership

class Catalog:
    def resolve_ownership(self, identifier):
        if identifier.startswith("analytics."):
            return ExternalCatalogOwnership(
                owner_id="analytics",
                read=True,
                write=True,
                batch_write=True,
            )
        return None

    def resolve_table(self, identifier):
        # Return ExternalTableDescriptor or None.
        ...
```

`write_table_batches` is the only write callback. It receives a lazy, bounded
`request.batches` iterator; materialized `write_table` callbacks are removed. A
callback exception after the provider call starts has an unknown commit outcome;
reconcile it with the supplied `operation_id` before retrying.

## Execution context and errors

Use `SparkSession::execution_context` and `sql_with_context` when an operation must
carry a stable operation ID, cancellation token, settings snapshot, or source snapshot.
The convenience `sql` and `sql_with_cancellation` methods construct a context for
callers that do not need to provide one.
An explicit context belongs to the session that captured it. If a provider callback
enters a different session, that session uses its own settings and source snapshot.
Nested SQL restores the outer operation's thread settings when it returns, including
on error.

`EngineError` is a structured type rather than an enum. Replace enum-variant matches
and message parsing with `category()`, `code()`, `retryability()`, `operation()`, and
`detail()`:

```rust
match error.category() {
    ErrorCategory::Cancelled => handle_cancel(),
    ErrorCategory::Provider if error.code() == "WRITE_OUTCOME_UNKNOWN" => reconcile(),
    _ => report(error.detail()),
}
```

The old constructor-shaped names are removed as well: use `invalid_input`, `internal`,
`io`, `sql`, `not_found`, `cancelled`, and `other` instead of `User`, `Internal`, `Io`,
`Sql`, `NotFound`, `Cancelled`, and `Other`.

Python exceptions expose `category`, `code`, `operation`, `retryability`, `metadata`,
`cause`, and `error_detail` attributes when the underlying boundary has structured
error information. Continue catching `SparklessError` (or
`SparklessExternalCatalogError` for provider callback failures) for broad handling.

## Typed logical plans

`execute_plan` remains the JSON compatibility boundary, and it decodes and validates
every operation before execution. Rust callers that retain a plan can decode it once
with `robin_sparkless_plan::LogicalPlan::decode` and pass it to `execute_typed_plan`.
Unsupported operation names return `UNSUPPORTED_PLAN_OPERATION`; malformed operations
return `INVALID_PLAN` before the Polars executor starts. Decoded operations expose an
operation-specific `PlanPayload`; expressions are available as `PlanExpression` trees,
while the executor-specific JSON spelling is available from `payload_json()`. The
Polars adapter dispatches directly on typed operations instead of converting each
operation back to a JSON payload.

The new `robin-sparkless-sql` crate owns SQL-to-relational normalization through
`lower_query`. Its `SqlQueryPlan` uses engine-neutral relations, projections, filters,
grouping keys, sort keys, offsets, limits and set operations. Adapters retain
responsibility for expression execution and for enforcing their own supported subset.
Local SQL consumes the normalized sort, offset, and limit clauses. Server SQL validation lowers the query
before applying its HTTP-specific policy. `lower_statement` separates read queries and
metadata commands from effectful commands; command plans classify DDL/DML families,
retain targets and nested queries where available, and preserve the canonical SQL for
adapter-specific options. Shared identifier and numeric-promotion helpers live below
the SQL and Connect adapters.

When a Polars SELECT is reconstructed from the normalized plan, the canonical
string literals preserve the values decoded by Spark SQL parsing. This applies to
both single-quoted and double-quoted literals, including embedded quotes and backslashes.

## Server crates and product selection

The server library is split by responsibility. `sparkless-server-core` owns
engine-neutral server contracts and product identity; `sparkless-server-runtime`
owns admission, sessions, and operation state; `sparkless-server-http` and
`sparkless-server-connect` own their transports; and `sparkless-server-polars`
composes the Polars application. Product consumers should depend on
`sparkless-server` for the base product or `sparkless-server-duckdb` for the
source-enabled product, rather than reaching into the former monolithic server
implementation. The DuckDB product uses the same Polars execution path and adds
read-only DuckDB sources; it does not execute queries in DuckDB or enable writes.

Custom Connect execution-port adapters receive `ConnectExecutionRequest.plan` as
a `ConnectExecutionPlan`. Return the output `StructType` and `ArrowIpcBatchStream`
in `ConnectExecutionResult`. The transport encodes the logical schema for the wire.

The product crates re-export the server application types. Embedders that need a
specific product identity can construct it explicitly:

```rust
use sparkless_server::{Server, ServerConfig, ServerProduct};

let server = Server::new_for_product(config, ServerProduct::Base)?;
```

Use `ServerProduct::DuckDb` with the DuckDB product crate when composing that
product. Select HTTP and Connect features explicitly for the listeners the
application needs. Lower-level runtime and transport crates are available for
integrations that implement their own composition; those consumers now supply the
execution and application contracts instead of depending on Polars policy inside
the transport.

## Python package and import changes

The launcher, product binaries, local extension, and test helpers now have separate
owners. Install only the packages needed by the application:

```bash
python -m pip install "sparkless-connect[client]==4.19.0"
python -m pip install "sparkless-server[connect]==4.19.0"
python -m pip install "sparkless-server-duckdb[connect]==4.19.0"
python -m pip install "sparkless[testing]==4.19.0"
```

The first command provides the shared launcher and pinned public Connect client,
without a local engine or server binary. The next two commands install one
product-bound server and its client. The last installs backend-independent test
fixtures through the separate `sparkless-testing` distribution. Use a separate
environment when selecting a mutually exclusive PySpark oracle version.

Replace removed imports as follows:

| Removed in 4.19 | Replacement |
| --- | --- |
| `from sparkless.server import SparklessServer` | `from sparkless_server import SparklessServer` for the base product, or `from sparkless_server_duckdb import SparklessServer` for the source-enabled product |
| `from sparkless.server import AsyncSparklessServer` | Import from `sparkless_server` or `sparkless_server_duckdb` to bind to a product. Use `sparkless_connect` for the shared launcher. |
| `import sparkless.testing` | `import sparkless_testing` |
| `pytest_plugins = ["sparkless.testing"]` | `pytest_plugins = ["sparkless_testing.plugin"]` to register fixtures and hooks |
| `sparkless._native` | `sparkless_native._native` for integrations that intentionally use the internal extension |
| `sparkless-server-bin` | `sparkless-server` or `sparkless-server-duckdb`, according to the required product |

`SparklessConnect` is new in 4.19. It owns both the server process and a new
public client session.
Its product-bound import determines the executable. The shared
`sparkless_connect.SparklessServer` can use an explicit executable path or a
product selection; it does not search `PATH`, download a binary, or infer that the
DuckDB product should be selected just because it is installed. If both server
products are installed, import the desired product helper explicitly. Ordinary
local applications should continue importing `sparkless` and should not import
the private native extension directly.

The functions API remains `sparkless.sql.functions`; its private implementation
modules are not a new application import surface. Likewise, launcher users should
import from `sparkless_connect` or a product package rather than the new private
launcher modules.
