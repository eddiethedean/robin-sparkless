# External catalog lookup

Sparkless exposes an engine-neutral catalog lookup contract for a backend that owns
external table registrations. The Polars backend reads local Delta snapshots and can
delegate table writes to a provider that implements the write hook.

## Public contract

The API is available from `robin_sparkless::engine` and `robin_sparkless_core::engine`:

```rust
pub trait ExternalCatalogProvider: Send + Sync {
    fn resolve_table(
        &self,
        identifier: &str,
    ) -> Result<Option<ExternalTableDescriptor>, EngineError>;

    fn write_table(
        &self,
        _request: ExternalTableWriteRequest,
    ) -> Result<ExternalTableWriteResult, EngineError> {
        Err(EngineError::User(
            "external catalog provider does not support table writes".into(),
        ))
    }
}

pub struct ExternalTableDescriptor {
    pub table_id: String,
    pub canonical_location: String,
    pub schema: StructType,
    pub version: Option<i64>,
    pub provider: String,
    pub permissions_context: Option<String>,
    pub format: ExternalTableFormat,
}
```

The optional `write_table` hook receives an Arrow IPC stream, save mode, writer options,
and partition columns. Its default implementation returns an unsupported error. A
provider must commit the write before returning a receipt for the committed snapshot.
The receipt must keep the stable table ID, canonical location, provider, and format and
identify a nondecreasing exact committed version. `DataFrameWriter.saveAsTable()` and
SQL `INSERT INTO` delegate writes when the identifier resolves externally. Session-local
tables and views shadow the external provider. A provider miss allows Sparkless' local
table path; provider errors and unsupported write hooks are surfaced without fallback.
`ErrorIfExists` rejects a resolved external table and `Ignore` leaves it unchanged.

The writer materializes the Sparkless DataFrame and buffers one Arrow IPC stream in
memory before calling the synchronous Rust provider. This API has no Python binding or
async transport yet. Path-based `DataFrameWriter.save()`, `insertInto()`, SQL
`UPDATE`/`DELETE`, and logical Map writes are not delegated by this hook. Logical Map
reads can match the Polars array-of-key/value-struct representation; writes carrying
Sparkless' logical Map metadata fail explicitly.

Attach a provider to a Rust `SparkSession` with
`session.set_external_catalog_provider(Some(Arc::new(provider)))`. Passing `None`
removes it. The provider receives the exact identifier supplied to `table()` or the
identifier resolved from a SQL relation. It owns namespace parsing and case rules.
Return `Ok(None)` only for a genuine miss; errors are surfaced and never retried as
local-name fallbacks.

Resolution checks existing global temporary views and exact session-local tables
first, then calls the provider, then tries the configured warehouse. When a provider
is installed, a missing multipart name does not fall back to a local table with the
same final component. `DataFrameReader.table()` and SQL relation reads use the same
session lookup path.

The descriptor must have a nonempty table ID, location, and provider name, plus a
nonnegative optional version. A Delta descriptor is read through Sparkless' native
Delta reader, configured path confinement, and a strict Delta snapshot path that does
not accept the plain-Parquet compatibility fallback. Builds without the `delta`
feature return an explicit error. The actual snapshot's field order, names, and types
must match the descriptor; nullability is not compared because the current Polars
schema adapter does not retain Delta nullability metadata. `permissions_context` is
opaque to Sparkless and must not carry credentials or secrets.

`SparkSession` clones share their resolver slot. `new_session()` creates an empty,
isolated slot. This is a synchronous Rust trait only; no Python callback, async/network
transport, or cross-thread callback guarantee is included.

## Availability

This change is developed against the `v4.13.4` source tree. Until a compatible
Sparkless release includes it, the API is available only to consumers building the
corresponding source revision. It is not part of the published `4.13.4` artifact.
