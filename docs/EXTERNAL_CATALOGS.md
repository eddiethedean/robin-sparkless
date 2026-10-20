# External catalog lookup

Sparkless exposes an engine-neutral catalog lookup contract for a backend that owns
external table registrations. The initial implementation reads local Delta snapshots;
it does not implement catalog persistence, authorization, or writes.

## Public contract

The API is available from `robin_sparkless::engine` and `robin_sparkless_core::engine`:

```rust
pub trait ExternalCatalogProvider: Send + Sync {
    fn resolve_table(
        &self,
        identifier: &str,
    ) -> Result<Option<ExternalTableDescriptor>, EngineError>;
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
Delta reader and configured path confinement. Builds without the `delta` feature
return an explicit error. The actual snapshot's field order, names, and types must
match the descriptor; nullability is not compared because the current Polars schema
adapter does not retain Delta nullability metadata. `permissions_context` is opaque
to Sparkless and must not carry credentials or secrets.

`SparkSession` clones share their resolver slot. `new_session()` creates an empty,
isolated slot. This is a synchronous Rust trait only; no Python callback, async/network
transport, write delegation, or cross-thread callback guarantee is included.

## Availability

This change is developed against the `v4.13.4` source tree. Until a compatible
Sparkless release includes it, the API is available only to consumers building the
corresponding source revision. It is not part of the published `4.13.4` artifact.
