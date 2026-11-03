# Migrating to Sparkless 4.19

Sparkless 4.19 is a breaking release. This guide covers the Phase 2 core-contract
changes. Later 4.19 refactor phases may add further migration notes here.

## Rust engine contracts

`DataFrameBackend` and its boxed, `as_any`-based frame handoff have been removed.
Implement the narrow `FrameTransform`, `FrameActions`, `FrameSchema`, and
`GroupedData` capabilities for a backend frame. Transformations and binary
operations now preserve the associated concrete frame type. `DataFrameReaderBackend`
and `SparkSessionBackend` use associated `Frame` and `Reader` types as well.

`SparkSessionBackend::create_dataframe(Vec<(i64, i64, String)>, names)` has been
removed. Use the general row constructor, which accepts JSON-compatible values and
an explicit schema:

```rust,ignore
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

The combined `ExternalCatalogProvider` trait is removed. Compose an ownership/table
`CatalogResolver` with optional `CatalogReader`, `CatalogWriter`, and
`CatalogBatchWriter` capabilities:

```rust,ignore
let resolver: Arc<dyn CatalogResolver> = Arc::new(my_resolver);
let reader: Option<Arc<dyn CatalogReader>> = Some(Arc::new(my_reader));
let writer: Option<Arc<dyn CatalogWriter>> = Some(Arc::new(my_writer));
let catalog = Arc::new(ExternalCatalog::new(resolver, reader, writer, None));
session.set_external_catalog(Some(catalog))?;
```

`CatalogResolver::resolve_ownership` is called before table lookup. Return
`CatalogOwnership::Local` only when the local catalog may handle the identifier;
return `CatalogOwnership::external(owner_id, capabilities)` for every identifier
inside a provider-owned namespace, even when that particular table does not exist.
The latter prevents a local table from shadowing a missing external table. Missing
tables use `Ok(None)`; provider failures remain errors. Read and write capability
absence is represented explicitly, and write preparation now returns a
`PreparedCatalogWrite` whose commit reports `Committed`, `NotCommitted`, or
`OutcomeUnknown`.

Existing DuckDB source registrations remain read-only. DuckDB write support is a
future capability and is not enabled by this contract migration.

## Python catalog providers

Python providers must now implement `resolve_ownership` as well as `resolve_table`.
Return `None` to delegate an identifier to the local catalog. Return
`ExternalCatalogOwnership` to claim it and advertise capabilities:

```python
from sparkless.external_catalog import ExternalCatalogOwnership

class Catalog:
    def resolve_ownership(self, identifier):
        if identifier.startswith("analytics."):
            return ExternalCatalogOwnership(
                owner_id="analytics",
                read=True,
                write=True,
            )
        return None

    def resolve_table(self, identifier):
        # Return ExternalTableDescriptor or None.
        ...
```

`write_table` and `write_table_batches` remain optional and are exposed as separate
capabilities. A callback exception after the provider call starts has an unknown
commit outcome; reconcile it with the supplied `operation_id` before retrying.

## Execution context and errors

Use `SparkSession::execution_context` and `sql_with_context` when an operation must
carry a stable operation ID, cancellation token, settings snapshot, or source snapshot.
The convenience `sql` and `sql_with_cancellation` methods construct a context for
callers that do not need to provide one.

`EngineError` is a structured type rather than an enum. Replace enum-variant matches
and message parsing with `category()`, `code()`, `retryability()`, `operation()`, and
`detail()`:

```rust,ignore
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
