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
memory before calling the synchronous provider. Python providers use the PyO3 adapter
described below. There is no async transport. Path-based `DataFrameWriter.save()`, `insertInto()`, SQL
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
isolated slot. Rust and Python callbacks are synchronous; async/network transport is
the provider's responsibility.

## Python providers

Python sessions expose `set_external_catalog_provider(provider)` and the camelCase
alias `setExternalCatalogProvider(provider)`. Pass `None` to detach. Providers implement
`resolve_table(identifier)` and may also implement `write_table(request)`:

```python
from sparkless.external_catalog import ExternalTableDescriptor
from sparkless.sql.types import LongType, StructField, StructType

class Catalog:
    def resolve_table(self, identifier):
        if identifier != "main.analytics.orders":
            return None
        return ExternalTableDescriptor(
            table_id="orders-123",
            canonical_location="/data/orders",
            schema=StructType([StructField("id", LongType())]),
            provider="my-catalog",
            version=None,  # Read the latest committed Delta version.
        )

spark.set_external_catalog_provider(Catalog())
orders = spark.table("main.analytics.orders")
```

`sparkless.external_catalog` exports the `ExternalCatalogProvider` and
`WritableExternalCatalogProvider` typing protocols, the `ExternalTableFormat` and
`ExternalTableSaveMode` string enums, `ArrowIpcStream`, and the
`ExternalTableDescriptor`, `ExternalTableWriteRequest`, and `ExternalTableWriteResult`
dataclasses. A read-only object may omit `write_table`;
attempted writes then raise an explicit unsupported error.

The write callback receives the exact identifier, resolved descriptor, save mode
(`"append"` or `"overwrite"`), optional format, ordered option pairs (including
duplicates), and partition column names. `request.data` is an `ArrowIpcStream`, a
Python `bytes` subclass with a `.bytes` property matching the Rust payload field.
For example, `pyarrow.ipc.open_stream(request.data).read_all()`
decodes it without a JSON row conversion. PyArrow is optional and is not imported by
the adapter. After committing through its storage backend, the callback must return
`ExternalTableWriteResult(table=committed_descriptor, committed_version=version)`.
The native backend validates the receipt's identity and version before reporting
success. A callback exception, invalid descriptor, or invalid receipt is surfaced as
a Sparkless error and never triggers a local fallback.

Descriptor schemas use Sparkless `StructType` objects and preserve integer widths,
decimal precision/scale, nested arrays/maps/structs, and field nullability through
the binding. The supported primitive types are String, Integer, Short, Long, Double,
Boolean, Date, Timestamp, and Binary; unsupported schema types fail explicitly.

The adapter owns a strong reference to the Python provider for as long as it is
attached. Callbacks acquire the Python GIL, including when entered from Rust.
Existing readers share the session's provider slot. `newSession()` starts without a
provider; detaching or stopping the Python session releases its provider reference.

## Rust and Python API coverage

Every public catalog operation introduced by this PR has a Python counterpart:

| Rust API or behavior | Python API |
| --- | --- |
| `ExternalCatalogProvider::resolve_table` | `ExternalCatalogProvider.resolve_table(identifier)` |
| `ExternalCatalogProvider::write_table` | Optional `write_table(request)`, typed by `WritableExternalCatalogProvider` |
| `ExternalTableDescriptor` and every field | `ExternalTableDescriptor` dataclass, using `StructType` for the schema |
| `ExternalTableDescriptor::validate` | `descriptor.validate()`, invoking the Rust validator |
| `ExternalTableFormat::Delta` | `ExternalTableFormat.DELTA` (also accepts `"delta"`) |
| All four `ExternalTableSaveMode` variants | `ExternalTableSaveMode.ERROR_IF_EXISTS`, `.OVERWRITE`, `.APPEND`, `.IGNORE` via `df.write.mode(mode)` |
| `ArrowIpcStream::bytes` | `request.data.bytes`; `request.data` also works directly as `bytes` |
| Every `ExternalTableWriteRequest` and `ExternalTableWriteResult` field | Corresponding dataclass attributes |
| `ExternalTableWriteResult::validate_against` | `receipt.validate_against(previous)`, invoking the Rust validator |
| Attach/replace/remove provider | `spark.set_external_catalog_provider(provider_or_none)` and `setExternalCatalogProvider(...)` |
| Native catalog reads | `spark.table(name)`, `spark.read.table(name)`, and SQL relations |
| Pinned, latest, or explicit Delta snapshot | `descriptor.version = version_or_none`; `spark.read_delta(name)` and `spark.read_delta_with_version(name, version)` |
| Delegated append/overwrite, options, format, partitions | `df.write.mode(...).format(...).option(...).partitionBy(...).saveAsTable(name)` |
| Ordered options including duplicate keys | Repeated `.option(key, value)` or `.options([(key, value), ...])`; `request.options` remains an ordered list |
| SQL inserts from values or a query | `spark.sql("INSERT INTO ... VALUES ...")` / `spark.sql("INSERT INTO ... SELECT ...")` |
| Catalog existence and safe conditional creation | `spark.catalog.tableExists(name)` and SQL `CREATE TABLE IF NOT EXISTS` |
| Shared resolver slot and new-session isolation | Existing readers observe replacements; `spark.newSession()` has no provider |
| Native descriptor/schema/receipt checks, strict Delta reads, path confinement | The same native checks run automatically for Python calls |

The string enums remain compatible with existing string-based writer APIs. Descriptor
and receipt validation are available before a provider returns metadata, without
duplicating Rust validation rules in Python. For example:

```python
from sparkless.external_catalog import ExternalTableFormat, ExternalTableSaveMode

descriptor.validate()
receipt.validate_against(descriptor)
df.write.format(ExternalTableFormat.DELTA).mode(
    ExternalTableSaveMode.APPEND
).saveAsTable("main.analytics.orders")
```

For exact-width IPC writes, `spark.createDataFrame(rows, [("id", "int32")])`
constructs a native 32-bit Integer column. Typed PyArrow inputs also preserve
`int16`, `int32`, and `int64` columns through the writer. Descriptor schemas use
`ShortType`, `IntegerType`, and `LongType` for those respective native types.

Named `read_delta` methods are also present in Python builds without Delta support:
they can resolve local in-memory tables, while attempts to read Delta storage raise
the native feature-required error. Logical Map writes and async transport have the
same limits in both languages.

## Availability

This change is developed against the `v4.13.4` source tree. Until a compatible
Sparkless release includes it, the API is available only to consumers building the
corresponding source revision. It is not part of the published `4.13.4` artifact.
