# Upgrade from 4.19 to 4.20

Sparkless 4.20 adds an optional DuckDB execution backend to the existing
`sparkless-server-duckdb` product. The local Python API, Rust embedding API,
base server, and default Polars behavior remain the same. You do not need to
change an application that continues to use the default backend.

This is separate from the 4.19 DuckDB source feature. `SPARKLESS_DUCKDB_SOURCES`
exposes read-only tables to Polars. `SPARKLESS_DUCKDB_EXECUTION_OWNER` configures
the writable execution database used by the DuckDB backend. A server may
configure both, but source aliases remain read-only.

## Keep the default behavior

The DuckDB server product still starts in Polars mode when the backend variable
is unset. Existing 4.19 installs can upgrade the coordinated packages together:

```bash
python -m pip install --upgrade \
  "sparkless==4.20.0" \
  "sparkless-server==4.20.0" \
  "sparkless-server-duckdb==4.20.0" \
  "sparkless-connect==4.20.0" \
  "sparkless-testing==4.20.0"
python -m pip check
```

Install only the packages your application uses. See the [package matrix](INSTALLATION.md#choose-a-package)
for minimal install specs and supported platforms. The 4.19 package split and
removed import paths are covered in the [4.19 migration guide](python_migration.md#from-sparkless-418-to-419).

## Opt into DuckDB execution

Install the DuckDB product and Connect client extra:

```bash
python -m pip install \
  "sparkless-server-duckdb[connect]==4.20.0" \
  "duckdb==1.5.1"
```

Set both variables before starting the server. Local ownership is the simplest
choice for one server process:

```bash
export SPARKLESS_EXECUTION_BACKEND=duckdb
export SPARKLESS_DUCKDB_EXECUTION_OWNER='{"mode":"local","database_path":"./sparkless-execution.duckdb"}'
```

Use the same `database_path` for independent DuckDB clients that access the
database sequentially. Only one writable process may open a local database at
a time. For shared access by independent clients, configure one Quack owner as
described in the [execution guide](DUCKDB_EXECUTION.md#use-a-shared-quack-owner).
Keep its token in your deployment secret manager.

The process uses one backend for its lifetime. DuckDB mode requires the
`sparkless-server-duckdb` product and a Spark Connect listener; startup fails
before readiness if the configuration is incomplete. Readiness and
`/capabilities` report the active backend. HTTP health and capability routes
remain available, but HTTP SQL statements are disabled in DuckDB mode.

The supported Connect surface is deliberately bounded. It includes the
documented table reads, DataFrame filters/projections/joins/aggregations,
read-only SQL subset, typed `CREATE TABLE`, and literal `INSERT ... VALUES`.
Unsupported plans fail explicitly and never fall back to Polars. See the
[request/type matrix](DUCKDB_EXECUTION.md#supported-request-subset) and
[resource limits](DUCKDB_EXECUTION.md#resource-and-lifecycle-limits) before
choosing this backend.

## Data, retries, and lifecycle

Tables and durable operation receipts live in the configured DuckDB owner and
survive a Sparkless process restart. Connect sessions, temporary state, and
retained query results belong to one server instance. A receipt lets an
unchanged write request replay without executing the write twice; it does not
restore an old Connect session. Changed content under an existing operation
identity is rejected.

DuckDB writes are currently limited to explicitly typed `CREATE TABLE` and
literal `INSERT INTO ... VALUES`. DataFrame writers, CTAS, insert-from-select,
updates, deletes, and general DDL remain unsupported. Test transaction outcome,
owner loss, restart, schema, and representative Spark results before upgrading
a production workload.

## Verify the rollout

Start with a disposable database and confirm the reported backend from the
readiness record. Exercise a supported read and write, query the committed row
from an independent client after Sparkless exits, and verify restart receipt
replay. Compare the same queries and schemas with the Spark version you use.
See the [V37/V38 release evidence](RELEASE_EVIDENCE.md) for the exact candidate
artifacts and tested limits.
