# sparkless-server-duckdb

Native HTTP and Spark Connect server with trusted DuckDB table sources and an
optional DuckDB Connect execution backend. Polars remains the default. Select
DuckDB execution with `SPARKLESS_EXECUTION_BACKEND=duckdb`.

Configure `SPARKLESS_DUCKDB_EXECUTION_OWNER` with a persistent local database
or a shared Quack owner. This execution owner is separate from the
`SPARKLESS_DUCKDB_SOURCES` read-only source aliases. The base `sparkless-server`
product continues to exclude DuckDB and Quack.

Build the Connect-enabled product from the workspace root:

```sh
cargo build --release -p sparkless-server-duckdb --no-default-features --features http,connect
```

Configure a trusted source in the deployment environment with `SPARKLESS_DUCKDB_SOURCES`. Clients
send table names and cannot select database paths, credentials, or extension binaries.

See the [DuckDB execution guide](../../docs/DUCKDB_EXECUTION.md) for backend
selection, transaction receipts, supported Connect plans, and operating limits.

For the Python Spark Connect client, install the same release version with
`pip install "sparkless-server-duckdb[connect]==4.20.0"`. See the
[DuckDB quickstart](../../docs/DUCKDB_SOURCE.md), [deployment runbook](../../docs/PRODUCTION.md),
and [release evidence](../../docs/RELEASE_EVIDENCE.md).
