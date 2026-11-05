# sparkless-server-duckdb

Native HTTP and Spark Connect server with trusted, read-only DuckDB table sources.

The server uses Polars for DataFrame execution. DuckDB supplies configured table batches through
the native Arrow source interface. Standalone sources are opened read-only. Shared Quack sources
must point to an explicitly configured DuckDB owner. The base `sparkless-server` product excludes
the DuckDB dependency and Quack extension.

Build the Connect-enabled product from the workspace root:

```sh
cargo build --release -p sparkless-server-duckdb --no-default-features --features http,connect
```

Configure a trusted source in the deployment environment with `SPARKLESS_DUCKDB_SOURCES`. Clients
send table names and cannot select database paths, credentials, or extension binaries.

For the Python Spark Connect client, install the same published version with
`pip install "sparkless-server-duckdb[connect]==4.19.0"`. See the
[DuckDB quickstart](../../docs/DUCKDB_SOURCE.md), [deployment runbook](../../docs/PRODUCTION.md),
and [release evidence](../../docs/RELEASE_EVIDENCE.md).
