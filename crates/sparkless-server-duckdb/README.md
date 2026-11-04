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
