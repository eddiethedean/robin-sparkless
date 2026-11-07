# sparkless-server-polars

Shared Sparkless service implementation for HTTP SQL and Spark Connect. It
adapts the Polars engine to the server contracts and provides optional DuckDB
integration through the `duckdb-source` feature.

| Product | Execution and data sources |
| --- | --- |
| [`sparkless-server`](https://crates.io/crates/sparkless-server) | Polars |
| [`sparkless-server-duckdb`](https://crates.io/crates/sparkless-server-duckdb) | Polars by default, read-only DuckDB sources, and optional DuckDB execution for Spark Connect |

The product crates choose which transports and capabilities to expose. The
`sparkless-server-connect` crate owns the vendored Apache Spark 4.0.5 protocol
definitions and generates the gRPC bindings for Connect builds.

[Server support matrix](https://robin-sparkless.readthedocs.io/en/latest/SUPPORT_MATRIX/) ·
[DuckDB execution guide](https://robin-sparkless.readthedocs.io/en/latest/DUCKDB_EXECUTION/)
