# sparkless-server

Base Polars HTTP and Spark Connect server product. This crate provides the
`sparkless-server` executable and reexports the shared Rust server API. Its
default product does not select or link the DuckDB source crate.

Build the advertised transport profiles:

```sh
cargo build --release -p sparkless-server --no-default-features --features http
cargo build --release -p sparkless-server --no-default-features --features connect
cargo build --release -p sparkless-server --no-default-features --features http,connect
```

Use `cargo install sparkless-server` for the base executable. Use
`cargo install sparkless-server-duckdb` to install the separate product with
read-only DuckDB sources. Both products execute transformations through Polars
in 4.19.

See the [server support contract](../../docs/SUPPORT_MATRIX.md), [Spark Connect
reference](../../docs/SPARK_CONNECT.md), and [release migration plan](../../docs/plans/sparkless-server/23-product-and-package-separation.md).
