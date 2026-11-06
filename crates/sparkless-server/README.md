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
read-only DuckDB sources and optional DuckDB Connect execution. Polars remains
the default backend for both products.

For an application using the 4.20 Rust API, install the matching client and
server release together:

```sh
cargo install sparkless-server --version 4.20.0 --locked
```

See the [Python package and platform matrix](../../docs/INSTALLATION.md),
[deployment runbook](../../docs/PRODUCTION.md), and
[4.20 release evidence](../../docs/RELEASE_EVIDENCE.md).

See the [server support contract](../../docs/SUPPORT_MATRIX.md), [Spark Connect
reference](../../docs/SPARK_CONNECT.md), and [release migration plan](../../docs/plans/sparkless-server/23-product-and-package-separation.md).
