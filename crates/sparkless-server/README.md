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

Install the 4.20.0 base executable with both HTTP and Spark Connect enabled:

```sh
cargo install sparkless-server --version 4.20.0 --locked --features connect
sparkless-server serve
```

Cargo enables HTTP by default. To install the DuckDB server instead, use
`sparkless-server-duckdb` with the same version and features. Both products
default to Polars. The DuckDB product adds read-only DuckDB sources and
optional DuckDB execution for supported Spark Connect plans.

See the [Python package and platform matrix](../../docs/INSTALLATION.md),
[deployment runbook](../../docs/PRODUCTION.md), and
[4.20 release evidence](../../docs/RELEASE_EVIDENCE.md).

See the [server support contract](../../docs/SUPPORT_MATRIX.md), [Spark Connect
reference](../../docs/SPARK_CONNECT.md), and [release migration plan](../../docs/plans/sparkless-server/23-product-and-package-separation.md).
