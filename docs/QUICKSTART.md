# Rust quickstart

`robin-sparkless` is the Rust DataFrame engine behind the Sparkless Python
package. It executes locally on Polars and does not require Python or a JVM.
Python users should start with [the Python guide](python_getting_started.md).

## Install

The 4.18.0 crates require Rust 1.93 or newer. Repository builds use the pinned
[rust-toolchain.toml](https://github.com/eddiethedean/robin-sparkless/blob/main/rust-toolchain.toml).

Add the engine to your application's `Cargo.toml`:

```toml
[dependencies]
robin-sparkless = "4"
```

**4.18.0 compression security:** Cargo does not inherit a dependency's workspace
patches. Ordinary crates.io builds retain zstd advisory exposure under a
maintainer-approved exception that expires after 2026-11-26 UTC. Apply the
[pinned consumer patch configuration](security/zstd-4.18-exception.md#apply-our-patch-in-a-rust-consumer)
when handling untrusted compressed input. The linked notice explains the
exposure, exact patches, lockfile update, and verification commands. Official
Python/server wheels and repository builds use the fixed graph.

## Create and filter a DataFrame

This example uses the Column API from `robin_sparkless::functions`:

```rust
use robin_sparkless::SparkSession;
use robin_sparkless::functions::{col, lit_i64};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let spark = SparkSession::builder().app_name("demo").get_or_create();
    let df = spark.create_dataframe(
        vec![
            (1, 25, "Alice".to_string()),
            (2, 30, "Bob".to_string()),
            (3, 35, "Charlie".to_string()),
        ],
        vec!["id", "age", "name"],
    )?;

    let adults = df.filter(col("age").gt(lit_i64(26).into_expr()).into_expr())?;
    adults.show(Some(10))?;
    Ok(())
}
```

The same runnable example lives in
[`examples/demo.rs`](https://github.com/eddiethedean/robin-sparkless/blob/main/examples/demo.rs).
From a repository checkout:

```bash
cargo run --locked --example demo
```

`create_dataframe` accepts the `(i64, i64, String)` rows used above. For other
schemas, use `create_dataframe_from_rows` with explicit schema information;
see the [Rust API](RUST_API_REFERENCE.md).

Transformations extend a lazy plan. Actions such as `collect`, `show`, `count`,
and writes execute it. Collecting materializes data in the calling process.

## Choose an expression API

There are two expression surfaces; use the matching methods and imports:

| Surface | Imports | DataFrame methods |
| --- | --- | --- |
| Engine-independent `ExprIr` | `robin_sparkless::engine` or crate-root helpers | `filter_expr_ir`, `select_expr_ir`, `collect_rows`, `*_engine` methods |
| Polars-backed `Column` | `robin_sparkless::functions` or `prelude` | `filter`, `with_column`, and Polars-expression methods |

For example, inside the function above, the engine-independent equivalent is:

```rust
use robin_sparkless::engine::{col, gt, lit_i64};

let adults = df.filter_expr_ir(&gt(col("age"), lit_i64(26)))?;
let rows = adults.collect_rows()?;
```

Treat that block as an alternative to the Column helper imports in the complete
example. Crate-root `col` returns `ExprIr`; `functions::col` returns `Column`.
For embedding and backend-independent code, prefer `ExprIr`. See the
[embedding guide](EMBEDDING.md) and [API reference](RUST_API_REFERENCE.md).

## Optional features

The facade has no default Cargo features. Enable capabilities explicitly, for
example:

```toml
[dependencies]
robin-sparkless = { version = "4", features = ["sql", "delta"] }
```

| Feature | Capability |
| --- | --- |
| `sql` | SQL queries, views, and table resolution |
| `delta` | Delta Lake reads and writes |
| `jdbc` | PostgreSQL database access |
| `sqlite` | SQLite database access |
| `jdbc_mysql`, `jdbc_mariadb`, `jdbc_mssql`, `jdbc_oracle`, `jdbc_db2` | Other database backends |

Backend options and supported behavior vary. See the [user guide](USER_GUIDE.md),
[persistence guide](PERSISTENCE_GUIDE.md), [database testing guide](JDBC_TESTING.md),
and [known PySpark differences](PYSPARK_DIFFERENCES.md). Rust feature support
does not imply the same operation is enabled through [Spark Connect](SPARK_CONNECT.md).

## Development and validation

Follow [CONTRIBUTING.md](https://github.com/eddiethedean/robin-sparkless/blob/main/CONTRIBUTING.md)
for the toolchain, Python environment, and build workflow.

- `cargo test --locked --workspace` runs default-feature Rust tests.
- `make check-full` runs Rust checks/tests plus Python lint and types.
- `make test-parity-phases` runs the Python parity fixture suite; the historical
  phase-specific Make targets are aliases for that same suite.

CI runs feature-specific Rust lanes and Python tests against a built wheel.
See [the release guide](RELEASING.md) for package validation and publication
requirements, and [parity status](PARITY_STATUS.md) for compatibility coverage.
