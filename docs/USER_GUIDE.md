# Rust user guide

Use this guide for the Rust **Column API**. Python applications should follow
the [Python quickstart](python_getting_started.md). For engine-independent
expressions and errors, follow [ExprIr embedding](EMBEDDING.md).

## Installation

Use Rust 1.93 or newer; the repository pins 1.93.1. Start with a new Cargo
application as shown in the [Rust quickstart](QUICKSTART.md).

```toml
[dependencies]
robin-sparkless = { version = "=4.20.0", features = ["sql"] }
```

SQL, Delta and database drivers are opt-in Cargo features. See the
[feature matrix](INSTALLATION.md#platforms-and-build-features). The
[security policy](SECURITY.md#known-dependency-exception) scopes historical
dependency exceptions by version; the 4.18 patch is not a blanket 4.x requirement.
Minor releases can break interfaces; review [upgrades](UPGRADING.md) before
changing the version pin.

## Filter, project, sort and aggregate

Save this complete program as `src/main.rs` and run `cargo run`. It keeps Bob
and Charlie, verifies the row count, and aggregates age by name. Transformations
build plans; `count`, `collect_as_json_rows` and `show` execute them.

```rust
use robin_sparkless::SparkSession;
use robin_sparkless::functions::{col, lit_i64};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let spark = SparkSession::builder().app_name("rust-guide").get_or_create();
    let people = spark.create_dataframe(
        vec![
            (1, 25, "Alice".to_string()),
            (2, 30, "Bob".to_string()),
            (3, 35, "Charlie".to_string()),
        ],
        vec!["id", "age", "name"],
    )?;
    let adults = people
        .filter(col("age").gt(lit_i64(25).into_expr()).into_expr())?
        .select(vec!["name", "age"])?
        .order_by(vec!["age"], vec![true])?
        .limit(10)?;
    assert_eq!(adults.count()?, 2);
    let rows = adults.collect_as_json_rows()?;
    assert_eq!(rows[0]["name"], "Bob");
    adults.group_by(vec!["name"])?.sum("age")?.show(Some(10))?;
    spark.create_or_replace_temp_view("adults", adults);
    assert_eq!(spark.sql("SELECT name FROM adults WHERE age > 30")?.count()?, 1);
    Ok(())
}
```

The checked copy is [examples/user_guide.rs](https://github.com/eddiethedean/robin-sparkless/blob/main/examples/user_guide.rs).
From a checkout, run `cargo run --locked --example user_guide --features sql`.

## Choose matching expressions

| Expression type | Import | Matching DataFrame method |
| --- | --- | --- |
| Column / Polars Expr | `robin_sparkless::functions::{col, lit_i64}` | `filter(expr)`, `with_column(name, &column)`, `select_exprs(exprs)` |
| Engine-independent ExprIr | `robin_sparkless::engine::{col, gt, lit_i64}` | `filter_expr_ir(&expr)`, `select_expr_ir(&exprs)`, `collect_rows()` |

The two sets of helpers share names but return different types. Do not replace
the imports in the complete program with crate-root helpers. For computed
columns use `with_column`; for joins use `join(&other, keys, JoinType::Inner)`.
Alias ambiguous fields and define null-key and duplicate-name expectations.
The [API reference](RUST_API_REFERENCE.md) supplies exact method signatures.

## Files, tables and extensions

Read local files with `read_csv`, `read_json` or `read_parquet`; write through
`df.write()`. Path writes default to overwrite. File append can read and rewrite
existing data and is not an atomic append log. See [format options](FILE_IO.md)
before choosing options, paths or save modes.

With SQL enabled, register session-local views with
`create_or_replace_temp_view`, then query through that same session. A configured
warehouse persists saved tables as Parquet; it is separate from server storage.
See [persistence and concurrent access](PERSISTENCE_GUIDE.md#storage-and-concurrency-contract).

Native expression UDFs use `SparkSession::register_udf`. Python scalar UDFs have
a different interface and execute only in the local Python API. External
storage uses the [catalog provider contract](EXTERNAL_CATALOGS.md). Database
access requires [driver features and per-driver options](JDBC_TESTING.md).

## Errors and next steps

Column API calls commonly return `PolarsError`; the embedding API uses structured
`EngineError`. Invalid names or types may fail when constructing a plan or running
an action. Propagate errors with `?`; inspect [error categories](ERRORS.md) at
application boundaries. Collecting returns all rows in process memory.

Continue with [embedding](EMBEDDING.md), [SQL dialects](SQL_REFERENCE.md), or
[architecture](ENGINE_ARCHITECTURE.md). Validate representative results against
your production Spark version using the [support contract](SUPPORT_MATRIX.md).
