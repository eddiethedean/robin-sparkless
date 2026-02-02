# Quick Start Guide (Rust)

Robin-sparkless is a PySpark-like DataFrame library in Rust, with Polars as the engine. Long-term, it is designed to replace the backend of [Sparkless](https://github.com/eddiethedean/sparkless). See [SPARKLESS_INTEGRATION_ANALYSIS.md](SPARKLESS_INTEGRATION_ANALYSIS.md) for integration details.

## Building the Project

### Prerequisites
- Rust (latest stable)

### Build

```bash
cargo build
```

### Add as a dependency

In your own crate:

```toml
[dependencies]
robin-sparkless = "0.1.0"
```

## Basic Usage

The current API is intentionally small and focused on wrapping Polars types.

```rust
use polars::prelude::*;
use robin_sparkless::{DataFrame, Column};

fn main() -> polars::prelude::PolarsResult<()> {
    // Build a Polars DataFrame
    let polars_df = df!(
        "id" => &[1, 2, 3],
        "age" => &[25, 30, 35],
        "name" => &["Alice", "Bob", "Charlie"],
    )?;

    // Wrap it
    let df = DataFrame::from_polars(polars_df);

    // Introspect
    println!("{:?}", df.columns()?);

    // Use a Column wrapper around a Polars expression
    let age_col: Column = df.column("age")?;
    let adults = df.filter(age_col.expr().clone().gt(lit(18)))?;

    adults.show(Some(10))?;

    Ok(())
}
```

### Joins

```rust
use robin_sparkless::{DataFrame, JoinType};

let joined = left_df.join(&right_df, vec!["dept_id"], JoinType::Inner)?;
```

Supported join types: `Inner`, `Left`, `Right`, `Outer`.

### Window Functions

```rust
use robin_sparkless::{col, DataFrame};

// row_number, rank, dense_rank over partition
let df_with_rn = df.with_column(
    "rn",
    col("salary").row_number(true).over(&["dept"]).into_expr(),
)?;

// lag and lead
let df_with_lag = df.with_column(
    "prev_salary",
    col("salary").lag(1).over(&["dept"]).into_expr(),
)?;
```

Supported window functions: `row_number()`, `rank()`, `dense_rank()`, `lag(n)`, `lead(n)` — each used with `.over(&["col1", "col2"])` for partitioning.

### String Functions

```rust
use robin_sparkless::{col, concat, concat_ws, DataFrame};

// upper, lower, substring (1-based start) - via Column methods
let df = df.with_column("name_upper", col("name").upper().into_expr())?;
let df = df.with_column("prefix", col("name").substr(1, Some(3)).into_expr())?;

// concat and concat_ws
let df = df.with_column("full", concat(&[&col("first"), &col("last")]).into_expr())?;
let df = df.with_column("joined", concat_ws("-", &[&col("a"), &col("b")]).into_expr())?;
```

For roadmap and Sparkless integration phases, see [ROADMAP.md](ROADMAP.md).
