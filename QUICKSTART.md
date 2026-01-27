# Quick Start Guide (Rust)

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

As more APIs are added (constructors from Rust data, IO helpers, richer expressions), this guide can be expanded with additional examples.
