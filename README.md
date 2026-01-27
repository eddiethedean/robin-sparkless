# Robin Sparkless

A Rust DataFrame library that aims to **emulate PySpark’s DataFrame behavior and semantics without requiring the JVM**, using Polars as the execution engine under the hood.

## Features

- **PySpark-inspired API**: `SparkSession`, `DataFrame`, `Column` modeled after PySpark concepts
- **PySpark-like behavior**: Semantics (null handling, groupBy, joins, etc.) should match PySpark as closely as practical
- **Polars-backed**: Uses Polars `DataFrame`/`Expr` internally for performance
- **Pure Rust**: No Python runtime, no PyO3, no JVM

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
robin-sparkless = "0.1.0"
```

## Quick Start

At the moment, `robin-sparkless` exposes a small Rust API and uses Polars as its execution engine. The long-term goal is to make PySpark-style code patterns natural to express in Rust.

```rust
use polars::prelude::*;
use robin_sparkless::{DataFrame, Column, SparkSession};

// In the future, most users should start from a SparkSession, like PySpark:
// let spark = SparkSession::builder().app_name("my_app").get_or_create();
// let df = spark.create_dataframe(data, schema);
//
// For now, we start from a Polars DataFrame:

let polars_df = df!(
    "id" => &[1, 2, 3],
    "age" => &[25, 30, 35],
    "name" => &["Alice", "Bob", "Charlie"],
)?;

let df = DataFrame::from_polars(polars_df);

// Introspect (PySpark-style: df.schema, df.columns)
let schema = df.schema()?;
let columns = df.columns()?;

// Build expressions using a Column wrapper (similar to PySpark Column)
let age_col: Column = df.column("age")?;
let adults = df.filter(age_col.expr().clone().gt(lit(18)))?;

adults.show(Some(10))?;
```

As the library evolves, higher-level constructors (for Rust tuples/records) and IO helpers (CSV/Parquet/JSON readers) will be added on top of this core.

## Development

### Prerequisites

- Rust (latest stable)

### Building

```bash
# Build the project
cargo build

# Run tests
cargo test

# Build documentation
cargo doc --open
```

## Architecture

Robin Sparkless aims to provide a **PySpark-like API layer** on top of Polars:

- **SparkSession**: Entry point for creating `DataFrame`s and (eventually) reading data sources.
- **DataFrame**: Main tabular data structure; behavior should mirror PySpark’s DataFrame where possible.
- **Column**: Represents expressions over columns, similar to PySpark’s `Column`.
- **Functions**: Helper functions like `col()`, `lit_*()`, `count()`, etc., modeled after PySpark’s `pyspark.sql.functions`.

Over time, more of PySpark’s behavior (null handling, grouping semantics, joins, expression behavior) will be matched, while still running entirely in Rust on top of Polars.

## License

MIT
