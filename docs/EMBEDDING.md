# Embedding robin-sparkless

This guide summarizes how to embed robin-sparkless in your app or binding (e.g. PyO3, Node, CLI) with a minimal, stable surface.

## Recommended embedding API

Use **`prelude::embed`** and the **`*_engine()`** methods plus schema helpers so your binding never depends on Polars error or schema types:

- **Session:** `create_dataframe_engine`, `create_dataframe_from_rows_engine`, `read_csv_engine`, `read_parquet_engine`, `read_json_engine`, `table_engine`.
- **DataFrame:** `schema_engine`, `columns_engine`, `count_engine`, `select_engine`, `filter_engine`, `with_column_engine`, `group_by_engine`, `limit_engine`, `collect_as_json_rows_engine`, `to_json_rows` (already returns `EngineError`). Use `get_column_data_type(name)` for a Polars-free column type.
- **Schema:** `StructType::to_json()` / `to_json_pretty()`, `schema_from_json(json)` to parse schema from the host, and re-exported `DataType` for building or interpreting schemas.

Where no `_engine` variant exists, use the existing method and `.map_err(EngineError::from)`.

## Prelude vs prelude::embed

- **`use robin_sparkless::prelude::*`** — One-stop import for application code: session, DataFrame, Column, GroupedData, common functions (`col`, `lit_*`, aggregates, string helpers), and config. Use this when you want the full convenience API in Rust.
- **`use robin_sparkless::prelude::embed::*`** — Minimal surface for FFI/embedding crates. Re-exports: `SparkSession`, `SparkSessionBuilder`, `DataFrame`, `GroupedData`, `DataFrameReader`, `Column`, `Expr`, `LiteralValue`, `StructType`, `StructField`, `DataType`, `SparklessConfig`, `EngineError`, and functions `col`, `lit_i64`, `lit_bool`, `lit_str`, `lit_null`, `lit_f64`, `lit_i32`, `count`, `sum`, `avg`, `min`, `max`. Kept small and stable so bindings can depend only on robin-sparkless types and avoid Polars imports.

## Config

- **`SparklessConfig`** — Struct with `warehouse_dir`, `temp_dir`, `case_sensitive`, and `extra` (Spark-style key-value config).
- **`SparklessConfig::from_env()`** — Reads `ROBIN_SPARKLESS_WAREHOUSE_DIR`, `ROBIN_SPARKLESS_TEMP_DIR`, `ROBIN_SPARKLESS_CASE_SENSITIVE`, and `ROBIN_SPARKLESS_CONFIG_*` (suffix becomes a config key with underscores as dots).
- **`SparkSession::from_config(&config)`** — Builds a session from a config (equivalent to `SparkSession::builder().with_config(config).get_or_create()`).

## Schema

- **`DataFrame::schema()`** / **`DataFrame::schema_engine()`** — Returns the schema as a Polars-free `StructType` (list of fields with name, data_type, nullable). Use `schema_engine()` in bindings to get `Result<StructType, EngineError>`.
- **`DataFrame::get_column_data_type(name)`** — Returns `Option<schema::DataType>` (robin-sparkless type only; no Polars).
- **`StructType::to_json()`** / **`StructType::to_json_pretty()`** — Serialize the schema to a JSON string. Returns `Result<String, serde_json::Error>`.
- **`schema_from_json(json)`** — Parse a schema from a JSON string (e.g. from the host). Returns `Result<StructType, EngineError>`.

## Error handling

- **`EngineError`** — Unified error type with variants: `User`, `Internal`, `Io`, `Sql`, `NotFound`, `Other`. Implements `From<PolarsError>`, `From<serde_json::Error>`, and `From<std::io::Error>`. Use `*_engine()` methods to get `Result<_, EngineError>` directly, or `.map_err(EngineError::from)` on APIs that still return `PolarsError`.
- **`DataFrame::to_json_rows()`** — Returns `Result<String, EngineError>`: collects rows as a JSON array of objects.

## Examples

- **[examples/embed_basic.rs](../examples/embed_basic.rs)** — Creates a session from config, runs a simple pipeline (filter + groupBy + agg), and prints schema and JSON rows. Run with: `cargo run --example embed_basic`.

## Traits (optional)

- **`IntoRobinDf`** — `fn into_robin_df(self, session: &SparkSession) -> Result<DataFrame, EngineError>`. Implemented for `Vec<(i64, i64, String)>`, `Vec<(i64, String)>`, and `Vec<(i64, i64, i64, String)>` (default column names `c0`, `c1`, …).
- **`FromRobinDf`** — `fn from_robin_df(df: &DataFrame) -> Result<Self, EngineError>`. Implemented for `Vec<HashMap<String, JsonValue>>` (row objects) and `Vec<Vec<JsonValue>>` (rows as arrays in column order).

Use these for idiomatic Rust (e.g. `data.into_robin_df(&spark)?` or `Vec::<HashMap<String, JsonValue>>::from_robin_df(&df)?`).
