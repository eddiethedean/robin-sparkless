# Embedding robin-sparkless

This guide summarizes how to embed robin-sparkless in your app or binding (e.g. PyO3, Node, CLI) with a minimal, stable surface.

## Prelude vs prelude::embed

- **`use robin_sparkless::prelude::*`** — One-stop import for application code: session, DataFrame, Column, GroupedData, common functions (`col`, `lit_*`, aggregates, string helpers), and config. Use this when you want the full convenience API in Rust.
- **`use robin_sparkless::prelude::embed::*`** — Minimal surface for FFI/embedding crates. Re-exports only: `SparkSession`, `SparkSessionBuilder`, `DataFrame`, `Column`, `GroupedData`, `Expr`, `LiteralValue`, and a small set of functions (`col`, `lit_i64`, `lit_bool`, `lit_str`, `lit_null`, `count`, `sum`, `avg`, `min`, `max`). Kept small and stable so bindings can depend only on robin-sparkless types and avoid Polars imports.

## Config

- **`SparklessConfig`** — Struct with `warehouse_dir`, `temp_dir`, `case_sensitive`, and `extra` (Spark-style key-value config).
- **`SparklessConfig::from_env()`** — Reads `ROBIN_SPARKLESS_WAREHOUSE_DIR`, `ROBIN_SPARKLESS_TEMP_DIR`, `ROBIN_SPARKLESS_CASE_SENSITIVE`, and `ROBIN_SPARKLESS_CONFIG_*` (suffix becomes a config key with underscores as dots).
- **`SparkSession::from_config(&config)`** — Builds a session from a config (equivalent to `SparkSession::builder().with_config(config).get_or_create()`).

## Schema

- **`DataFrame::schema()`** — Returns `Result<StructType, PolarsError>`. The schema is a Polars-free type (list of fields with name, data_type, nullable).
- **`StructType::to_json()`** / **`StructType::to_json_pretty()`** — Serialize the schema to a JSON string (e.g. for bindings that need to expose schema to the host without Polars). Returns `Result<String, serde_json::Error>`.

## Error handling

- **`EngineError`** — Unified error type with variants: `User`, `Internal`, `Io`, `Sql`, `NotFound`, `Other`. Implements `From<PolarsError>`, `From<serde_json::Error>`, and `From<std::io::Error>` so you can map Polars results with `.map_err(EngineError::from)` or use helpers that return `EngineError` directly.
- **`DataFrame::to_json_rows()`** — Returns `Result<String, EngineError>`: collects rows as a JSON array of objects. Use this to return results to a host (e.g. Python/Node) without dealing with Polars errors.

## Examples

- **[examples/embed_basic.rs](../examples/embed_basic.rs)** — Creates a session from config, runs a simple pipeline (filter + groupBy + agg), and prints schema and JSON rows. Run with: `cargo run --example embed_basic`.

## Traits (optional)

- **`IntoRobinDf`** — `fn into_robin_df(self, session: &SparkSession) -> Result<DataFrame, EngineError>`. Implemented for `Vec<(i64, i64, String)>` (with default column names `c0`, `c1`, `c2`).
- **`FromRobinDf`** — `fn from_robin_df(df: &DataFrame) -> Result<Self, EngineError>`. Implemented for `Vec<HashMap<String, JsonValue>>` (via `collect_as_json_rows`).

Use these for idiomatic Rust (e.g. `data.into_robin_df(&spark)?` or `Vec::<HashMap<String, JsonValue>>::from_robin_df(&df)?`).
