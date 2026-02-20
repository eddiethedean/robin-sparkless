# Embedding robin-sparkless

This guide summarizes how to embed robin-sparkless in your app or binding (e.g. PyO3, Node, CLI) with a minimal, stable surface.

## Recommended embedding API

Use the **engine-agnostic ExprIr API** and **`*_engine()`** methods so your binding never depends on Polars types. Import expression builders from the **crate root** (they build `ExprIr`), not from `prelude` (which gives `Column`).

- **Expressions:** From the crate root: `col`, `lit_i64`, `lit_str`, `lit_bool`, `when`, `gt`, `eq`, `sum`, `count`, `min`, `max`, `mean`, `alias`, etc. These build an `ExprIr` tree.
- **DataFrame ops:** `filter_expr_ir(&ExprIr)`, `select_expr_ir(&[ExprIr])`, `with_column_expr_ir(name, &ExprIr)`, `collect_rows() -> CollectedRows` (JSON-like rows). For aggregations: `GroupedData::agg_expr_ir(&[ExprIr])`.
- **Session:** `create_dataframe_engine`, `create_dataframe_from_rows_engine`, `read_csv_engine`, `read_parquet_engine`, `read_json_engine`, `table_engine`.
- **DataFrame (other):** `schema_engine`, `columns_engine`, `count_engine`, `select_engine`, `filter_engine`, `with_column_engine`, `group_by_engine`, `limit_engine`, `collect_as_json_rows_engine`, `to_json_rows` (returns `EngineError`). Use `get_column_data_type(name)` for a Polars-free column type.
- **Schema:** `StructType::to_json()` / `to_json_pretty()`, `schema_from_json(json)`, and re-exported `DataType`.

Where a method still returns `PolarsError`, convert with **`robin_sparkless::to_engine_error(e)`** (the root crate does not implement `From<PolarsError>` for `EngineError`).

## Prelude vs prelude::embed vs crate root

- **Crate root (ExprIr)** — For embedding: `use robin_sparkless::{col, lit_i64, gt, ...};` — these build `ExprIr`. Use with `filter_expr_ir`, `select_expr_ir`, `with_column_expr_ir`, `collect_rows`, `agg_expr_ir`, and `*_engine()` methods. No Polars types in signatures; errors are `EngineError`.
- **`use robin_sparkless::prelude::*`** — Full application API: session, DataFrame, Column, GroupedData, and functions that return `Column`/`Expr` (`col`, `lit_*`, aggregates, string helpers). Use when you want the full PySpark-like API in Rust.
- **`use robin_sparkless::prelude::embed::*`** — Minimal re-exports for FFI: `SparkSession`, `DataFrame`, `GroupedData`, `DataFrameReader`, `StructType`, `DataType`, `EngineError`, etc. For expressions in bindings, use the crate root (`col`, `lit_i64`, `gt`, …) to build `ExprIr` and the `*_expr_ir` / `*_engine` methods.

## Config

- **`SparklessConfig`** — Struct with `warehouse_dir`, `temp_dir`, `case_sensitive`, and `extra` (Spark-style key-value config).
- **`SparklessConfig::from_env()`** — Reads `ROBIN_SPARKLESS_WAREHOUSE_DIR`, `ROBIN_SPARKLESS_TEMP_DIR`, `ROBIN_SPARKLESS_CASE_SENSITIVE`, and `ROBIN_SPARKLESS_CONFIG_*` (suffix becomes a config key with underscores as dots).
- **`SparkSession::from_config(&config)`** — Builds a session from a config (equivalent to `SparkSession::builder().with_config(config).get_or_create()`).

## Schema

- **`DataFrame::schema()`** / **`DataFrame::schema_engine()`** — Returns the schema as a Polars-free `StructType` (list of fields with name, data_type, nullable). Use `schema_engine()` in bindings to get `Result<StructType, EngineError>`.
- **`DataFrame::get_column_data_type(name)`** — Returns `Option<schema::DataType>` (robin-sparkless type only; no Polars).
- **`StructType::to_json()`** / **`StructType::to_json_pretty()`** — Serialize the schema to a JSON string. Returns `Result<String, serde_json::Error>`.
- **`schema_from_json(json)`** — Parse a schema from a JSON string (e.g. from the host). Returns `Result<StructType, EngineError>`.

## Temp views

- **Temp views** are **session-scoped**. After `create_or_replace_temp_view(name, df)`, the same session must be used for `table(name)` and `spark.sql("SELECT ... FROM name")`. If your binding (e.g. PyO3) uses a different session handle for `table()` or `sql()` than for `create_or_replace_temp_view()`, the view will not be found. Ensure the same `SparkSession` instance is used for both registration and lookup.

## Error handling

- **`EngineError`** — Unified error type with variants: `User`, `Internal`, `Io`, `Sql`, `NotFound`, `Other`. Use `*_engine()` and `*_expr_ir` methods to get `Result<_, EngineError>` directly. For APIs that still return `PolarsError`, use **`robin_sparkless::to_engine_error(e)`** (the root crate does not implement `From<PolarsError>` for `EngineError`).
- **`DataFrame::to_json_rows()`** — Returns `Result<String, EngineError>`: collects rows as a JSON array of objects.

## Examples

- **[examples/embed_basic.rs](../examples/embed_basic.rs)** — Uses the ExprIr API: `create_dataframe_engine`, `filter_expr_ir`, `group_by_engine`, `agg_expr_ir`. Creates a session from config, runs a simple pipeline (filter + groupBy + agg), and prints schema and JSON rows. Run with: `cargo run --example embed_basic`.

  Example output (key order in JSON may vary):

  ```
  Schema: Ok(StructType { fields: [StructField { name: "id", data_type: Long, nullable: true }, StructField { name: "score", data_type: String, nullable: true }, StructField { name: "score_1", data_type: Long, nullable: true }] })
  Rows (JSON): [{"score_1":300,"id":3,"score":1},{"score_1":200,"id":2,"score":1}]
  ```

- **[examples/embed_readme.rs](../examples/embed_readme.rs)** — Matches the README embedding snippet using ExprIr: `create_dataframe_engine`, `filter_expr_ir(&gt(col("id"), lit_i64(1)))`, then `to_json_rows`. Run with: `cargo run --example embed_readme`. Example output: `[{"label":"b","value":20,"id":2},{"id":3,"value":30,"label":"c"}]` (key order may vary).

## Traits (optional)

- **`IntoRobinDf`** — `fn into_robin_df(self, session: &SparkSession) -> Result<DataFrame, EngineError>`. Implemented for `Vec<(i64, i64, String)>`, `Vec<(i64, String)>`, and `Vec<(i64, i64, i64, String)>` (default column names `c0`, `c1`, …).
- **`FromRobinDf`** — `fn from_robin_df(df: &DataFrame) -> Result<Self, EngineError>`. Implemented for `Vec<HashMap<String, JsonValue>>` (row objects) and `Vec<Vec<JsonValue>>` (rows as arrays in column order).

Use these for idiomatic Rust (e.g. `data.into_robin_df(&spark)?` or `Vec::<HashMap<String, JsonValue>>::from_robin_df(&df)?`).
