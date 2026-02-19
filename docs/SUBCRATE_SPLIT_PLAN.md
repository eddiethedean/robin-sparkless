# Plan: Splitting robin-sparkless into Sub-crates

This document outlines a plan to split the single `robin-sparkless` crate into multiple workspace crates. The main goal is **faster iterative compilation**: avoid recompiling Polars and other heavy dependencies when changing only a subset of the codebase.

---

## 1. Rationale

- **Current pain**: One large crate means any code change can trigger a full or near-full rebuild, including Polars and its many features. Polars is a large dependency; recompiling it dominates iteration time.
- **Goal**: By splitting into crates with clear dependency boundaries:
  - Changes to **Polars-free** code (e.g. config, schema types, error) do not trigger a Polars rebuild.
  - Changes to the **expression layer** (column, functions, udfs) recompile only that crate and dependents, not the entire tree.
  - Changes to **session** or **dataframe** recompile only the main library crate (and optional sql/delta), not the expression crate.

---

## 2. Current Structure (Summary)

- **Single crate** `robin-sparkless` (~40k lines across 26 `.rs` files under `src/`).
- **Heavy Polars usage** in: `session`, `dataframe`, `column`, `functions`, `udfs`, `udf_registry`, `plan` (including `plan/expr`), `schema` (conversion to/from Polars), `expression`, `type_coercion`.
- **Polars-free or minimal**: `config`, `error` (API is Polars-free; `From<PolarsError>` ties it to Polars), `traits` (use DataFrame/Session so stay with main lib), `date_utils` (chrono only), and the **type definitions** in `schema` (DataType, StructField, StructType as enums/structs; conversion functions use Polars).
- **Feature-gated**: `sql` (sqlparser + translator), `delta` (deltalake, tokio).

Rough size by area:

| Area              | Approx. lines | Polars usage      |
|-------------------|---------------|-------------------|
| session           | ~2.7k         | Heavy             |
| column            | ~3.2k         | Heavy             |
| functions         | ~3.3k         | Heavy             |
| udfs              | ~4.7k         | Heavy             |
| dataframe/*       | ~4.2k         | Heavy             |
| plan (mod + expr) | ~3.5k         | Heavy             |
| schema            | ~360          | Types + conversion |
| type_coercion     | ~380          | Uses Column/Expr  |
| error, config     | ~140          | Minimal / none   |
| rest              | smaller       | Mixed             |

---

## 3. Proposed Crate Layout

### 3.1 Crate List

| Crate                    | Purpose                         | Polars? | Approx. size |
|--------------------------|---------------------------------|---------|--------------|
| **robin-sparkless-core** | Shared types, config, error     | No      | Small        |
| **robin-sparkless-expr** | Column, Expr, functions, UDFs   | Yes     | Large        |
| **robin-sparkless**      | DataFrame, Session, Plan, API  | Yes     | Large        |
| **robin-sparkless-sql**  | SQL parsing and translation    | Via main| Medium       |
| **robin-sparkless-delta**| Delta Lake I/O                  | Via main| Medium       |

- **robin-sparkless** remains the main library and **facade**: it re-exports the public API from `core` and `expr` so existing dependents do not need to change.
- **robin-sparkless-sql** and **robin-sparkless-delta** stay optional (features or separate crates).

### 3.2 Dependency Diagram

```
                    ┌─────────────────────────┐
                    │ robin-sparkless-core    │  (no Polars)
                    │ schema types, config,   │
                    │ EngineError, date_utils │
                    └───────────┬─────────────┘
                                │
        ┌───────────────────────┼───────────────────────┐
        │                       │                       │
        ▼                       ▼                       │
┌───────────────┐     ┌─────────────────┐              │
│ robin-        │     │ robin-sparkless  │              │
│ sparkless-expr│     │ (main / facade)  │◄─────────────┘
│ column,       │     │ dataframe,       │
│ functions,    │────►│ session, plan,   │
│ udfs,         │     │ schema convert,  │
│ type_coercion │     │ traits, prelude  │
└───────────────┘     └────────┬────────┘
        │                      │
        │              ┌───────┴───────┐
        │              ▼               ▼
        │     robin-sparkless-sql   robin-sparkless-delta
        │     (optional)            (optional)
        │
        └── All "expr" and "main" crates depend on polars (and core).
```

- **core** does not depend on Polars. So edits to config/schema types/error only rebuild core and then dependents (expr, main); Polars is not rebuilt.
- **expr** depends on **core** and **polars**. Edits to session/dataframe/plan only rebuild the main crate (and optional sql/delta); expr (and Polars for that crate) are not rebuilt.
- **main** depends on **core**, **expr**, and **polars**. It holds DataFrame, Session, Plan, schema conversion, traits, prelude.

---

## 4. Module → Crate Mapping

### robin-sparkless-core (no Polars)

- **schema types only**: `DataType`, `StructField`, `StructType` (the enum/struct definitions and methods that do not touch Polars). No `from_polars_schema` / `to_polars_schema` here.
- **config**: `SparklessConfig` (from `config.rs`).
- **error**: `EngineError` (variant definitions and `Display`/`Error`; `From<std::io::Error>`, `From<serde_json::Error>`). Do **not** put `From<PolarsError>` here; that impl lives in the main crate (which has Polars).
- **date_utils**: Chrono-only helpers.
- **Dependencies**: `serde`, `serde_json`, `chrono` (for date_utils). No Polars.

### robin-sparkless-expr (Polars)

- **column**: `Column` and Expr construction.
- **functions**: All `functions.rs` (col, lit, when, aggregations, etc.).
- **expression**: Thin helpers (`column_to_expr`, `lit_*`).
- **type_coercion**: Coercion and comparison helpers (use `Column`/Expr).
- **udf_registry**: `UdfRegistry`, `RustUdf` (uses Polars `Series`).
- **udfs**: All built-in UDFs (string, list, hash, etc.).
- **Dependencies**: `robin-sparkless-core`, `polars` (same feature set as today). Plus existing UDF deps (regex, base64, rand, etc.).

### robin-sparkless (main / facade)

- **schema (conversion)**: Keep `StructType::from_polars_schema`, `StructType::to_polars_schema`, `polars_type_to_data_type`, `data_type_to_polars_type`, and `schema_from_json` in the main crate. Re-export `DataType`, `StructField`, `StructType` from `core` so the public API is unchanged.
- **dataframe**: All of `dataframe/` (mod, transformations, joins, aggregations, stats).
- **session**: `SparkSession`, `SparkSessionBuilder`, `DataFrameReader`, session-scoped UDF wiring.
- **plan**: `plan/mod.rs` and `plan/expr.rs` (execute_plan, apply_op, expression tree from JSON).
- **traits**: `FromRobinDf`, `IntoRobinDf` (they use DataFrame/Session).
- **prelude**: Re-exports from core, expr, and main (column, config, dataframe, error, functions, schema, session, Expr, LiteralValue).
- **lib.rs**: Same public surface as today; feature gates for `sql` and `delta`.
- **Dependencies**: `robin-sparkless-core`, `robin-sparkless-expr`, `polars`, and existing deps (e.g. spark-ddl-parser, url, etc.). Optional: sqlparser (sql), deltalake + tokio (delta).

### robin-sparkless-sql (optional)

- **sql**: `sql/mod.rs`, `sql/parser.rs`, `sql/translator.rs`.
- **Dependencies**: `robin-sparkless` (or core + expr + main), `sqlparser`. Can be a separate crate or a feature that compiles a subfolder of the main crate; the plan prefers a **separate crate** so that default `cargo build` does not pull sqlparser.

### robin-sparkless-delta (optional)

- **delta**: `delta/mod.rs`.
- **Dependencies**: `robin-sparkless`, `deltalake`, `tokio`. Same as today’s `delta` feature; can be a separate crate for clarity and to avoid pulling deltalake when not needed.

---

## 5. Workspace Setup

- Root `Cargo.toml`: define `[workspace]` with `members = ["crates/core", "crates/expr", "crates/robin-sparkless", "crates/sql", "crates/delta"]` (or keep current package at root and add `crates/robin-sparkless-core`, etc.). Choose one layout, e.g.:
  - **Option A**: All crates under `crates/` (e.g. `crates/robin-sparkless-core`, `crates/robin-sparkless-expr`, `crates/robin-sparkless`, `crates/robin-sparkless-sql`, `crates/robin-sparkless-delta`).
  - **Option B**: Current package stays at repo root as `robin-sparkless`; new crates `robin-sparkless-core` and `robin-sparkless-expr` live in `crates/` and the root crate becomes the facade that depends on them.
- Publish: either a single workspace with multiple publishable crates, or a single top-level package that pulls in the others (users depend only on `robin-sparkless`). The plan recommends **publishing** `robin-sparkless-core`, `robin-sparkless-expr`, and `robin-sparkless` so that advanced users can depend on `core` or `expr` only if they want; the main crate remains the primary dependency.

---

## 6. Migration Order

1. **Create workspace** and **robin-sparkless-core**  
   - Move schema types (DataType, StructField, StructType) into core; no Polars.  
   - Move config, EngineError (without `From<PolarsError>`), date_utils.  
   - Add `core` to workspace; root crate (or future `crates/robin-sparkless`) depends on `core`.

2. **Create robin-sparkless-expr**  
   - Move column, functions, expression, type_coercion, udf_registry, udfs into expr.  
   - expr depends on `robin-sparkless-core` and `polars`.  
   - Main crate depends on `robin-sparkless-expr` and uses its types (Column, Expr builders, UDFs).

3. **Refactor main crate**  
   - Keep dataframe, session, plan, traits, prelude in main.  
   - Add schema conversion module (from_polars_schema, to_polars_schema, schema_from_json) and re-export core schema types.  
   - Add `From<PolarsError>` for `EngineError` in main.  
   - Ensure prelude and lib.rs re-export from core and expr so the public API is unchanged.

4. **Extract optional crates (optional phase)**  
   - **robin-sparkless-sql**: Move `sql/*` into its own crate; depend on `robin-sparkless`. Feature `sql` on the main crate can mean “re-export robin-sparkless-sql” or “include sql in main” depending on preference.  
   - **robin-sparkless-delta**: Same for `delta/*` and `deltalake`/`tokio`.

5. **CI and docs**  
   - Update `deny.toml`, `Makefile`, and CI to build/test the workspace (e.g. `cargo build --workspace`, `cargo test --workspace`).  
   - Document that `robin-sparkless` is the main entry point; `robin-sparkless-core` and `robin-sparkless-expr` are for advanced or minimal-use cases.

---

## 7. API Stability and Breaking Changes

- **Public API**: The **robin-sparkless** crate (facade) should preserve the current public API. Existing code that `use robin_sparkless::prelude::*` or imports `SparkSession`, `DataFrame`, `Column`, `Expr`, `StructType`, etc., should not need to change.
- **Re-exports**: Prefer re-exporting from core and expr in the main crate so that `robin_sparkless::schema::StructType` and `robin_sparkless::Column` remain the same paths.
- **Semver**: If `robin-sparkless-core` or `robin-sparkless-expr` are published, their public APIs become part of the project’s stability guarantee; consider keeping their surfaces minimal (e.g. core: types and config only; expr: column/functions/udfs as today).

---

## 8. Expected Compile-Time Wins

- **Edit only core** (config, schema types, error): Only `robin-sparkless-core` and crates that depend on it recompile; **Polars is not rebuilt**.
- **Edit only session or dataframe or plan**: Only the main crate (and optionally sql/delta) recompile; **robin-sparkless-expr and Polars for that crate are not rebuilt**.
- **Edit only column, functions, or udfs**: `robin-sparkless-expr` and the main crate recompile; dataframe/session/plan code is not recompiled (only the crate that contains it is), and Polars is built once for expr.

This gives clear boundaries and reduces the amount of code (and especially Polars) that must be recompiled on each change.

---

## 9. Possible Future Splits

- **Plan in its own crate**: `robin-sparkless-plan` could contain only `plan/mod.rs` and `plan/expr.rs`, depending on main (or expr + main). Then edits to the plan interpreter would not rebuild session/dataframe. This can be a later step after the core/expr/main split is done.
- **Dataframe vs session**: Keeping them in one crate avoids circular dependency (DataFrame uses SparkSession in a few places; Session uses DataFrame everywhere). If desired later, a shared trait or interface could allow splitting, at higher complexity.

---

## 10. Summary

| Step | Action |
|------|--------|
| 1 | Add workspace; create **robin-sparkless-core** (schema types, config, error, date_utils; no Polars). |
| 2 | Create **robin-sparkless-expr** (column, functions, expression, type_coercion, udf_registry, udfs; with Polars). |
| 3 | Refactor **robin-sparkless** to depend on core + expr; keep dataframe, session, plan, schema conversion, traits, prelude; preserve public API. |
| 4 | Optionally extract **robin-sparkless-sql** and **robin-sparkless-delta** as separate crates. |
| 5 | Update CI, docs, and release process for the workspace. |

This plan prioritizes the split that gives the largest compile-time benefit: a Polars-free **core** and a dedicated **expr** crate so that Polars-heavy code is isolated and not all rebuilt when only session/dataframe/plan change.
