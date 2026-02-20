# Plan: Splitting robin-sparkless into Sub-crates

This document outlines the split of `robin-sparkless` into multiple workspace crates. The main goal is **faster iterative compilation**: avoid recompiling Polars when changing only a subset of the codebase.

**Current layout (as implemented):** All Polars-using code lives in **robin-sparkless-polars**; the root **robin-sparkless** is a thin facade with no direct Polars dependency. The former **robin-sparkless-expr** crate was merged into robin-sparkless-polars.

---

## 1. Rationale

- **Goal**: Only one crate (**robin-sparkless-polars**) depends on the `polars` library. Changes to **robin-sparkless-core** (config, schema types, error types, date_utils) do not trigger a Polars rebuild.
- **robin-sparkless** (root) is a facade: it re-exports the public API from core and robin-sparkless-polars so existing dependents do not need to change.

---

## 2. Current Crate Layout (Implemented)

### 2.1 Crate List

| Crate                    | Purpose                                      | Polars? | Notes |
|--------------------------|----------------------------------------------|---------|-------|
| **robin-sparkless-core** | Schema types, config, EngineError, date_utils | No      | Unchanged. |
| **robin-sparkless-polars** | Column, functions, UDFs, DataFrame, Session, Plan, schema conversion, traits, sql, delta | Yes | **Only** crate that depends on `polars`. Contains former expr + main-crate Polars code. |
| **robin-sparkless**      | Facade: re-exports from core and polars      | No      | No direct polars dependency. |
| **spark-sql-parser**     | SQL parsing                                  | No      | Used by robin-sparkless-polars when feature `sql` is enabled. |

- **robin-sparkless-expr** has been **removed**; its code lives in robin-sparkless-polars.
- **sql** and **delta** are optional features on robin-sparkless-polars (and forwarded from the root crate).

### 2.2 Dependency Diagram

```
                    ┌─────────────────────────┐
                    │ robin-sparkless-core    │  (no Polars)
                    │ schema types, config,   │
                    │ EngineError, date_utils │
                    └───────────┬─────────────┘
                                │
                                ▼
                    ┌─────────────────────────┐
                    │ robin-sparkless-polars   │  (only Polars dependency)
                    │ column, functions, udfs,│
                    │ dataframe, session,     │
                    │ plan, schema_conv,      │
                    │ traits, sql, delta      │
                    └───────────┬─────────────┘
                                │
                                ▼
                    ┌─────────────────────────┐
                    │ robin-sparkless         │  (facade; re-exports)
                    │ config, prelude, schema │
                    └─────────────────────────┘
```

- **core** does not depend on Polars.
- **robin-sparkless-polars** is the only crate that depends on `polars`. It contains all expression, DataFrame, Session, and plan execution code.
- **robin-sparkless** (root) depends only on core and robin-sparkless-polars; it re-exports the public API so `robin_sparkless::*` and `robin_sparkless::prelude::*` are unchanged.

---

## 3. Legacy / Original Proposed Layout (Pre–Polars isolation)

The following described an earlier split (core + expr + main). It has been superseded by the **Polars isolation** (core + robin-sparkless-polars + facade), but is kept for reference.

### 3.1 Original Crate List

| Crate                    | Purpose                         | Polars? | Approx. size |
|--------------------------|---------------------------------|---------|--------------|
| **robin-sparkless-core** | Shared types, config, error     | No      | Small        |
| **robin-sparkless-expr** | Column, Expr, functions, UDFs   | Yes     | Large        |
| **robin-sparkless**      | DataFrame, Session, Plan, API  | Yes     | Large        |
| **robin-sparkless-sql**  | SQL parsing and translation    | Via main| Medium       |
| **robin-sparkless-delta**| Delta Lake I/O                  | Via main| Medium       |

### 3.2 Original Dependency Diagram

```
                    ┌─────────────────────────┐
                    │ robin-sparkless-core    │  (no Polars)
                    └───────────┬─────────────┘
                                │
        ┌───────────────────────┼───────────────────────┐
        ▼                       ▼                       │
┌───────────────┐     ┌─────────────────┐              │
│ robin-        │     │ robin-sparkless  │              │
│ sparkless-expr│────►│ (main / facade)  │◄─────────────┘
└───────────────┘     └────────┬────────┘
        │                      │
        └── expr and main both depended on polars.
```

---

## 4. Module → Crate Mapping (Current)

### robin-sparkless-core (no Polars)

- **schema types**: `DataType`, `StructField`, `StructType`. No Polars conversion here.
- **config**: `SparklessConfig`.
- **error**: `EngineError` (variant definitions, `Display`/`Error`; `From<std::io::Error>`, `From<serde_json::Error>`). No `From<PolarsError>` (that impl lives in robin-sparkless-polars).
- **date_utils**: Chrono-only helpers.
- **Dependencies**: `serde`, `serde_json`, `chrono`. No Polars.

### robin-sparkless-polars (only Polars crate)

- **column**, **expression**, **functions**, **type_coercion**, **udf_registry**, **udfs**: Former robin-sparkless-expr contents.
- **dataframe**, **session**, **plan** (mod + expr), **schema_conv**, **traits**, **error** (with `From<PolarsError>` for EngineError), **schema** (re-exports core types + StructTypePolarsExt, schema_from_json).
- **sql**, **delta**: Feature-gated modules (optional).
- **Dependencies**: `robin-sparkless-core`, `polars`, `polars-plan`, and all UDF/expression deps. Optional: spark-sql-parser, sqlparser (sql), deltalake, tokio (delta).

### robin-sparkless (facade)

- **config**, **prelude**, **schema**: Thin modules; re-export from core and/or robin-sparkless-polars.
- **lib.rs**: Re-exports from `robin_sparkless_core` and `robin_sparkless_polars` so `robin_sparkless::*` and `robin_sparkless::prelude::*` are unchanged. Feature `sql` / `delta` forwarded to robin-sparkless-polars.
- **Dependencies**: `robin-sparkless-core`, `robin-sparkless-polars` only. No direct `polars` dependency.

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

## 10. Summary (Current State)

| Crate | Role |
|-------|------|
| **robin-sparkless-core** | Schema types, config, EngineError, date_utils; no Polars. |
| **robin-sparkless-polars** | All Polars-using code (column, functions, udfs, dataframe, session, plan, schema conversion, traits, sql, delta). **Only** crate that depends on `polars`. |
| **robin-sparkless** | Facade: depends only on core and robin-sparkless-polars; re-exports public API. No direct Polars dependency. |

Polars is isolated in **robin-sparkless-polars**. The former **robin-sparkless-expr** crate was merged into it. The root crate is a thin re-export layer so the public API (`robin_sparkless::*`) is unchanged. See [docs/POLARS_CRATE_ISOLATION.md](POLARS_CRATE_ISOLATION.md) for the isolation design.
