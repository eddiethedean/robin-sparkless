# Isolating All Polars Code into robin-sparkless-polars

This document describes how to put **all** Polars-using code into a single crate (`robin-sparkless-polars`) so that only one workspace crate depends on the `polars` library. The root crate becomes a thin facade with no direct Polars dependency.

## Goals

- **Single Polars boundary**: Only `robin-sparkless-polars` depends on the `polars` crate. Root and core do not.
- **Preserve public API**: Existing `robin_sparkless::*` and `robin_sparkless::prelude::*` remain unchanged for downstream users.
- **Faster non-Polars iteration**: Changes to core (config, schema types, error types, date_utils) do not trigger a Polars rebuild.

## Current State (as implemented)

| Crate / location | Polars usage |
|------------------|--------------|
| **robin-sparkless-core** | None |
| **robin-sparkless-polars** | Column, functions, UDFs, type_coercion, expression, DataFrame, Session, plan, schema_conv, traits, error (`From<PolarsError>`) — the only crate that depends on Polars |
| **robin-sparkless** (root) | Facade only; re-exports from core and robin-sparkless-polars. No direct Polars dependency. |

Polars is used only in **robin-sparkless-polars**. The former **robin-sparkless-expr** crate was merged into robin-sparkless-polars.

## Target Layout

- **robin-sparkless-core**  
  Unchanged: schema types, config, error (no `From<PolarsError>`), date_utils. No Polars.

- **robin-sparkless-polars** (new)  
  **Only** crate that depends on the `polars` library. It contains:
  - Everything currently in **robin-sparkless-expr**: `column`, `expression`, `functions`, `udfs`, `udf_registry`, `udf_context`, `type_coercion`.
  - Everything in the root that uses Polars: `dataframe/`, `session`, `plan/`, `schema_conv`, `traits`, root `functions` (e.g. `broadcast`), and the Polars-specific error impl.
  - Optional features: `sql`, `delta` (same as today, with same deps: spark-sql-parser, sqlparser, deltalake, tokio).

- **robin-sparkless** (root)  
  Facade only:
  - Depends on `robin-sparkless-core` and `robin-sparkless-polars` (no direct `polars` dependency).
  - Re-exports public API from core and from `robin-sparkless-polars` so that `robin_sparkless::*` and `robin_sparkless::prelude::*` stay the same.
  - Keeps `config` (re-export of core), `schema` (re-export of core types + `schema_from_json` and `StructTypePolarsExt` from the polars crate), `prelude`, and feature flags for `sql`/`delta` that are passed through to `robin-sparkless-polars`.

- **robin-sparkless-expr**  
  **Removed** as a separate crate; its code lives inside `robin-sparkless-polars`.

## Dependency Graph (After)

```
robin-sparkless-core  (no Polars)
         │
         ▼
robin-sparkless-polars  (depends on polars; contains expr + dataframe + session + plan + schema_conv + traits + Polars error impl)
         │
         ▼
robin-sparkless  (facade; re-exports core + robin-sparkless-polars)
```

## Implementation Notes

### Error handling

- **Core** keeps the single `EngineError` type and existing `From` impls (e.g. `serde_json::Error`, `std::io::Error`). It does **not** implement `From<PolarsError>`.
- **robin-sparkless-polars** adds `impl From<PolarsError> for robin_sparkless_core::EngineError` (moving the current root `error.rs` logic into the polars crate). No duplicate enum.
- **Root** re-exports `EngineError` from core; callers still get the same type, with Polars errors convertible via the impl in the polars crate.

### Schema

- **Core**: `DataType`, `StructField`, `StructType` (unchanged).
- **robin-sparkless-polars**: `schema_conv` (Polars schema conversion), `StructTypePolarsExt`, and `schema_from_json` (can live in a small `schema` module that re-exports core types and adds these).
- **Root** `schema.rs`: re-exports from core and from `robin_sparkless_polars` (`StructTypePolarsExt`, `schema_from_json`) so the public API is unchanged.

### Types currently re-exported from root

- `Expr` and `LiteralValue`: today `pub type Expr = polars::prelude::Expr`. These can become re-exports from `robin_sparkless_polars` (which re-exports `polars::prelude::Expr` / `LiteralValue`), so root still exposes `robin_sparkless::Expr` and `robin_sparkless::LiteralValue` without depending on Polars.

### Cargo and features

- **robin-sparkless-polars**:
  - Same `polars` and `polars-plan` versions/features as today.
  - Same extra deps as current root + expr (serde, chrono, regex, rand, etc.).
  - Features `sql` and `delta` mirror current root (spark-sql-parser/sqlparser, deltalake/tokio).
- **robin-sparkless** (root):
  - `[dependencies]`: `robin-sparkless-core`, `robin-sparkless-polars` (with optional features for sql/delta). No direct `polars` dependency.
  - `[features]`: `sql` and `delta` forwarded to `robin-sparkless-polars`.

### Tests and examples

- Tests and examples that use DataFrame/Session/Column stay as they are; they depend on the root crate. The root crate’s re-exports ensure they still see the same API. No need to depend on `robin-sparkless-polars` directly unless we want to add crate-specific tests there.

## Summary

Yes, all Polars code can be isolated into a single crate, **robin-sparkless-polars**, by:

1. Adding the new crate and moving into it everything that currently uses Polars (current expr crate contents + root’s dataframe, session, plan, schema_conv, traits, broadcast, and the `From<PolarsError>` impl).
2. Removing the **robin-sparkless-expr** crate (merged into robin-sparkless-polars).
3. Turning the root into a facade that depends only on core and robin-sparkless-polars and re-exports the existing public API.

That leaves only **robin-sparkless-polars** depending on the `polars` library and keeps the current public API and behavior intact.
