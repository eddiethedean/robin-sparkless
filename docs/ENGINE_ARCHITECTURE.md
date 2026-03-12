## Engine Architecture Overview

This document summarizes the engine layering in `robin-sparkless` and how to extend it while
keeping the design SOLID-aligned (single responsibility, clear interfaces, and dependency
inversion around the execution engine).

### Crate layout

- **`robin-sparkless-core`** (engine-agnostic core)
  - Owns shared types, config, errors, and the expression IR.
  - Key modules:
    - `engine`: traits for `SparkSessionBackend`, `DataFrameBackend`, `GroupedDataBackend`,
      `DataFrameReaderBackend`, `PlanExecutor`, plus `JoinType` and `CollectedRows`.
    - `expr`: engine-agnostic `ExprIr` and helpers (`col`, `when`, `sum`, etc.).
    - `schema`: schema and data-type types (`StructType`, `DataType`).
  - **Does not depend on Polars**; only on serde/json and std.

- **`robin-sparkless-polars`** (Polars backend)
  - Implements the core engine traits for a Polars-backed engine.
  - Key modules:
    - `engine_backend`: `SparkSessionBackend`, `DataFrameBackend`, `GroupedDataBackend`,
      and `DataFrameReaderBackend` impls for the Polars types.
    - `expr_ir`: converts `ExprIr` into Polars `Expr` (`expr_ir_to_expr` and helpers).
    - `plan`: JSON logical plan interpreter plus `PolarsPlanExecutor`, the
      `PlanExecutor<SparkSession>` implementation.
    - `error`: maps `PolarsError` into `EngineError` for trait boundaries.
  - Re-exports Polars-centric types (`Expr`, `PlDataFrame`, `PlDataType`, `PolarsError`)
    and engine-facing helpers for the root crate to use.

- **Root crate (`robin-sparkless`)** (public façade)
  - Provides the public Rust API: `SparkSession`, `DataFrame`, `Column`, functions, and
    helpers geared toward embedding and PySpark-like usage.
  - Key modules:
    - `engine` (new): re-exports engine-agnostic types and traits from `robin-sparkless-core`
      (`ExprIr`, `EngineError`, `StructType`, engine traits, etc.).
    - `polars` (new): re-exports Polars-backed types and helpers from `robin-sparkless-polars`
      (`Expr`, `Column`, `UdfRegistry`, `PolarsError`, `schema_from_json`, etc.).
    - `dataframe`: root-owned `DataFrame`/`GroupedData`/`CubeRollupData` wrappers that
      delegate to the Polars backend. Defines the `EngineDataFrame` trait (IR-based
      operations) implemented for the root `DataFrame`.
    - `session`: root-owned `SparkSession`/`SparkSessionBuilder`/`DataFrameReader` wrappers
      that delegate to the Polars backend.
  - `execute_plan` in the root crate now routes through the engine-generic
    `PlanExecutor<SparkSession>` impl provided by `robin-sparkless-polars`, and then
    adapts the boxed `DataFrameBackend` to a root-owned `DataFrame`.

### Design guidelines

- **Engine-agnostic vs backend-specific code**
  - New engine-generic behavior (shared IR, types, or traits) should live in
    `robin-sparkless-core` and be exposed via the `engine` module in the root crate.
  - Polars-specific behavior (functions that take/return `Expr`, Polars dtypes, or
    `PolarsError`) should live in `robin-sparkless-polars` and be surfaced via the
    `polars` module or via clearly Polars-flavored methods on root types.

- **Expressions**
  - Prefer building expressions in terms of `ExprIr` and adding new variants or call
    names in `robin-sparkless-core::expr`.
  - Implement the mapping from new `ExprIr` operations to Polars `Expr` in
    `robin-sparkless-polars::expr_ir`, using the category-specific helpers (aggregation,
    casts, string, coalesce) to keep the mapping modular.

- **Plan execution**
  - New logical plan behavior should be implemented behind the `PlanExecutor<S>` trait.
    The Polars backend wires this via `PolarsPlanExecutor` in `robin-sparkless-polars::plan`.
  - Root-level plan entry points (`execute_plan`) should always work with
    `DataFrameBackend` and `EngineError` at the boundary, then adapt to root-owned
    `DataFrame`.

- **Errors**
  - Engine traits and IR APIs should use `EngineError` from `robin-sparkless-core`.
  - Backend crates are responsible for converting backend-specific errors (e.g.
    `PolarsError`) into `EngineError` at the trait boundaries (`engine_backend` and
    `error::polars_to_core_error`).

- **When adding new features**
  - Ask: *is this engine-generic or backend-specific?*
    - If engine-generic, add it to `robin-sparkless-core` and re-export via the root
      `engine` module.
    - If backend-specific, add it to `robin-sparkless-polars` and surface it via the
      `polars` module or Polars-specific extension methods.
  - Prefer small, focused traits (e.g. `DataFrameBackend`, `PlanExecutor`) and helper
    functions over large, monolithic modules.
  - Keep the root crate as a façade: it should compose `core` + a backend, not contain
    heavy execution logic itself.

