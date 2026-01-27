# Roadmap: PySpark Semantics in a Rust Crate (No JVM)

## Core Principle: PySpark Parity on a Polars Backend

**Primary Goal**: Implement a Rust crate whose behavior closely emulates PySpark’s `SparkSession` / `DataFrame` / `Column` semantics, but runs entirely in Rust with Polars as the execution engine (no JVM, no Python runtime).

**Constraints**:
- Use Polars as the underlying dataframe/expressions engine.
- Match PySpark behavior where practical (null handling, grouping, joins, expression semantics).
- Stay honest about differences (and document them) when perfect parity is impossible.

### Short-Term Objectives (0–1 month)

1. **Clarify API Surface**
   - Decide which parts of the PySpark API we emulate first (`SparkSession.builder`, `createDataFrame`, core `DataFrame` transforms / actions).
   - Sketch Rust equivalents (same names where reasonable, but with Rust types and error handling).

2. **Minimal Parity Slice**
   - End-to-end support for “hello world” PySpark-style pipelines in Rust:
     - Session creation
     - `createDataFrame` from simple rows
     - `select`, `filter`, `groupBy(...).count()`
     - `show`, `collect`, `count`
   - Behavior-check these operations against PySpark on small fixtures.

3. **Behavioral Tests**
   - Introduce a small test harness that:
     - Runs a pipeline in PySpark.
     - Runs the logical equivalent through Robin Sparkless.
     - Compares schemas and results (within reasonable tolerances).

### Medium-Term Objectives (1–3 months)

4. **Data Source Readers**
   - Implement CSV/Parquet/JSON readers using Polars IO.
   - Aim for PySpark-like options and schema inference behavior.

5. **Expression Semantics**
   - Flesh out `Column` and functions (`col`, `lit`, `when`, `coalesce`, aggregates).
   - Carefully match PySpark’s null, type coercion, and comparison semantics where possible.

6. **Grouping and Joins**
   - Ensure `groupBy` + aggregates behave like PySpark (especially null/grouping edge cases).
   - Implement common join types (inner, left, right, outer) and compare behavior against PySpark.

### Longer-Term Objectives (3+ months)

7. **Broader API Coverage**
   - Window functions, more SQL/math/string/date functions.
   - UDF story (likely pure-Rust UDFs, with a clear note that Python UDFs are out of scope).

8. **Performance & Robustness**
   - Benchmark against both PySpark and “plain Polars”.
   - Ensure we stay within a reasonable performance factor of Polars for supported operations.
   - Harden error handling and diagnostics.

## Success Metrics

We know we’re on track if:

- **Behavioral parity**: For a set of agreed “core” operations, PySpark and Robin Sparkless produce the same schema and data on a shared test corpus.
- **Documentation of differences**: Any divergence from PySpark semantics is called out explicitly.
- **Performance envelope**: For supported operations, we stay within a small constant factor of doing the same thing directly in Polars.

## Testing Strategy

To enforce the roadmap above, we will:

- **Use PySpark as the oracle**
  - Maintain a small Python tool (`tests/gen_pyspark_cases.py`) that:
    - Runs pipelines in PySpark.
    - Emits JSON fixtures describing inputs, operations, and expected outputs.
  - Fixtures live under `tests/fixtures/` and are versioned with the repo.

- **Drive Rust tests from fixtures**
  - `tests/parity.rs`:
    - Reconstructs a `DataFrame` from each fixture’s `input`.
    - Applies the listed operations (`filter`, `select`, `groupBy+agg`, `join`, `orderBy`, etc.) via the Rust API.
    - Collects results and compares schema + rows against `expected`, with well-defined tolerances (e.g. for floats, or order-insensitive comparisons where PySpark doesn’t guarantee ordering).

- **Track parity coverage**
  - Maintain a parity matrix (operations × data types × edge cases) in a doc such as `PARITY_STATUS.md`.
  - Each cell should indicate:
    - “Covered by fixture X”,
    - “Not yet covered”, or
    - “Intentionally diverges from PySpark (documented)”.

This testing strategy makes PySpark behavior the reference and gives us a clear, automated way to detect regressions as the Rust API and Polars versions evolve.
