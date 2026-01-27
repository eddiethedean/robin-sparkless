# Roadmap: PySpark Semantics in a Rust Crate (No JVM)

## Core Principle: PySpark Parity on a Polars Backend

**Primary Goal**: Implement a Rust crate whose behavior closely emulates PySpark's `SparkSession` / `DataFrame` / `Column` semantics, but runs entirely in Rust with Polars as the execution engine (no JVM, no Python runtime).

**Constraints**:
- Use Polars as the underlying dataframe/expressions engine.
- Match PySpark behavior where practical (null handling, grouping, joins, expression semantics).
- Stay honest about differences (and document them) when perfect parity is impossible.

### Short-Term Objectives (0–1 month) ✅ **COMPLETED**

1. **Clarify API Surface** ✅
   - ✅ Decided on core PySpark API surface: `SparkSession.builder`, `createDataFrame`, core `DataFrame` transforms/actions
   - ✅ Implemented Rust equivalents with Rust types and error handling

2. **Minimal Parity Slice** ✅
   - ✅ End-to-end support for PySpark-style pipelines in Rust:
     - ✅ Session creation (`SparkSession::builder().get_or_create()`)
     - ✅ `createDataFrame` from simple rows (`Vec<(i64, i64, String)>` tuples)
     - ✅ `select`, `filter`, `groupBy(...).count()`, `orderBy`
     - ✅ `show`, `collect`, `count`
   - ✅ Behavior-checked these operations against PySpark on fixtures (3 test scenarios passing)

3. **Behavioral Tests** ✅
   - ✅ Test harness implemented (`tests/parity.rs`):
     - ✅ Runs pipelines in PySpark via `tests/gen_pyspark_cases.py`
     - ✅ Runs logical equivalent through Robin Sparkless
     - ✅ Compares schemas and results with proper null/type handling
   - ✅ JSON fixtures generated and versioned (`tests/fixtures/*.json`)
   - ✅ All parity tests passing for initial slice

### Medium-Term Objectives (1–3 months) 🚧 **IN PROGRESS**

4. **Data Source Readers** 🔜 **NEXT**
   - [ ] Implement CSV/Parquet/JSON readers using Polars IO
   - [ ] Aim for PySpark-like options and schema inference behavior
   - [ ] Add parity tests for file reading operations

5. **Expression Semantics** ⚠️ **PARTIAL**
   - ✅ Basic `Column` and functions (`col`, `lit`, basic aggregates)
   - [ ] Expand functions: `when`, `coalesce`, more aggregates
   - [ ] Carefully match PySpark's null, type coercion, and comparison semantics
   - [ ] Add more complex filter expressions (AND, OR, NOT, nested conditions)

6. **Grouping and Joins** ⚠️ **PARTIAL**
   - ✅ Basic `groupBy` + `count()` working with parity tests
   - [ ] Additional aggregates: `sum`, `avg`, `min`, `max` on GroupedData
   - [ ] Ensure `groupBy` + aggregates behave like PySpark (especially null/grouping edge cases)
   - [ ] Implement common join types (inner, left, right, outer) and compare behavior against PySpark

### Longer-Term Objectives (3+ months)

7. **Broader API Coverage**
   - Window functions, more SQL/math/string/date functions.
   - UDF story (likely pure-Rust UDFs, with a clear note that Python UDFs are out of scope).

8. **Performance & Robustness**
   - Benchmark against both PySpark and "plain Polars".
   - Ensure we stay within a reasonable performance factor of Polars for supported operations.
   - Harden error handling and diagnostics.

## Success Metrics

We know we're on track if:

- ✅ **Behavioral parity**: For the initial "core" operations (filter, select, orderBy, groupBy+count), PySpark and Robin Sparkless produce the same schema and data on test fixtures. **Status: PASSING**
- ⚠️ **Documentation of differences**: Any divergence from PySpark semantics should be called out explicitly. **Status: TO BE DOCUMENTED**
- ⚠️ **Performance envelope**: For supported operations, we stay within a small constant factor of doing the same thing directly in Polars. **Status: NOT YET BENCHMARKED**

## Current Status (January 2026)

**Completed Core Parity Slice:**
- ✅ `SparkSession::create_dataframe` for simple tuples
- ✅ `DataFrame::filter` with simple expressions (`col('x') > N`)
- ✅ `DataFrame::select` 
- ✅ `DataFrame::order_by` / `sort`
- ✅ `DataFrame::group_by` → `GroupedData::count()`
- ✅ Parity test harness with 3 passing fixtures:
  - `filter_age_gt_30`: filter + select + orderBy
  - `groupby_count`: groupBy + count + orderBy
  - `groupby_with_nulls`: groupBy with null values + count

**Next Priority:**
- Data source readers (CSV/Parquet/JSON) to enable real-world workflows

## Testing Strategy

To enforce the roadmap above, we will:

- **Use PySpark as the oracle**
  - ✅ Maintain a small Python tool (`tests/gen_pyspark_cases.py`) that:
    - ✅ Runs pipelines in PySpark.
    - ✅ Emits JSON fixtures describing inputs, operations, and expected outputs.
  - ✅ Fixtures live under `tests/fixtures/` and are versioned with the repo.

- **Drive Rust tests from fixtures**
  - ✅ `tests/parity.rs`:
    - ✅ Reconstructs a `DataFrame` from each fixture's `input`.
    - ✅ Applies the listed operations (`filter`, `select`, `groupBy+agg`, `orderBy`, etc.) via the Rust API.
    - ✅ Collects results and compares schema + rows against `expected`, with well-defined tolerances (e.g. for floats, or order-insensitive comparisons where PySpark doesn't guarantee ordering).

- **Track parity coverage**
  - ✅ Initial parity test infrastructure in place
  - [ ] Maintain a parity matrix (operations × data types × edge cases) in a doc such as `PARITY_STATUS.md`
  - [ ] Each cell should indicate:
    - "Covered by fixture X",
    - "Not yet covered", or
    - "Intentionally diverges from PySpark (documented)"

This testing strategy makes PySpark behavior the reference and gives us a clear, automated way to detect regressions as the Rust API and Polars versions evolve.
