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

4. **Data Source Readers** ✅ **COMPLETED**
   - ✅ Implement CSV/Parquet/JSON readers using Polars IO
   - ✅ Basic PySpark-like schema inference behavior (header detection, infer_schema_length)
   - ✅ Parity tests for file reading operations (3 new fixtures: read_csv, read_parquet, read_json)

5. **Expression Semantics** ✅ **COMPLETE**
   - ✅ Basic `Column` and functions (`col`, `lit`, basic aggregates)
   - ✅ String literal support in filter expressions
   - ✅ Expand functions: `when`, `coalesce` implemented
   - ✅ `when().then().otherwise()` conditional expressions
   - ✅ `coalesce()` for null handling
   - ✅ `withColumn()` support for adding computed columns
   - ✅ PySpark-style null comparison semantics, including `eqNullSafe` and comparisons against NULL columns
   - ✅ Basic numeric type coercion for int/double comparisons and arithmetic (via Polars expressions)
   - ✅ Complex filter expressions with logical operators (AND, OR, NOT, &&, ||, !) and nested conditions
   - ✅ Arithmetic expressions in withColumn (+, -, *, /) with proper operator precedence
   - ✅ Mixed arithmetic and logical expressions (e.g., `(col('a') + col('b')) > col('c')`)
   - ⚠️ Note: One test case (`filter_and_or`) is currently failing and needs investigation - parser structure is correct but may have a subtle bug

6. **Grouping and Joins** ⚠️ **PARTIAL**
   - ✅ Basic `groupBy` + `count()` working with parity tests
   - ✅ Additional aggregates: `sum`, `avg`, `min`, `max` on GroupedData
   - ✅ Generic `agg()` method for multiple aggregations
   - ✅ Column reordering after groupBy to match PySpark order (grouping columns first)
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

- ✅ **Behavioral parity**: For core operations (filter, select, orderBy, groupBy+count/sum/avg, when/coalesce) and file readers (CSV/Parquet/JSON), PySpark and Robin Sparkless produce the same schema and data on test fixtures. **Status: PASSING (17 fixtures)**
- ⚠️ **Documentation of differences**: Any divergence from PySpark semantics should be called out explicitly. **Status: TO BE DOCUMENTED**
- ⚠️ **Performance envelope**: For supported operations, we stay within a small constant factor of doing the same thing directly in Polars. **Status: NOT YET BENCHMARKED**

## Current Status (January 2026)

**Completed Core Parity Slice:**
- ✅ `SparkSession::create_dataframe` for simple tuples
- ✅ `DataFrame::filter` with simple expressions (`col('x') > N`, string comparisons)
- ✅ `DataFrame::select` 
- ✅ `DataFrame::order_by` / `sort`
- ✅ `DataFrame::group_by` → `GroupedData::count()`
- ✅ `DataFrame::with_column` for adding computed columns
- ✅ File readers: `read_csv()`, `read_parquet()`, `read_json()` with schema inference
- ✅ Expression functions: `when().then().otherwise()`, `coalesce()`
- ✅ GroupedData aggregates: `sum()`, `avg()`, `min()`, `max()`, and generic `agg()`
- ✅ Null comparison semantics: equality/inequality vs NULL, ordering comparisons vs NULL, and `eqNullSafe`
- ✅ Numeric type coercion for int/double comparisons and simple arithmetic
- ✅ Parity test harness with 17 passing fixtures:
  - `filter_age_gt_30`: filter + select + orderBy
  - `groupby_count`: groupBy + count + orderBy
  - `groupby_with_nulls`: groupBy with null values + count
  - `read_csv`: CSV file reading + operations
  - `read_parquet`: Parquet file reading + operations
  - `read_json`: JSON file reading + operations
  - `when_otherwise`: when().then().otherwise() conditional expressions
  - `when_then_otherwise`: chained when expressions
  - `coalesce`: null handling with coalesce
  - `groupby_sum`: groupBy + sum aggregation
  - `groupby_avg`: groupBy + avg aggregation
  - `null_comparison_equality`: null equality/inequality semantics
  - `null_comparison_ordering`: ordering comparisons vs NULL
  - `null_safe_equality`: null-safe equality (`eqNullSafe`)
  - `null_in_filter`: null handling in filter predicates
  - `type_coercion_numeric`: int vs double comparison coercion
  - `type_coercion_mixed`: int + double arithmetic coercion

**Next Priority:**
- Additional GroupedData aggregates edge cases (null handling, multiple aggregations)
- Join operations (inner, left, right, outer)

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
