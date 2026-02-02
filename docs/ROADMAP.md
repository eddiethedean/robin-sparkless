# Roadmap: PySpark Semantics in a Rust Crate (No JVM)

## Core Principle: PySpark Parity on a Polars Backend

**Primary Goal**: Implement a Rust crate whose behavior closely emulates PySpark's `SparkSession` / `DataFrame` / `Column` semantics, but runs entirely in Rust with Polars as the execution engine (no JVM, no Python runtime).

**Sparkless Integration Goal**: Robin-sparkless is designed to **replace the backend logic** of [Sparkless](https://github.com/eddiethedean/sparkless) (the Python PySpark drop-in). Sparkless would call robin-sparkless via PyO3/FFI for DataFrame execution. See [SPARKLESS_INTEGRATION_ANALYSIS.md](SPARKLESS_INTEGRATION_ANALYSIS.md) for architecture mapping, structural learnings, and test conversion strategy.

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
   - ✅ Behavior-checked these operations against PySpark on fixtures (36 scenarios passing)

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

6. **Grouping and Joins** ✅ **COMPLETE**
   - ✅ Basic `groupBy` + `count()` working with parity tests
   - ✅ Additional aggregates: `sum`, `avg`, `min`, `max` on GroupedData
   - ✅ Generic `agg()` method for multiple aggregations
   - ✅ Column reordering after groupBy to match PySpark order (grouping columns first)
   - [ ] Ensure `groupBy` + aggregates behave like PySpark (especially null/grouping edge cases)
   - ✅ Implement common join types (inner, left, right, outer) with parity fixtures
   - ✅ Multi-agg support in `GroupedData::agg()` with parity fixture
   - ✅ Window functions: row_number, rank, dense_rank, lag, lead with `.over(partition_by)` parity fixtures
- ✅ String functions: upper, lower, substring, concat, concat_ws with parity fixtures

### Longer-Term Objectives (3+ months) – Full Sparkless Backend

The path to full backend replacement is planned in [FULL_BACKEND_ROADMAP.md](FULL_BACKEND_ROADMAP.md). Summary:

| Phase | Goal | Est. Effort |
|-------|------|-------------|
| **1. Foundation** | Structural alignment, case sensitivity, fixture converter | 2–3 weeks |
| **2. High-Value Functions** | String (length, trim, regexp_*), datetime (to_date, date_add), math (stddev, variance) | 4–6 weeks |
| **3. DataFrame Methods** | union, distinct, drop, fillna, limit, withColumnRenamed | 3–4 weeks |
| **4. PyO3 Bridge** | Python bindings so Sparkless can call robin-sparkless | 4–6 weeks |
| **5. Test Conversion** | Convert 50+ Sparkless tests, CI integration | 2–3 weeks |
| **6. Broad Function Parity** | Array, Map, JSON, remaining string/window (~200 functions) | 8–12 weeks |
| **7. SQL & Advanced** | SQL executor, Delta Lake, performance | Ongoing |

7. **Broader API Coverage** (Phases 2 & 6)
   - String: length, trim, regexp_extract, regexp_replace, split, initcap (string basics ✅ done).
   - Datetime: to_date, date_add, date_sub, date_format, year, month, day, etc.
   - Math: abs, ceil, floor, sqrt, stddev, variance, count_distinct.
   - Array/Map/JSON: array_*, map_*, get_json_object, from_json, to_json.
   - Function parity with Sparkless (403+); use [PYSPARK_FUNCTION_MATRIX](https://github.com/eddiethedean/sparkless/blob/main/PYSPARK_FUNCTION_MATRIX.md) as checklist.
   - UDF story: pure-Rust UDFs; Python UDFs out of scope.

8. **DataFrame Methods** (Phase 3)
   - union, unionByName, distinct, drop, dropna, fillna, limit, withColumnRenamed.
   - crossJoin, replace, describe, cache/persist.

9. **PyO3 Bridge** (Phase 4) ✅ **COMPLETED**
   - Optional `pyo3` feature; `robin_sparkless` Python module with SparkSession, DataFrame, Column, GroupedData.
   - `create_dataframe`, `read_csv`/`read_parquet`/`read_json`, filter, select, join, group_by, collect (list of dicts), etc.
   - See [PYTHON_API.md](PYTHON_API.md). Sparkless BackendFactory "robin" option lives in Sparkless repo.

10. **Performance & Robustness** (Phase 7)
    - Benchmark against PySpark and plain Polars.
    - Ensure within 2x of Polars for supported ops.
    - Harden error handling and diagnostics.

### Sparkless Integration Phases (see SPARKLESS_INTEGRATION_ANALYSIS.md, FULL_BACKEND_ROADMAP.md)

- **Phase 1 – Foundation**: Structural alignment (split dataframe.rs), case sensitivity, fixture converter. *Prereqs done: joins, windows, strings.*
- **Phase 2 – High-Value Functions**: String (length, trim, regexp_*), datetime (to_date, date_add), math (stddev, variance)
- **Phase 3 – DataFrame Methods**: union, unionByName, distinct, drop, dropna, fillna, limit, withColumnRenamed ✅ **COMPLETED**
- **Phase 4 – PyO3 Bridge**: Python bindings for Sparkless to call robin-sparkless ✅ **COMPLETED** (see [PYTHON_API.md](PYTHON_API.md))
- **Phase 5 – Test Conversion**: Converter extended (join, window, withColumn, union, distinct, drop, dropna, fillna, limit, withColumnRenamed); parity discovers `tests/fixtures/` + `tests/fixtures/converted/`; `make sparkless-parity` (set SPARKLESS_EXPECTED_OUTPUTS); [SPARKLESS_PARITY_STATUS.md](SPARKLESS_PARITY_STATUS.md) for pass/fail; **51 passing** (50+ target met) ✅ **COMPLETED**
- **Phase 6 – Broad Parity**: Array, Map, JSON, remaining functions (~200)
- **Phase 7 – SQL & Advanced**: SQL executor, Delta Lake, performance

## Success Metrics

We know we're on track if:

- ✅ **Behavioral parity**: For core operations (filter, select, orderBy, groupBy+count/sum/avg/min/max/agg, when/coalesce, basic type coercion, null semantics, joins, window functions, string functions), DataFrame methods (union, distinct, drop, dropna, fillna, limit, withColumnRenamed), and file readers (CSV/Parquet/JSON), PySpark and Robin Sparkless produce the same schema and data on test fixtures. **Status: PASSING (51 fixtures)**
- ⚠️ **Documentation of differences**: Any divergence from PySpark semantics should be called out explicitly. **Status: TO BE DOCUMENTED**
- ⚠️ **Performance envelope**: For supported operations, we stay within a small constant factor of doing the same thing directly in Polars. **Status: NOT YET BENCHMARKED**

**Full backend targets** (see [FULL_BACKEND_ROADMAP.md](FULL_BACKEND_ROADMAP.md)):

| Metric | Current | Phase 5 | Full Backend |
|--------|---------|---------|--------------|
| Parity fixtures | 51 | 80+ | 150+ |
| Functions | ~25 | ~120 | 250+ |
| DataFrame methods | ~25 | ~40 | 60+ |
| Sparkless tests passing (robin backend) | 0 | 50+ | 200+ |
| PyO3 bridge | ✅ Yes (optional) | Yes | Yes |

## Current Status (February 2026)

**Completed Core Parity Slice:**
- ✅ `SparkSession::create_dataframe` for simple tuples
- ✅ `DataFrame::filter` with simple expressions (`col('x') > N`, string comparisons)
- ✅ `DataFrame::select` 
- ✅ `DataFrame::order_by` / `sort`
- ✅ `DataFrame::group_by` → `GroupedData::count()`
- ✅ `DataFrame::with_column` for adding computed columns
- ✅ `DataFrame::join()` for inner, left, right, outer joins
- ✅ File readers: `read_csv()`, `read_parquet()`, `read_json()` with schema inference
- ✅ Expression functions: `when().then().otherwise()`, `coalesce()`
- ✅ GroupedData aggregates: `sum()`, `avg()`, `min()`, `max()`, and generic `agg()`
- ✅ Null comparison semantics: equality/inequality vs NULL, ordering comparisons vs NULL, and `eqNullSafe`
- ✅ Numeric type coercion for int/double comparisons and simple arithmetic
- ✅ Window functions: `Column::rank()`, `row_number()`, `dense_rank()`, `lag()`, `lead()` with `.over(partition_by)`
- ✅ String functions: `upper()`, `lower()`, `substring()`, `concat()`, `concat_ws()`
- ✅ DataFrame methods: `union`, `union_by_name`, `distinct`, `drop`, `dropna`, `fillna`, `limit`, `with_column_renamed`
- ✅ **PyO3 bridge** (optional `pyo3` feature): Python module `robin_sparkless` with SparkSession, DataFrame, Column, GroupedData; `create_dataframe`, filter, select, join, group_by, collect (list of dicts), etc. Build: `maturin develop --features pyo3`. Tests: `make test` runs Rust + Python smoke tests. See [PYTHON_API.md](PYTHON_API.md).
- ✅ Parity test harness with 51 passing fixtures:
  - `filter_age_gt_30`: filter + select + orderBy
  - `filter_and_or`: nested boolean logic with AND/OR and parentheses
  - `filter_nested`: nested boolean logic
  - `filter_not`: NOT / negation semantics
  - `groupby_count`: groupBy + count + orderBy
  - `groupby_with_nulls`: groupBy with null values + count
  - `groupby_sum`: groupBy + sum aggregation
  - `groupby_avg`: groupBy + avg aggregation
  - `read_csv`: CSV file reading + operations
  - `read_parquet`: Parquet file reading + operations
  - `read_json`: JSON file reading + operations
  - `with_logical_column`: boolean columns / logical expressions in withColumn
  - `with_arithmetic_logical_mix`: mixed arithmetic + logical comparison in withColumn
  - `when_otherwise`: when().then().otherwise() conditional expressions
  - `when_then_otherwise`: chained when expressions
  - `coalesce`: null handling with coalesce
  - `null_comparison_equality`: null equality/inequality semantics
  - `null_comparison_ordering`: ordering comparisons vs NULL
  - `null_safe_equality`: null-safe equality (`eqNullSafe`)
  - `null_in_filter`: null handling in filter predicates
  - `type_coercion_numeric`: int vs double comparison coercion
  - `type_coercion_mixed`: int + double arithmetic coercion
  - `inner_join`, `left_join`, `right_join`, `outer_join`: join operations
  - `groupby_multi_agg`: multiple aggregations in one agg() call
  - `row_number_window`, `rank_window`, `lag_lead_window`: window functions
  - `string_upper_lower`, `string_substring`, `string_concat`: string functions
  - `union_all`, `union_by_name`, `distinct`, `drop_columns`, `dropna`, `fillna`, `limit`, `with_column_renamed`: DataFrame methods

**Next Priority** (per [FULL_BACKEND_ROADMAP.md](FULL_BACKEND_ROADMAP.md)):
- Phase 5: Test conversion (convert 50+ Sparkless tests)
- Phase 6: Broad function parity (array, map, JSON, remaining string/window)

## Testing Strategy

To enforce the roadmap above, we will:

- **Use PySpark as the oracle**
  - ✅ Maintain a small Python tool (`tests/gen_pyspark_cases.py`) that:
    - ✅ Runs pipelines in PySpark.
    - ✅ Emits JSON fixtures describing inputs, operations, and expected outputs.
  - ✅ Fixtures live under `tests/fixtures/` and are versioned with the repo.
  - **Sparkless test conversion**: Build a fixture converter to reuse Sparkless's 270+ expected_outputs; see [SPARKLESS_INTEGRATION_ANALYSIS.md](SPARKLESS_INTEGRATION_ANALYSIS.md) §4.

- **Drive Rust tests from fixtures**
  - ✅ `tests/parity.rs`:
    - ✅ Reconstructs a `DataFrame` from each fixture's `input`.
    - ✅ Applies the listed operations (`filter`, `select`, `groupBy+agg`, `orderBy`, etc.) via the Rust API.
    - ✅ Collects results and compares schema + rows against `expected`, with well-defined tolerances (e.g. for floats, or order-insensitive comparisons where PySpark doesn't guarantee ordering).

- **Track parity coverage**
  - ✅ Initial parity test infrastructure in place
  - ✅ Maintain a parity matrix (operations × data types × edge cases) in `PARITY_STATUS.md` (see `PARITY_STATUS.md`)
    - Each cell indicates whether it's covered (and by which fixture), not yet covered, or intentionally diverges.

This testing strategy makes PySpark behavior the reference and gives us a clear, automated way to detect regressions as the Rust API and Polars versions evolve.
