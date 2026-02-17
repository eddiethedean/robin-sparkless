# Sparkless → Robin-Sparkless Integration Analysis

This document analyzes the [Sparkless](https://github.com/eddiethedean/sparkless) Python project and how **robin-sparkless** (Rust) can eventually replace its backend logic. It covers architecture, structural learnings, and test conversion strategy.

---

## 1. Relationship Between the Projects

| Aspect | Sparkless (Python) | Robin-Sparkless (Rust) |
|--------|-------------------|------------------------|
| **Role** | PySpark drop-in replacement for testing | Pure Rust engine (no JVM) |
| **Backend** | Polars (Python) via `backend/polars/` | Polars (Rust) directly |
| **API** | `from sparkless.sql import SparkSession` | `robin_sparkless::{SparkSession, DataFrame}` |
| **Goal** | Run existing PySpark tests 10x faster | **Become the execution backend** for Sparkless |

**Integration path**: Sparkless (Python) would use FFI to call robin-sparkless for DataFrame execution. Robin-sparkless implements the Polars-backed engine; Sparkless keeps the PySpark API surface, schema parsing, and Python compatibility. The focus of this repo is now the Rust engine and its parity fixtures; Python bindings live out-of-tree.

---

## 2. Sparkless Backend Architecture (What Robin-Sparkless Replaces)

The core execution logic lives in `sparkless/backend/polars/`:

```
sparkless/backend/
├── protocols.py              # QueryExecutor, DataMaterializer, StorageBackend, ExportBackend
├── factory.py                # BackendFactory (dependency injection)
└── polars/
    ├── expression_translator.py   # Column/ColumnOperation → Polars Expr (~5000 lines)
    ├── operation_executor.py      # filter, select, join, groupBy, etc. (~4200 lines)
    ├── materializer.py            # Lazy evaluation / CTE optimization
    ├── window_handler.py          # Window functions
    ├── type_mapper.py             # PySpark types ↔ Polars types
    ├── schema_registry.py         # JSON schema storage
    ├── parquet_storage.py         # Parquet file I/O
    ├── storage.py                 # Polars storage backend
    └── translators/
        ├── arithmetic_translator.py
        ├── string_translator.py
        └── type_translator.py
```

**Robin-sparkless equivalents** (current + planned):

| Sparkless Module | Robin-Sparkless Module | Status |
|------------------|------------------------|--------|
| `expression_translator` | `expression.rs`, `column.rs`, `functions.rs` | Partial – expression parsing exists; many functions missing |
| `operation_executor` | `dataframe.rs`, `session.rs` | Partial – filter, select, orderBy, groupBy, withColumn done |
| `window_handler` | `column.rs`, `functions.rs` (rank, row_number, dense_rank, lag, lead + over) | Done |
| `type_mapper` | `type_coercion.rs`, `schema.rs` | Partial |
| Parquet/CSV/JSON | `session.rs` (DataFrameReader) | Done |

---

## 3. Structural Learnings for Robin-Sparkless

### 3.1 Protocol-Based Backend Abstraction

Sparkless uses Python `Protocol` types for backend interfaces. Robin-sparkless can adopt Rust traits:

```rust
// Future: trait-based backend for pluggability
pub trait QueryExecutor: Send + Sync {
    fn execute_query(&self, query: &str) -> Result<Vec<Row>>;
}

pub trait DataMaterializer: Send + Sync {
    fn materialize(&self, data: &[Row], schema: &StructType, operations: &[Operation]) -> Result<Vec<Row>>;
}
```

### 3.2 Service-Oriented DataFrame Logic

Sparkless splits DataFrame logic into services:
- `transformation_service.py` – select, filter, withColumn
- `aggregation_service.py` – groupBy, agg
- `join_service.py` – joins
- `schema_service.py` – schema ops

Robin-sparkless currently has logic in `dataframe.rs`. As it grows, consider splitting into modules like:
- `src/dataframe/transformations.rs`
- `src/dataframe/aggregations.rs`
- `src/dataframe/joins.rs`

### 3.3 Expression Model: Column vs ColumnOperation vs Literal

Sparkless uses:
- **Column** – simple column reference
- **ColumnOperation** – binary/unary ops (e.g. `col("a") > 5` is `ColumnOperation(operation="gt", column=col("a"), value=5)`)
- **Literal** – constant values

Robin-sparkless `Column` wraps `Expr` directly. For parity, ensure the `Column` API can represent the same expression trees Sparkless builds.

### 3.4 Case Sensitivity

Sparkless has `spark.sql.caseSensitive` (default: false). Robin-sparkless does not yet handle case-insensitive column resolution. Sparkless’s `ColumnResolver` centralizes this.

### 3.5 Function Coverage

Sparkless implements **403+ PySpark functions** ([PYSPARK_FUNCTION_MATRIX.md](https://github.com/eddiethedean/sparkless/blob/main/PYSPARK_FUNCTION_MATRIX.md)). Robin-sparkless has a small subset. Priority for backend replacement:
1. **Aggregates**: count, sum, avg, min, max ✓; extend with stddev, variance, etc.
2. **Conditional**: when/then/otherwise ✓, coalesce ✓
3. **String**: concat, upper, lower, substring ✓; regexp_extract, etc.
4. **Datetime**: date_add, to_date, date_format, etc.
5. **Window**: row_number, rank, lag, lead

---

## 4. Test Conversion Strategy

### 4.1 Fixture Format Comparison

**Robin-sparkless** (`tests/fixtures/*.json`):
```json
{
  "name": "filter_age_gt_30",
  "input": {
    "schema": [{"name": "id", "type": "bigint"}, ...],
    "rows": [[1, 25, "Alice"], ...]
  },
  "operations": [
    {"op": "filter", "expr": "col('age') > 30"},
    {"op": "select", "columns": ["name", "age"]},
    {"op": "orderBy", "columns": ["name"], "ascending": [true]}
  ],
  "expected": {
    "schema": [...],
    "rows": [...]
  }
}
```

**Sparkless** (`tests/expected_outputs/*.json`):
```json
{
  "input_data": [{"id": 1, "name": "Alice", "age": 25}, ...],
  "operation": "filter_operations",
  "expected_output": {
    "schema": {"field_names": [...], "field_types": [...], "fields": [...]},
    "data": [{"age": 35, "name": "Charlie", ...}],
    "row_count": 2
  }
}
```

**Conversion approach**:
1. **Adapter script**: Python or Rust tool that reads Sparkless JSON, outputs robin-sparkless JSON.
2. **Unified format**: Define a common schema that both can consume; Sparkless could emit robin-sparkless format when generating fixtures.
3. **Shared fixtures**: Put canonical fixtures in a shared repo or submodule.

### 4.2 Sparkless Test Categories → Robin-Sparkless

| Sparkless Test Dir | Tests | Conversion Priority | Notes |
|--------------------|-------|---------------------|-------|
| `parity/dataframe/` | filter, select, groupby, join, transformations, window | High | Core DataFrame ops; joins ✅ implemented; window needs implementation |
| `parity/functions/` | aggregate, array, datetime, string, math, null_handling | High | Many map 1:1 to robin-sparkless `functions` |
| `parity/sql/` | queries, DDL, DML | Medium | Robin-sparkless has no SQL yet |
| `unit/` | 47+ unit tests | Medium | Good for isolated behavior |
| `expected_outputs/` | 270+ JSON files | High | Can drive both Python and Rust tests |

### 4.3 Conversion Steps for a Single Test

1. **Identify operations**: e.g. `df.filter(df.age > 30)` → `filter` with `col('age') > 30`
2. **Build input**: Convert `input_data` to robin-sparkless schema + rows
3. **Build operations list**: Map Python calls to `Operation` enum variants
4. **Compare output**: Use existing `assert_schema_eq` / `assert_rows_eq` logic

### 4.4 Automation Ideas

- **Script**: `tests/convert_sparkless_fixtures.py` – reads Sparkless `expected_outputs/`, writes robin-sparkless `fixtures/`
- **CI**: Run Sparkless fixture generator, then run robin-sparkless parity tests on converted fixtures
- **Bidirectional**: When robin-sparkless adds a feature, add fixture; regenerate Sparkless expected if schema aligns

---

## 5. Recommended Next Steps

### Phase 1: Structural Alignment
1. Add `src/dataframe/` submodules (transformations, aggregations, joins) mirroring Sparkless services
2. Document expression model (Column/Expr) and ensure it can represent Sparkless’s ColumnOperation tree
3. Add case sensitivity configuration to match PySpark

### Phase 2: Function Parity
1. Use [PYSPARK_FUNCTION_MATRIX.md](https://github.com/eddiethedean/sparkless/blob/main/PYSPARK_FUNCTION_MATRIX.md) as a checklist
2. Implement high-value functions: string (concat, upper, lower, substring), datetime (date_add, to_date), aggregates (stddev, variance)
3. Add robin-sparkless fixtures for each new function, aligned with Sparkless expected outputs

### Phase 3: Test Conversion
1. Build a fixture converter: Sparkless JSON → robin-sparkless JSON
2. Convert 10–20 high-value Sparkless tests (filter, select, groupby, basic aggregates)
3. Integrate into CI so both projects validate against the same logical fixtures

### Phase 4: Windows
1. ✅ Joins (inner, left, right, outer) implemented in robin-sparkless
2. Implement window functions (row_number, rank, lag, lead)
3. Add parity fixtures; convert Sparkless window tests

---

## 6. Key Files Reference

### Sparkless (Backend Logic)
- `sparkless/backend/polars/operation_executor.py` – filter, select, join, groupBy, withColumn, etc.
- `sparkless/backend/polars/expression_translator.py` – Column → Polars Expr
- `sparkless/dataframe/services/transformation_service.py` – DataFrame transformation API
- `sparkless/functions/` – 26 modules for PySpark functions

### Sparkless (Tests)
- `tests/fixtures/parity_base.py` – ParityTestBase, load_expected, assert_parity
- `tests/tools/output_loader.py` – load_expected_output
- `tests/tools/comparison_utils.py` – assert_dataframes_equal
- `tests/expected_outputs/` – 270+ JSON fixtures

### Robin-Sparkless
- `tests/parity.rs` – run_fixture, create_df_from_input, apply_operations, assert_schema_eq, assert_rows_eq
- `src/expression.rs` – expression parsing for fixture `expr` strings
- `tests/fixtures/` – 29 JSON fixtures

---

## 7. Backend Replacement Viability (Investigation)

This section summarizes the outcome of reading the Sparkless source to assess how viable it is to replace backend logic with **simple** robin-sparkless hookups.

### 7.1 What Is a Simple Hookup

| Area | Simple hookup? | Notes |
|------|----------------|--------|
| **BackendFactory** | Yes | Add `"robin"` branches in `create_storage_backend`, `create_materializer`, `create_export_backend`; extend `get_backend_type(storage)` to detect `"robin"` (e.g. `"robin" in module_name` or class name); add `"robin"` to `list_available_backends()` and allow it in `validate_backend_type`. Sparkless `config.resolve_backend_type()` already calls `BackendFactory.validate_backend_type(candidate_normalized)`, so once "robin" is in the list, `SparkSession.builder.config("spark.sparkless.backend", "robin").getOrCreate()` (or `SPARKLESS_BACKEND=robin`) will resolve to `"robin"`. On the order of 20–40 lines in `sparkless/backend/factory.py`. |
| **ExportBackend** | Yes | Implement a `RobinExporter` that, given a Sparkless DataFrame, obtains rows (e.g. via materialization if needed) and uses `robin_sparkless` only for format conversion if desired, or delegates to existing Sparkless export helpers after converting to a robin DataFrame and calling `collect()` / `to_pandas()`. The protocol is minimal; main method is effectively “DataFrame → external format”. |
| **StorageBackend (IStorageManager)** | Partial | Robin-sparkless has session, temp views, `read_csv`/`read_parquet`/`read_json`, `write_delta`. It does **not** have a first-class catalog (create_schema, create_table, insert_data, query_data). A **RobinStorageManager** can be a thin adapter: e.g. map schema/table to temp view names or file paths; implement create_table/insert_data using robin session + temp view or write to a path. Some methods (e.g. full catalog listing, metadata) may be no-op or limited. |

### 7.2 What Is Not Simple: DataMaterializer

The **DataMaterializer** is the core execution hook. Its contract is:

- `materialize(data: List[Dict], schema: StructType, operations: List[Tuple[str, Any]]) -> List[Row]`

Operations are queued by Sparkless as `(op_name, payload)`. Payloads are **live Python objects**, not serializable primitives:

- **filter**: `payload` = `Column` or `ColumnOperation` (e.g. `F.col("age") > 30` → tree of Column/ColumnOperation/Literal).
- **select**: `payload` = tuple/list of `Column` or `str` (column names or expressions).
- **withColumn**: `payload` = `(col_name: str, expression: Column)`.
- **join**: `payload` = `(other_df: DataFrame, on, how)` (other_df may have `_operations_queue` and must be materialized first).
- **union**: `payload` = other `DataFrame`.
- **orderBy**: `payload` = `(columns, ascending)` (columns can be Column or str).
- **limit** / **offset**: `payload` = `n: int`.
- **groupBy**: `payload` = `(group_by_columns, agg_expressions)` (aggs are Column/AggregateFunction, etc.).
- **distinct**: `payload` = `()`.
- **drop**: `payload` = column name(s) (str or list/tuple).
- **withColumnRenamed**: `payload` = `(old_name: str, new_name: str)`.

So a robin backend **cannot** be “send op name + JSON to Rust”. It must either:

1. **Translate** Sparkless types to robin_sparkless types in Python or another host language: walk Column/ColumnOperation trees and build `robin_sparkless::Column` (and robin DataFrame API calls) via FFI. This is comparable in spirit to `PolarsExpressionTranslator` + `PolarsOperationExecutor`; the bulk of the work is **expression translation**.
2. **Refactor Sparkless** to emit a serializable op format (e.g. expr strings or JSON) that backends interpret. Then robin could implement a thin interpreter. That would be a larger change in Sparkless.

The surface to mirror for translation is:

- **Sparkless**: `backend/polars/expression_translator.py` (`translate(expr, ...)` → Polars `Expr`), `backend/polars/operation_executor.py` (`apply_filter`, `apply_select`, `apply_with_column`, `apply_join`, `apply_union`, `apply_order_by`, `apply_limit`, `apply_offset`, `apply_group_by_agg`, `apply_distinct`, `apply_drop`, `apply_with_column_renamed`).
- **Robin**: Same op names map to `DataFrame.filter`, `select`, `with_column`, `join`, `union`, `order_by`, `limit`, `offset`, `group_by`+`agg`, `distinct`, `drop`, `with_column_renamed`; expressions must be built as `robin_sparkless.Column` via `col`, `lit`, `when`/`then`/`otherwise`, and the function set in PYTHON_API.md.

### 7.3 Materializer Op Names and Payload Shapes (Reference)

| Op name | Payload shape | Notes |
|---------|---------------|--------|
| filter | `Column` or `ColumnOperation` | Condition expression tree. |
| select | `Tuple[Union[Column, str], ...]` or list | Column refs or expressions. |
| withColumn | `(col_name: str, expression: Column)` | New column expression. |
| join | `(other_df: DataFrame, on, how)` | Other df may be lazy; must materialize. |
| union | `other_df: DataFrame` | Same. |
| orderBy | `(columns, ascending)` | columns: tuple/list of Column or str; ascending: bool or list. |
| limit | `n: int` | |
| offset | `n: int` | |
| groupBy | `(group_by, aggs)` | group_by: columns; aggs: list of aggregation expressions (Column/AggregateFunction). |
| distinct | `()` | |
| drop | `str` or `List[str]` / tuple | Column names to drop. |
| withColumnRenamed | `(old_name: str, new_name: str)` | |

### 7.4 Gaps and Out-of-Scope for Initial Robin Backend

- **SQL executor**: Sparkless has a full SQL layer (DDL/DML, MERGE, etc.) that uses the storage backend and may use a different execution path. Robin-sparkless has optional `sql` feature (`spark.sql()`, temp views). Replacing the SQL executor with robin would be a separate effort.
- **Delta MERGE**: Implemented in Sparkless with mock/SQL; not in robin-sparkless.
- **Full catalog**: IStorageManager has create_schema, list_schemas, create_table, insert_data, query_data, etc. Robin has no native catalog; adapter only (e.g. temp views + file paths).
- **Window spec details**: Window functions in Sparkless use `WindowFunction` and partition/order specs. Robin supports window functions; the translator would need to map Sparkless window specs to robin’s `.over(...)` API.

### 7.5 Dependency and install constraints (Sparkless → robin backend)

When Sparkless is used with `backend_type="robin"` (e.g. `SparkSession.builder.config("spark.sparkless.backend", "robin").getOrCreate()` or `SPARKLESS_BACKEND=robin`), it must be able to call the **robin-sparkless** Rust crate via FFI. The exact packaging of those bindings (e.g., a Python wheel maintained in the Sparkless repo) is intentionally left out-of-tree:

- **Rust dependency**: Sparkless (or a companion binding crate) depends on the `robin-sparkless` crate from crates.io and exposes a thin FFI surface.
- **Python-facing package (optional, out-of-tree)**: If Sparkless wants to offer a `sparkless[robin]` extra, that extra would depend on a separately maintained Python package that wraps the Rust crate via FFI. That package is not built or published from this repository.

If the FFI bindings are not available and the user selects the robin backend, `BackendFactory.create_materializer("robin")` should surface a clear error explaining how to enable the robin backend (for example, by installing the appropriate extra in the Sparkless project).

---

## 8. Related Documentation

- [README.md](../README.md) – Project overview and Sparkless integration goal
- [ROADMAP.md](ROADMAP.md) – Development roadmap including integration phases
- [PARITY_STATUS.md](PARITY_STATUS.md) – Parity matrix and Sparkless test conversion
- [TEST_CREATION_GUIDE.md](TEST_CREATION_GUIDE.md) – How to add parity tests; §7 covers Sparkless fixture conversion
- [SPARKLESS_REFACTOR_PLAN.md](SPARKLESS_REFACTOR_PLAN.md) – Refactor plan for Sparkless to prepare for a robin-sparkless backend (serializable logical plan, phased approach)
- [READINESS_FOR_SPARKLESS_PLAN.md](READINESS_FOR_SPARKLESS_PLAN.md) – What robin-sparkless can do in parallel (plan interpreter, expression interpreter, fixture tests, API extensions) to prepare for the post-refactor merge

---

## 9. Summary

| Goal | Action |
|------|--------|
| **Replace Sparkless backend** | Robin-sparkless implements the same operations as `PolarsOperationExecutor` and `PolarsExpressionTranslator` |
| **Learn structure** | Adopt service-style modules, protocol/trait-based backends, centralized column resolution |
| **Convert tests** | Build fixture converter; reuse Sparkless expected_outputs; run robin-sparkless parity on converted fixtures |
| **Function coverage** | Use PYSPARK_FUNCTION_MATRIX.md; prioritize aggregates, string, datetime, window |

Robin-sparkless is well-positioned as the Rust engine. The main gaps are windows and broad function coverage (joins ✅ implemented). Aligning fixture formats and converting Sparkless tests will accelerate parity and ensure both projects stay consistent.
