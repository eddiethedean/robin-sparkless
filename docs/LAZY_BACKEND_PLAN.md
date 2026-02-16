# Plan: Convert Robin Sparkless Backend to Full Lazy Execution

## Summary

Convert the internal representation of `DataFrame` from **eager** Polars `DataFrame` to **lazy** Polars `LazyFrame`. Transformations would extend the lazy plan; only **actions** (e.g. `collect`, `show`, `write`, `count`) would trigger materialization. This aligns with PySpark semantics, enables Polars query optimization across the full pipeline, and reduces intermediate materializations.

---

## Motivation

1. **Query optimization** — Polars can optimize the entire plan (predicate pushdown, projection pushdown, join reordering, etc.). Today, each operation does `eager → .lazy() → op → .collect()`, so every step is optimized and executed in isolation.

2. **Fewer materializations** — A chain like `read_csv(...).filter(...).select(...).group_by(...).agg(...)` currently materializes after every step. With a lazy backend, we build one plan and execute once at collect/show/write.

3. **PySpark parity** — PySpark DataFrames are lazy until an action runs. A lazy-by-default design better matches Spark semantics.

4. **Performance** — Expected speedup for multi-step pipelines, especially on large data.

---

## Architecture Overview

### Current State

```
DataFrame {
    df: Arc<PlDataFrame>,  // eager
    case_sensitive: bool,
}
```

Each transformation: `df.as_ref().clone().lazy().op().collect()` → new eager DataFrame.

### Target State

```
DataFrame {
    lf: LazyFrame,  // or Arc<LazyFrame> if clone is expensive
    case_sensitive: bool,
    /// Optional cached schema; populated on first schema()/resolve_column_name() via lf.collect_schema()
    schema_cache: Option<Arc<Schema>>,
}
```

Transformations extend the `LazyFrame`; actions call `lf.collect()` (or equivalent).

---

## Schema Handling

Polars `LazyFrame` exposes `collect_schema()` for schema discovery without full collect. For lazy-by-default:

- **`resolve_column_name(name)`** — Call `lf.collect_schema()` (or use cached schema) and resolve case-insensitively against column names.
- **`get_column_dtype(name)`** — Same: use schema from `collect_schema()`.
- **`schema()`** — Return `StructType` from `lf.collect_schema()`; cache result if desired.
- **`coerce_string_numeric_comparisons(expr)`** — Needs column types; use schema from `collect_schema()`.

**Caveat:** `collect_schema()` still does a lightweight execution. For pure scans (Parquet, CSV), Polars can infer schema from metadata. For complex plans (e.g. many expressions), schema may require evaluation. Document that `schema()` can be lazy-expensive in some cases.

---

## Actions vs Transformations

### Actions (must trigger `collect`)

| Method | Notes |
|--------|-------|
| `collect()` | Returns `Arc<PlDataFrame>` |
| `show(n)` | Prints first n rows |
| `count()` | Returns row count |
| `first()` | First row as DataFrame |
| `head(n)` / `limit(n)` | First n rows |
| `take(n)` | Same as head |
| `tail(n)` | Last n rows (needs count → slice) |
| `isEmpty` | Needs count or limit(1).collect |
| `write().parquet/csv/json(...)` | All write paths |
| `write().saveAsTable(...)` | Persistence |
| `createOrReplaceTempView` | Must materialize for SQL catalog (tables are queried by name) |
| `createOrReplaceGlobalTempView` | Same |
| `randomSplit(weights)` | Needs `height()` for split indices |
| `describe()` / `summary()` | Stats over data |
| `sample(fraction/withReplacement)` | Requires count for exact semantics; or use lazy `sample_n` |
| `toPandas()` (Python) | Materializes |
| Python UDF execution | Any row-wise or grouped UDF must materialize |
| `pivot()` | Needs distinct values from pivot column → action |
| `DataFrameStat.corr/cov` | Needs numeric columns from schema + data |

### Transformations (extend lazy plan, no collect)

| Method | Implementation |
|--------|----------------|
| `filter(expr)` | `lf.filter(condition)` |
| `select(...)` | `lf.select(exprs)` |
| `withColumn(name, expr)` | `lf.with_column(expr)` |
| `drop(...)` | `lf.drop(cols)` |
| `orderBy` / `order_by` | `lf.sort(...)` |
| `limit(n)` | Can stay lazy: `lf.slice(0, n)` (but `head`/`take` are actions) |
| `union(other)` | `concat([lf, other.lf], ...)` |
| `join(...)` | `lf.join(other.lf, ...)` |
| `groupBy(...).agg(...)` | `lf.group_by(...).agg(...)` → returns lazy-based DataFrame |
| `dropDuplicates` | `lf.unique(...)` |
| `distinct()` | `lf.unique(...)` |
| `fillna` | `lf.fill_null(...)` |
| `dropna` | `lf.drop_nulls(...)` |
| `toDF(names)` | `lf.with_columns([col.rename(...)])` |
| `selectExpr` | Parse and translate to select |
| Semi/anti joins | `lf.join(..., how=semi/anti)` |

---

## Data Sources

| Source | Current | Target |
|--------|---------|--------|
| `read_csv(path)` | `LazyCsvReader::finish()?.collect()` | Return `DataFrame` wrapping `LazyCsvReader::finish()?` (no collect) |
| `read_parquet(path)` | `LazyFrame::scan_parquet()?.collect()` | Return `DataFrame` wrapping `LazyFrame::scan_parquet()?` |
| `read_json(path)` | `LazyJsonLineReader::finish()?.collect()` | Return `DataFrame` wrapping lazy JSON reader |
| `create_dataframe(rows)` | Build `PlDataFrame`, wrap in `DataFrame` | Build small `PlDataFrame`, then `DataFrame::from_polars(pl_df)` could internally store `pl_df.lazy()` |
| `create_dataframe_from_polars(pl_df)` | Wrap as-is | Store `pl_df.lazy()` |
| `sql(query)` | Translates to operations, collects at end | Same translation, but return lazy `DataFrame` from final plan |
| `read_delta(...)` | Delta scan + collect | Return lazy scan |
| `table(name)` / temp views | Lookup from catalog, return stored DataFrame | Catalog stores lazy `DataFrame`; lookup returns it (lazy) |

**Caveat:** Temp views are used in `sql()`. If the view is lazy, the SQL translator would need to operate on `LazyFrame` (or collect at query execution). Option A: Collect when registering a temp view (simpler). Option B: SQL translator produces a `LazyFrame` from multiple lazy sources (more work).

---

## GroupedData and PivotedGroupedData

Currently `GroupedData` holds:
- `df: PlDataFrame` (eager)
- `lazy_grouped: LazyGroupBy`
- `grouping_cols`, `case_sensitive`

For full lazy:
- `GroupedData` holds `LazyGroupBy` (from `lf.group_by(...)`) + metadata.
- `agg(...)` returns a `DataFrame` wrapping the resulting `LazyFrame` (no collect).
- No need for eager `df` except for Python grouped UDF execution (which must collect).

`PivotedGroupedData` / `CubeRollupData` — similar: hold lazy structures, return lazy `DataFrame` from agg/pivot.

**Pivot:** `pivot(pivot_col, values)` — PySpark allows omitting `values`; then it computes distinct values from the column. That requires an action. Options: (a) Require `values` when going full lazy; (b) Document that pivot without values triggers a schema/collect for distinct values.

---

## UDFs and Python

- **Scalar/vectorized UDFs:** Polars supports `map_batches` / `apply` on `LazyFrame`. We can keep UDFs as expressions that map to `map_batches`; Polars will include them in the plan. Execution still happens at collect.
- **Grouped vectorized UDFs:** Require materialization per group. These remain “action-like” — when used in `group_by().agg(udf(...))`, the plan includes the UDF, and Polars will execute it at collect. No change to the lazy model.
- **Row-wise Python UDFs:** Must materialize (Python GIL, row iteration). These are the boundary where we break the lazy chain: collect → pass to Python → build new DataFrame from result.

---

## Edge Cases

1. **`cache()` / `persist()`** — PySpark materializes on first action. We could add `DataFrame::cache()` which collects and replaces the inner `LazyFrame` with a trivial scan over the cached `PlDataFrame` (e.g. `df.lazy()`).

2. **`createOrReplaceTempView`** — If we store lazy DataFrames in the catalog, `sql("SELECT * FROM t")` would need to build a plan from `t`’s `LazyFrame`. The SQL module already produces Polars operations; we could have it produce/merge `LazyFrame`s instead of collecting. Alternatively, collect on `createOrReplaceTempView` to keep the current SQL implementation.

3. **`isEmpty`** — Use `lf.select([len()]).collect()` or `lf.slice(0, 1).collect()` and check row count to avoid full scan.

4. **`tail(n)`** — Requires total row count. Use `lf.tail(n)` if Polars supports it lazily, or `lf.reverse().head(n).reverse()`.

5. **`describe()` / `summary()`** — Full actions; no change.

6. **`sample(fraction)`** — Can use `lf.sample_fraction(fraction)` (lazy) if Polars supports it.

---

## Migration Strategy

### Phase 1: Core DataFrame Type
- Introduce `DataFrameInner` enum: `Eager(Arc<PlDataFrame>) | Lazy(LazyFrame)`.
- Implement `collect()` as the single place that materializes.
- Change transformations to prefer extending `LazyFrame` when inner is `Lazy`, and to produce `Lazy` when possible.
- Keep `Eager` for compatibility during migration (e.g. `from_polars`).

### Phase 2: Data Sources
- `read_csv`, `read_parquet`, `read_json` return lazy-backed `DataFrame`.
- `create_dataframe` from rows: build small eager `PlDataFrame`, then wrap as `Lazy` via `pl_df.lazy()`.

### Phase 3: All Transformations
- Ensure every transformation path produces/extends `Lazy` and never collects except in actions.
- Update `GroupedData`, `PivotedGroupedData` to work with lazy plans.

### Phase 4: Actions Only Collect
- Audit all public APIs; ensure only actions call `collect()`.
- Add `cache()` if desired.

### Phase 5: Simplify
- Remove `Eager` variant if no longer needed, or keep only for `from_polars` / internal use.
- Update docs and run full parity suite.

---

## Testing

1. **Parity fixtures** — All 200+ fixtures must pass unchanged (same outputs).
2. **New tests** — Verify that chained transformations do not collect until an action (e.g. assert no accidental collect in transformation paths).
3. **Benchmarks** — Compare before/after for pipelines like `read → filter → select → group_by → agg → collect`. Expect improvement for large data.

---

## Files to Modify

| File / Area | Changes |
|-------------|---------|
| `src/dataframe/mod.rs` | DataFrame struct, inner representation, schema/resolve using collect_schema, actions |
| `src/dataframe/transformations.rs` | All transformations to extend LazyFrame, remove .collect() |
| `src/dataframe/aggregations.rs` | GroupedData/PivotedGroupedData to hold LazyGroupBy, return lazy DataFrame |
| `src/dataframe/joins.rs` | Operate on LazyFrame |
| `src/dataframe/stats.rs` | DataFrameStat: collect only in corr/cov/describe |
| `src/session.rs` | read_* return lazy, create_dataframe from lazy, create_dataframe_from_polars → lazy |
| `src/sql/mod.rs` | Produce lazy DataFrame from query plan |
| `src/sql/translator.rs` | Build LazyFrame-based plan |
| `src/delta/mod.rs` | read_delta return lazy |
| `src/python/dataframe.rs` | Ensure collect/show/write are the only materialization points |
| `src/python/udf.rs` | UDF paths that need materialization |
| `src/udfs.rs` | Ensure UDFs integrate with lazy plans (map_batches) |

---

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Schema access becomes slower (collect_schema) | Cache schema; for scans, Polars often infers schema without full read |
| Breaking changes for from_polars users | `from_polars` stores `pl_df.lazy()`; behavior unchanged for transformations + actions |
| SQL + lazy views complexity | Option: collect when registering temp view; SQL operates on eager. Revisit in Phase 3 |
| Python compatibility | Ensure .collect(), .show(), .toPandas() are the only materialization points; test Python parity |
| Polars version drift | Pin to Polars version that supports needed LazyFrame API; document minimum version |

---

## Success Criteria

1. All existing parity fixtures pass.
2. No collect in transformation code paths (only in actions).
3. Benchmark improvement for multi-step pipelines.
4. Python API unchanged from user perspective.
5. Documentation updated to describe lazy execution model.
