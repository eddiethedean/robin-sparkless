# Window-like benchmark performance (Issue #1430)

## Summary

Improves Sparkless 4.0 performance on the window-like **groupBy + join + orderBy** workload described in #1430 by reducing redundant schema work in the engine.

## Changes

### 1. GroupBy aggregation (`GroupedData::agg`)

- **File:** `crates/robin-sparkless-polars/src/dataframe/aggregations.rs`
- **Optimization:** Collect the grouped `LazyFrame` schema **once** at the start of `agg()`, then resolve all aggregation expressions using that schema instead of calling `collect_schema()` per expression/column.
- **How:** Added `resolve_column_with_schema` and `resolve_expr_column_names_with_schema`; `agg()` now does one `lf.collect_schema()?` and passes the schema into resolution for every aggregation expression. Semantics unchanged.

### 2. Join (same-name key path)

- **File:** `crates/robin-sparkless-polars/src/dataframe/joins.rs`
- **Optimization:** For the coalesce-same-name-keys path (e.g. `join(right, on="group", how="inner")`), collect left and right schema **once** and use them for key dtype lookups instead of calling `get_column_dtype()` (and thus `schema_or_collect()`) per key.
- **How:** Added `DataFrame::polars_schema()` in `dataframe/mod.rs`; in the join coercion block we call `left.polars_schema()?` and `right.polars_schema()?` once, then use `schema.get(name)` for each key. Semantics unchanged.

### 3. OrderBy

- No code changes. Per-step timings show `orderBy` is already negligible for this workload; existing implementation is sufficient.

### 4. Benchmark harness and entrypoint

- **`tests/benchmarks/window_like_benchmark.py`** – Python benchmark that reproduces the window-like workload: `createDataFrame → groupBy("group").agg(count/sum/avg) → join(on="group") → orderBy("group","id").collect()`, with configurable `rows`, `groups`, and `repetitions` and per-step timings (createDataFrame, groupBy_agg, join, orderBy, collect).
- **`scripts/run_window_benchmark.py`** – Convenience script to run the benchmark from repo root.
- **`make bench-window`** – Runs the benchmark with default 100k rows, 1k groups, 5 repetitions.

## How to run the benchmark

From repo root (with sparkless installed, e.g. `maturin develop` in `python/` or `pip install -e python/`):

```bash
make bench-window
# or with options:
python scripts/run_window_benchmark.py --rows 100000 --groups 1000 --repetitions 5
```

Output is JSON with `params`, `per_run` timings, and aggregated `stats` (mean/median/min/max) per step.

## Example timings (100k rows, 1k groups, 5 reps)

After these optimizations, a typical run on one machine gives step means in this ballpark:

| Step            | Mean (s) |
|-----------------|----------|
| createDataFrame | ~0.31    |
| groupBy_agg     | ~0.008   |
| join            | ~0.00024 |
| orderBy         | ~0.00004 |
| collect         | ~0.98    |

Most of the total time is in `collect` (Polars execution) and `createDataFrame` (data path into the engine). The engine-side overhead for groupBy, join, and orderBy is small; the optimizations reduce redundant schema work so that overhead stays low as data size grows.

## Testing

- `cargo test -p robin-sparkless-polars -- dataframe::aggregations` – all passed.
- `cargo test -p robin-sparkless-polars -- dataframe::joins` – all passed.
- `pytest tests/parity/dataframe/test_join.py tests/dataframe` – 1701 passed, 6 skipped.

## Non-goals

- Optimizations are scoped to the window-like **groupBy + join + orderBy** pattern and to reducing **schema collection** overhead in the Rust engine. Other workloads are covered by existing tests but may be tuned separately in future work.
- No changes to Python API or public semantics; all changes are internal to the Polars backend.
