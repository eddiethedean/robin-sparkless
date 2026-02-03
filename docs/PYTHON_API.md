# Robin-Sparkless Python API (Phase 4 PyO3 Bridge)

This document describes the **Python API contract** exposed by the `robin_sparkless` module when built with the `pyo3` feature. It is intended for Sparkless maintainers implementing the "robin" backend and for anyone using the Python bindings directly.

## Building and installing

**Prerequisites**: Rust (stable), Python 3.8+, [maturin](https://www.maturin.rs/) (`pip install maturin`). The extension is built with **PyO3 0.24**.

```bash
# From the repo root
maturin develop --features pyo3   # Editable install into current env
# With optional SQL and/or Delta:
maturin develop --features "pyo3,sql"       # SQL support
maturin develop --features "pyo3,delta"    # Delta Lake read/write
maturin develop --features "pyo3,sql,delta"  # Both
# or
maturin build --features pyo3     # Build wheel (e.g. target/wheels/)
pip install target/wheels/robin_sparkless-*.whl
```

Rust-only build (no Python):

```bash
cargo build
cargo test
```

Python module is only compiled when the `pyo3` feature is enabled. Default `cargo test` does not include PyO3 code.

## Module surface

| Rust type       | Python class / function | Notes |
|-----------------|-------------------------|--------|
| SparkSession    | `SparkSession`          | `builder()`, `get_or_create()`, `create_dataframe`, `read_csv`, `read_parquet`, `read_json`, `is_case_sensitive()`; with `sql`: `sql(query)`, `create_or_replace_temp_view(name, df)`, `table(name)`; with `delta`: `read_delta(path)`, `read_delta_version(path, version)` |
| SparkSessionBuilder | `SparkSessionBuilder` | `app_name()`, `master()`, `config()`, `get_or_create()` |
| DataFrame       | `DataFrame` | `filter`, `select`, `with_column`, `order_by`, `group_by`, `join`, `union`, `union_by_name`, `distinct`, `drop`, `dropna`, `fillna`, `limit`, `with_column_renamed`, `count`, `show`, `collect`; **Phase 12**: `sample`, `random_split`, `first`, `head`, `tail`, `take`, `is_empty`, `to_json`, `to_pandas`, `explain`, `print_schema`, `checkpoint`, `local_checkpoint`, `repartition`, `coalesce`, `offset`, `summary`, `to_df`, `select_expr`, `col_regex`, `with_columns`, `with_columns_renamed`, `stat`, `na`; with `delta`: `write_delta(path, overwrite)` |
| DataFrameStat   | `DataFrameStat` (returned by `df.stat()`) | `cov(col1, col2)` → `float`, `corr(col1, col2)` → `float` |
| DataFrameNa     | `DataFrameNa` (returned by `df.na()`) | `fill(value: Column)` → `DataFrame`, `drop(subset=None)` → `DataFrame` |
| Column          | `Column` | Built via `col(name)`, `lit(value)`; methods: `gt`, `ge`, `lt`, `le`, `eq`, `ne`, `and_`, `or_`, `alias`, `is_null`, `is_not_null`, `upper`, `lower`, `substr`; **Phase 13**: `ascii_`, `format_number(decimals)`, `overlay(replace, pos, length)`, `char_`, `chr_`, `base64_`, `unbase64_`, `sha1_`, `sha2_(bit_length)`, `md5_`, `array_compact` |
| GroupedData     | `GroupedData` | `count()`, `sum(column)`, `avg(column)`, `min(column)`, `max(column)`, `agg(exprs)` |
| Functions       | Module-level            | `col`, `lit`, `when`, `coalesce`, `sum`, `avg`, `min`, `max`, `count`; **Phase 13**: `ascii`, `format_number`, `overlay`, `position`, `char`, `chr`, `base64`, `unbase64`, `sha1`, `sha2`, `md5`, `array_compact` |

## Key signatures (Python)

- **SparkSession**
  - `SparkSession.builder()` → `SparkSessionBuilder`
  - `get_or_create()` → `SparkSession`
  - `create_dataframe(data: list of (int, int, str), column_names: list of 3 str)` → `DataFrame`
  - `read_csv(path: str)`, `read_parquet(path: str)`, `read_json(path: str)` → `DataFrame`
  - `is_case_sensitive()` → `bool`
  - **When `sql` feature enabled**: `sql(query: str)` → `DataFrame`; `create_or_replace_temp_view(name: str, df: DataFrame)` → `None`; `table(name: str)` → `DataFrame`

- **DataFrame**
  - `filter(condition: Column)` → `DataFrame`
  - `select(cols: list[str])` → `DataFrame`
  - `with_column(column_name: str, expr: Column)` → `DataFrame`
  - `order_by(cols: list[str], ascending: list[bool] | None)` → `DataFrame`
  - `group_by(cols: list[str])` → `GroupedData`
  - `join(other: DataFrame, on: list[str], how: str = "inner")` → `DataFrame` (how: "inner" | "left" | "right" | "outer")
  - `union(other: DataFrame)`, `union_by_name(other: DataFrame)` → `DataFrame`
  - `distinct(subset: list[str] | None = None)` → `DataFrame`
  - `drop(cols: list[str])`, `dropna(subset: list[str] | None = None)` → `DataFrame`
  - `fillna(value: Column)` → `DataFrame`
  - `limit(n: int)`, `with_column_renamed(old: str, new: str)` → `DataFrame`
  - `count()` → `int`
  - `show(n: int | None = None)` → `None`
  - `collect()` → `list[dict[str, Any]]` (list of row dicts)
  - **Phase 12 (completed)**: `sample(with_replacement=False, fraction=1.0, seed=None)` → `DataFrame`; `random_split(weights: list[float], seed=None)` → `list[DataFrame]`; `first()`, `head(n)`, `tail(n)`, `take(n)` → `DataFrame`; `is_empty()` → `bool`; `to_json()` → `list[str]`; `to_pandas()` → `list[dict[str, Any]]` (same as `collect()`; use `pandas.DataFrame.from_records(df.to_pandas())` for a pandas DataFrame); `explain()` → `str`; `print_schema()` → `str`; `checkpoint()`, `local_checkpoint()`, `repartition(n)`, `coalesce(n)`, `offset(n)` → `DataFrame`; `summary()` → `DataFrame`; `to_df(names: list[str])` → `DataFrame`; `select_expr(exprs: list[str])` → `DataFrame`; `col_regex(pattern: str)` → `DataFrame`; `with_columns(mapping: dict[str, Column] | list[tuple[str, Column]])` → `DataFrame`; `with_columns_renamed(mapping: dict[str, str] | list[tuple[str, str]])` → `DataFrame`; `stat()` → `DataFrameStat`; `na()` → `DataFrameNa`.
  - **DataFrameStat** (returned by `df.stat()`): `cov(col1: str, col2: str)` → `float`; `corr(col1: str, col2: str)` → `float`.
  - **DataFrameNa** (returned by `df.na()`): `fill(value: Column)` → `DataFrame`; `drop(subset: list[str] | None = None)` → `DataFrame`.
  - **When `delta` feature enabled**: `write_delta(path: str, overwrite: bool)` → `None`

- **Column / expressions**
  - `col(name: str)` → `Column`
  - `lit(value: None | int | float | bool | str)` → `Column`
  - `when(condition: Column).then(value: Column).otherwise(value: Column)` → `Column`
  - `coalesce(columns: list[Column])` → `Column`
  - `sum(column: Column)`, `avg`, `min`, `max`, `count` → `Column` (aggregation expressions for use in `agg()`)
  - **Phase 13**: `ascii(column)` → `Column` (Int32); `format_number(column, decimals)` → `Column`; `overlay(column, replace, pos, length)` → `Column`; `position(substr, column)` → `Column`; `char(column)` / `chr(column)` → `Column`; `base64(column)`, `unbase64(column)` → `Column`; `sha1(column)`, `sha2(column, bit_length)`, `md5(column)` → `Column` (hex string); `array_compact(column)` → `Column`. Column methods use trailing underscore where name clashes with Python built-in: `ascii_`, `char_`, `chr_`, `base64_`, `unbase64_`, `sha1_`, `sha2_`, `md5_`.

- **GroupedData**
  - `count()` → `DataFrame`
  - `sum(column: str)`, `avg(column: str)`, `min(column: str)`, `max(column: str)` → `DataFrame`
  - `agg(exprs: list[Column])` → `DataFrame` (exprs are aggregation expressions, e.g. `sum(col("x")), count(col("y"))`)

## Data transfer

- **create_dataframe**: Python passes a list of 3-tuples `(int, int, str)` and three column names. Supported schema for this entry point is fixed (id-like, numeric, string). Other schemas may be added later.
- **collect** / **to_pandas**: Both return a **list of Python dicts** (`list[dict[str, Any]]`), one dict per row (column name → value). Types: `None`, `int`, `float`, `bool`, `str` (and `int` for uint64). Use `pandas.DataFrame.from_records(df.to_pandas())` to obtain a pandas DataFrame. Unsupported Polars types raise a clear runtime error.

## Errors

Unsupported operations or invalid arguments raise Python exceptions (e.g. `RuntimeError`, `ValueError`, `TypeError`) with a clear message. Sparkless can catch these and fall back to another backend or fail with a user-friendly message.

## Running Python tests

From the repo root, either:

```bash
make test        # Runs Rust tests, then Python tests (creates .venv if needed, maturin develop --features pyo3, pytest tests/python/)
make test-python # Python tests only (same setup)
```

Or manually (with an activated virtualenv):

```bash
maturin develop --features pyo3
pytest tests/python/
# or
python -m pytest tests/python/
```

## Out of scope (this repo)

- Full Sparkless backend implementation (lives in the Sparkless repo).
- Supporting every Sparkless operation (only the subset implemented in Rust and covered by parity tests).
- Full SQL DDL/DML; UDFs. Delta Lake is optional (see `delta` feature).
