# Robin-Sparkless Python API (Phase 4 PyO3 Bridge)

This document describes the **Python API contract** exposed by the `robin_sparkless` module when built with the `pyo3` feature. It is intended for Sparkless maintainers implementing the "robin" backend and for anyone using the Python bindings directly.

## Building and installing

**Prerequisites**: Rust (stable), Python 3.8+, [maturin](https://www.maturin.rs/) (`pip install maturin`). The extension is built with **PyO3 0.24**.

```bash
# From the repo root
maturin develop --features pyo3   # Editable install into current env
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
| SparkSession    | `SparkSession`          | `builder()`, `get_or_create()`, `create_dataframe`, `read_csv`, `read_parquet`, `read_json`, `is_case_sensitive()` |
| SparkSessionBuilder | `SparkSessionBuilder` | `app_name()`, `master()`, `config()`, `get_or_create()` |
| DataFrame       | `DataFrame` | `filter`, `select`, `with_column`, `order_by`, `group_by`, `join`, `union`, `union_by_name`, `distinct`, `drop`, `dropna`, `fillna`, `limit`, `with_column_renamed`, `count`, `show`, `collect` |
| Column          | `Column` | Built via `col(name)`, `lit(value)`; methods: `gt`, `ge`, `lt`, `le`, `eq`, `ne`, `and_`, `or_`, `alias`, `is_null`, `is_not_null`, `upper`, `lower`, `substr` |
| GroupedData     | `GroupedData` | `count()`, `sum(column)`, `avg(column)`, `min(column)`, `max(column)`, `agg(exprs)` |
| Functions       | Module-level            | `col`, `lit`, `when`, `coalesce`, `sum`, `avg`, `min`, `max`, `count` |

## Key signatures (Python)

- **SparkSession**
  - `SparkSession.builder()` → `SparkSessionBuilder`
  - `get_or_create()` → `SparkSession`
  - `create_dataframe(data: list of (int, int, str), column_names: list of 3 str)` → `DataFrame`
  - `read_csv(path: str)`, `read_parquet(path: str)`, `read_json(path: str)` → `DataFrame`
  - `is_case_sensitive()` → `bool`

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

- **Column / expressions**
  - `col(name: str)` → `Column`
  - `lit(value: None | int | float | bool | str)` → `Column`
  - `when(condition: Column).then(value: Column).otherwise(value: Column)` → `Column`
  - `coalesce(columns: list[Column])` → `Column`
  - `sum(column: Column)`, `avg`, `min`, `max`, `count` → `Column` (aggregation expressions for use in `agg()`)

- **GroupedData**
  - `count()` → `DataFrame`
  - `sum(column: str)`, `avg(column: str)`, `min(column: str)`, `max(column: str)` → `DataFrame`
  - `agg(exprs: list[Column])` → `DataFrame` (exprs are aggregation expressions, e.g. `sum(col("x")), count(col("y"))`)

## Data transfer

- **create_dataframe**: Python passes a list of 3-tuples `(int, int, str)` and three column names. Supported schema for this entry point is fixed (id-like, numeric, string). Other schemas may be added later.
- **collect**: Returns a **list of Python dicts** (`list[dict[str, Any]]`), one dict per row (column name → value). Types: `None`, `int`, `float`, `bool`, `str` (and `int` for uint64). Unsupported Polars types raise a clear runtime error.

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
- SQL execution, Delta Lake, UDFs.
