# Robin-Sparkless Python API (Phase 4 PyO3 Bridge)

This document describes the **Python API contract** exposed by the `robin_sparkless` module when built with the `pyo3` feature. It is intended for Sparkless maintainers implementing the "robin" backend and for anyone using the Python bindings directly.

## Building and installing

Choose one of the following:

### Option 1: Install from PyPI

No Rust or build tools required. Wheels are provided for Linux, macOS, and Windows (see [PyPI](https://pypi.org/project/robin-sparkless/)).

```bash
pip install robin-sparkless
```

### Option 2: Build from source

Use this if you need optional features (SQL, Delta) or want to develop the crate. Prerequisites: Rust (stable), Python 3.8+, and [maturin](https://www.maturin.rs/) (`pip install maturin`). The extension is built with **PyO3 0.24**.

```bash
# From the repo root (recommended for tests: SQL + Delta)
maturin develop --features "pyo3,sql,delta"
# Or minimal:
maturin develop --features pyo3
# Build a wheel:
maturin build --features "pyo3,sql,delta"
pip install target/wheels/robin_sparkless-*.whl
```

### Rust-only (no Python)

```bash
cargo build
cargo test
```

Python module is only compiled when the `pyo3` feature is enabled. Default `cargo test` does not include PyO3 code.

## Module surface

| Rust type       | Python class / function | Notes |
|-----------------|-------------------------|--------|
| SparkSession    | `SparkSession`          | `builder()`, `get_or_create()`, `createDataFrame(data, schema=None, sampling_ratio=None, verify_schema=True)`, `read_csv`, `read_parquet`, `read_json`, `is_case_sensitive()`, `udf()`; with `sql`: `sql(query)`, `create_or_replace_temp_view(name, df)`, `table(name)` (temp view then saved table), `read_delta(name_or_path)` (path → Delta on disk; name → in-memory table), `catalog()` → `listTables(dbName=None)`, `tableExists(tableName, dbName=None)`, `dropTempView(name)`, `dropTable(tableName)`; with `delta`: `read_delta_version(path, version)` for path-based time travel |
| SparkSessionBuilder | `SparkSessionBuilder` | `app_name()`, `master()`, `config()`, `get_or_create()` |
| DataFrame       | `DataFrame` | `filter`, `select`, `with_column`, `order_by`, `group_by`, `join`, `union`, `union_by_name`, `distinct`, `drop`, `dropna`, `fillna`, `limit`, `with_column_renamed`, `count`, `show`, `collect`; **Phase 12**: `sample`, `random_split`, `first`, `head`, `tail`, `take`, `is_empty`, `to_json`, `to_pandas`, `explain`, `print_schema`, `checkpoint`, `local_checkpoint`, `repartition`, `coalesce`, `offset`, `summary`, `to_df`, `select_expr`, `col_regex`, `with_columns`, `with_columns_renamed`, `stat`, `na`; **Gap closure**: `cube(cols)`, `rollup(cols)` → `CubeRollupData`; `write()` → `DataFrameWriter` (`.mode()`, `.format()`, `.save(path)`, `.saveAsTable(name, ...)` with sql); `data()`, `toLocalIterator()` (same as collect); `persist()`, `unpersist()` (no-op); stubs: `rdd`, `foreach`, `foreachPartition`, `mapInPandas`, `mapPartitions`, `storageLevel`, `isStreaming`, `withWatermark`; with `sql`: `createOrReplaceTempView(name)` (uses default session); with `delta`: `write_delta(path, overwrite)`; with `sql`: `write_delta_table(name)` (in-memory table for `read_delta(name)`) |
| CubeRollupData  | `CubeRollupData` (returned by `df.cube()` / `df.rollup()`) | `agg(exprs)` → `DataFrame` |
| DataFrameWriter | `DataFrameWriter` (returned by `df.write()`) | `mode("overwrite"\|"append")`, `format("parquet"\|"csv"\|"json")`, `save(path)` (chainable); with `sql`: `saveAsTable(name, format=None, mode=None, partition_by=None)` (in-memory; mode: "error", "overwrite", "append", "ignore") |
| DataFrameStat   | `DataFrameStat` (returned by `df.stat()`) | `cov(col1, col2)` → `float`, `corr(col1, col2)` → `float` |
| DataFrameNa     | `DataFrameNa` (returned by `df.na()`) | `fill(value: Column)` → `DataFrame`, `drop(subset=None)` → `DataFrame` |
| Column          | `Column` | **#187 (0.4.0)**: `over(partition_by)`, `row_number`, `rank`, `dense_rank`, `lag`, `lead` (window API); **#186**: `lit(date)` / `lit(datetime)`; ... **Phase 16**: `regexp_count`, `regexp_instr`, `regexp_substr`, `split_part`, `find_in_set`; **Phase 17**: `unix_timestamp`, `from_unixtime`, `timestamp_seconds`, etc., `pmod`, `factorial`; **Phase 19**: `try_divide`, `try_add`, `try_subtract`, `try_multiply`, `bit_length`, `typeof_`; **Phase 21**: `btrim`, `locate`, `conv`, `hex`, `unhex`, `bin`, `getbit`, `to_char`, `to_varchar`, `to_number`, `try_to_number`, `try_to_timestamp`, `str_to_map`, `arrays_overlap`, `arrays_zip`, `explode_outer`, `array_agg`; **Phase 22**: `curdate`, `now`, `localtimestamp`, `date_diff`, `dateadd`, `datepart`, `extract`, `date_part`, `unix_micros`, `unix_millis`, `unix_seconds`, `dayname`, `weekday`, `make_timestamp`, `make_timestamp_ntz`, `make_interval`, `timestampadd`, `timestampdiff`, `days`, `hours`, `minutes`, `months`, `years`, `from_utc_timestamp`, `to_utc_timestamp`, `convert_timezone`, `current_timezone`, `to_timestamp` |
| GroupedData     | `GroupedData` | `count()`, `sum(column)`, `avg(column)`, `min(column)`, `max(column)`, `agg(exprs)`; **Phase 19**: `any_value(column)`, `bool_and(column)`, `bool_or(column)`, `product(column)`, `collect_list(column)`, `collect_set(column)`, `count_if(column)`, `percentile(column, p)`, `max_by(value_column, ord_column)`, `min_by(value_column, ord_column)`; **Grouped UDFs (v1)**: `agg([pandas_udf(..., function_type="grouped_agg")(col("x")).alias("out"), ...])` for grouped vectorized UDF aggregations |
| Functions       | Module-level            | ... **Phase 16**: `regexp_count`, `regexp_instr`, `regexp_substr`, `split_part`, `find_in_set`, `format_string`, `printf`; **Phase 17**: `unix_timestamp`, `to_unix_timestamp`, `from_unixtime`, `make_date`, `timestamp_seconds`, `timestamp_millis`, `timestamp_micros`, `unix_date`, `date_from_unix_date`, `pmod`, `factorial`; **Phase 19**: `try_divide(left, right)`, `try_add`, `try_subtract`, `try_multiply`, `width_bucket(value, min_val, max_val, num_bucket)`, `elt(index, columns)`, `bit_length(column)`, `typeof(column)`; **Phase 20**: `asc`, `desc`, `asc_nulls_first`, `asc_nulls_last`, `desc_nulls_first`, `desc_nulls_last`, `bround`, `median`, `mode`, etc.; **Phase 21**: `btrim(column, trim_str=None)`, `locate(substr, column, pos=1)`, `conv(column, from_base, to_base)`, `hex`, `unhex`, `bin`, `getbit(column, pos)`, `to_char`, `to_varchar`, `to_number`, `try_to_number`, `try_to_timestamp`, `str_to_map(column, pair_delim=None, key_value_delim=None)`, `arrays_overlap`, `arrays_zip`, `explode_outer`, `array_agg`; **Phase 22**: `curdate()`, `now()`, `localtimestamp()`, `date_diff(end, start)`, `dateadd(column, n)`, `datepart(column, field)`, `extract(column, field)`, `date_part(column, field)`, `unix_micros`, `unix_millis`, `unix_seconds`, `dayname`, `weekday`, `make_timestamp(year, month, day, hour, minute, sec)`, `make_timestamp_ntz`, `make_interval(years, months, weeks, days, hours, mins, secs)`, `timestampadd(unit, amount, ts)`, `timestampdiff(unit, start, end)`, `days(n)`, `hours(n)`, `minutes(n)`, `months(n)`, `years(n)`, `from_utc_timestamp(column, tz)`, `to_utc_timestamp(column, tz)`, `convert_timezone(source_tz, target_tz, column)`, `current_timezone()`, `to_timestamp(column)`; **Internal (underscore-prefixed, not PySpark)**: `_execute_plan`, `_configure_for_multiprocessing`, `_create_dataframe_from_rows`, `_isin_i64`, `_isin_str`, `_zip_with_coalesce`, `_map_filter_value_gt`, `_map_zip_with_coalesce` |

## Signature parity (PySpark alignment)

A **signature gap analysis** against PySpark (parameter names, types, defaults) is maintained to guide API alignment. See [SIGNATURE_GAP_ANALYSIS.md](SIGNATURE_GAP_ANALYSIS.md) for:

- Counts: exact match, partial (param name differences), missing (PySpark-only), extra (robin-only)
- Per-function and per-class method comparison
- Recommendations (e.g. align param names like `column` → `col` for drop-in compatibility)

Signatures are exported with `scripts/export_pyspark_signatures.py` and `scripts/export_robin_signatures.py`, then compared with `scripts/compare_signatures.py`.

## Key signatures (Python)

- **SparkSession**
  - `SparkSession.builder()` → `SparkSessionBuilder`
  - `get_or_create()` → `SparkSession`
  - **createDataFrame(data, schema=None, sampling_ratio=None, verify_schema=True)** (PySpark parity, #372): primary API. `data` is a list of dicts (keyed by column name) or list of list/tuple (row values in order). `schema` may be `None` (infer names from first dict keys or use `_1`, `_2`, … for list rows; infer types from first non-null per column), a **DDL string** (e.g. `"name: string, age: int"` or `"name string, age int"`), a list of column name strings (use those names, infer types), or a StructType-like object with `.fields` (each field has `.name` and `.dataType.typeName()`), or a list of `(name, dtype_str)` tuples. `sampling_ratio` is ignored for list data (PySpark uses it for RDD schema inference). `verify_schema` (default True) is accepted for API compatibility. Returns `DataFrame`.
  - **Internal**: `_create_dataframe_from_rows(data, schema)` — same as `createDataFrame(data, schema)` with schema as list of `(name, dtype_str)`; retained for compatibility.
  - `read_csv(path: str)`, `read_parquet(path: str)`, `read_json(path: str)` → `DataFrame`
  - `is_case_sensitive()` → `bool`
  - `udf()` → `UDFRegistration` (register scalar or vectorized Python UDFs; see UDF guide)
  - **Python UDF configuration** (when built with `pyo3`): `SparkSession.builder().config("spark.robin.pythonUdf.batchSize", "N")` and `"spark.robin.pythonUdf.maxConcurrentBatches"` can be used to tune vectorized UDF execution. `batchSize` controls how many rows are sent to non-grouped vectorized UDFs per Python call; `maxConcurrentBatches` is reserved for future concurrency controls (Python GIL still limits true parallelism).
  - **When `sql` feature enabled**: `sql(query: str)` → `DataFrame`; `create_or_replace_temp_view(name: str, df: DataFrame)` → `None`; `table(name: str)` → `DataFrame` (resolution: temp view first, then saved table); `read_delta(name_or_path: str)` → `DataFrame` (if path-like: read from Delta on disk; else resolve as table name like `table()`); `catalog()` → Catalog with `listTables(dbName=None)` (names from temp views + saved tables), `tableExists(tableName, dbName=None)`, `dropTempView(name)`, `dropTable(tableName)` (saved tables only).
  - **Internal (plan interpreter)**: Module-level `_execute_plan(data: list[dict]|list[list], schema: list[tuple[str, str]], plan_json: str)` → `DataFrame`. Run a serialized logical plan; call `.collect()` on the result to get `list[dict]`. `plan_json` is e.g. `json.dumps([{"op": "filter", "payload": ...}, ...])`. See [LOGICAL_PLAN_FORMAT.md](LOGICAL_PLAN_FORMAT.md).

- **Internal (multiprocessing)**: `_configure_for_multiprocessing()` → `None`. Call as early as possible (e.g. in `conftest.py`) before any SparkSession/DataFrame operations when using pytest-xdist (`pytest -n N`) or multiprocessing with fork. Alternatively, set `ROBIN_SPARKLESS_MULTIPROCESSING=1` before running. See [Multiprocessing and pytest-xdist](#multiprocessing-and-pytest-xdist) below.

- **DataFrame**
  - `filter(condition: Column)` → `DataFrame`
  - `select(cols: list[str])` → `DataFrame`
  - `with_column(column_name: str, expr: Column)` → `DataFrame` (use a `Column` so `rand(seed)`/`randn(seed)` get one value per row; same for `with_columns`)
  - `order_by(cols: list[str], ascending: list[bool] | None)` → `DataFrame`
  - `group_by(cols: list[str])` → `GroupedData`
  - `cube(cols: list[str])` → `CubeRollupData` (then `.agg(exprs)` → `DataFrame`)
  - `rollup(cols: list[str])` → `CubeRollupData` (then `.agg(exprs)` → `DataFrame`)
  - `write()` → `DataFrameWriter` (then `.mode("overwrite"|"append")`, `.format("parquet"|"csv"|"json")`, `.save(path: str)`; with sql: `.saveAsTable(name: str, format=None, mode=None, partition_by=None)` — in-memory table; mode `"error"` (default), `"overwrite"`, `"append"`, `"ignore"`)
  - With `sql`: `write_delta_table(name: str)` → registers DataFrame as in-memory table so `spark.read_delta(name)` returns it
  - `data()` → same as `collect()` (list of row dicts); `toLocalIterator()` → same as `collect()` (iterable)
  - `persist()`, `unpersist()` → `DataFrame` (no-op)
  - Stubs (raise or no-op): `rdd()` → NotImplementedError; `foreach(f)`, `foreachPartition(f)` → NotImplementedError; `mapInPandas(func, schema)`, `mapPartitions(f, schema)` → NotImplementedError; `storageLevel()` → None; `isStreaming()` → False; `withWatermark(event_time, delay_threshold)` → self (no-op)
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
  - `lit(value: None | int | float | bool | str | datetime.date | datetime.datetime)` → `Column` (#186: date/datetime for PySpark parity)
  - `when(condition: Column).then(value: Column).otherwise(value: Column)` → `Column`
  - **Window (#187)**: `column.over(partition_by: list[str])` → `Column` (partition-only; no order-by in API). Window functions: `col("x").row_number(descending: bool = False).over(["dept"])`, `.rank(descending=False).over(...)`, `.dense_rank(descending=False).over(...)`, `.lag(n: int).over(...)`, `.lead(n: int).over(...)`. Aggregations over window: `sum(col("amount")).over(["id"])`, `avg`, `min`, `max`, `count` similarly.
  - `coalesce(columns: list[Column])` → `Column`
  - `sum(column: Column)`, `avg`, `min`, `max`, `count` → `Column` (aggregation expressions for use in `agg()`)
  - **Phase 13**: `ascii(column)` → `Column` (Int32); `format_number(column, decimals)` → `Column`; `overlay(column, replace, pos, length)` → `Column`; `position(substr, column)` → `Column`; `char(column)` / `chr(column)` → `Column`; `base64(column)`, `unbase64(column)` → `Column`; `sha1(column)`, `sha2(column, bit_length)`, `md5(column)` → `Column` (hex string); `array_compact(column)` → `Column`. Column methods use trailing underscore where name clashes with Python built-in: `ascii_`, `char_`, `chr_`, `base64_`, `unbase64_`, `sha1_`, `sha2_`, `md5_`.
  - **Phase 14**: Math: `sin(column)`, `cos`, `tan`, `asin`, `acos`, `atan`, `atan2(y, x)` → `Column` (Float64); `degrees(column)`, `radians(column)`, `signum(column)` → `Column`. Datetime: `quarter(column)`, `weekofyear(column)`, `dayofweek(column)`, `dayofyear(column)` → `Column`; `add_months(column, n)`, `months_between(end, start)`, `next_day(column, day_of_week: str)` → `Column`. Type: `cast(column, type_name: str)` → `Column` (raises on invalid); `try_cast(column, type_name: str)` → `Column` (null on invalid); `isnan(column)` → `Column` (bool); `greatest(columns: list[Column])`, `least(columns: list[Column])` → `Column`. Column methods: `sin`, `cos`, `tan`, `asin_`, `acos_`, `atan_`, `atan2(x)`, `degrees_`, `radians_`, `signum`, `quarter`, `weekofyear`, `dayofweek`, `dayofyear`, `add_months(n)`, `months_between(start)`, `next_day(day_of_week)`, `cast(type_name)`, `try_cast(type_name)`, `isnan`.
  - **Phase 16**: `regexp_count(column, pattern: str)` → `Column` (Int64); `regexp_instr(column, pattern: str, group_idx: int | None = None)` → `Column` (Int64); `regexp_substr(column, pattern: str)` → `Column`; `split_part(column, delimiter: str, part_num: int)` → `Column`; `find_in_set(str_column, set_column)` → `Column` (Int64); `format_string(format: str, columns: list[Column])` → `Column`; `printf(format: str, columns: list[Column])` → `Column` (alias). Column methods: `regexp_count(pattern)`, `regexp_instr(pattern, group_idx)`, `regexp_substr(pattern)`, `split_part(delimiter, part_num)`, `find_in_set(set_column)`.
  - **Phase 17**: `unix_timestamp(column=None, format=None)` → `Column`; `to_unix_timestamp`, `from_unixtime`, `make_date`, `timestamp_seconds/millis/micros`, `unix_date`, `date_from_unix_date`, `pmod`, `factorial`. Column methods: `unix_timestamp`, `from_unixtime`, `timestamp_seconds`, etc.
  - **Phase 19**: `try_divide(left, right)` → `Column` (null on divide-by-zero); `try_add`, `try_subtract`, `try_multiply` → `Column`; `width_bucket(value, min_val, max_val, num_bucket)` → `Column`; `elt(index, columns: list)` → `Column`; `bit_length(column)`, `typeof(column)` → `Column`. Column methods: `try_divide(right)`, `try_add(right)`, `try_subtract(right)`, `try_multiply(right)`, `bit_length()`, `typeof_()`.
  - **Phase 20**: `asc()`, `desc()`, `asc_nulls_first()`, `asc_nulls_last()`, `desc_nulls_first()`, `desc_nulls_last()` → `SortOrder`; `order_by_exprs(sort_orders)` on DataFrame; `bround(column, scale)`, `median`, `mode`, `stddev_pop`, `var_pop`, `try_sum`, `try_avg`, `negate`, `positive`, `cot`, `csc`, `sec`, `e`, `pi`.
  - **Phase 21**: `btrim(column, trim_str=None)` → `Column`; `locate(substr, column, pos=1)` → `Column`; `conv(column, from_base, to_base)` → `Column`; `hex(column)`, `unhex(column)`, `bin(column)`, `getbit(column, pos)` → `Column`; `to_char(column)`, `to_varchar(column)`, `to_number(column)`, `try_to_number(column)`, `try_to_timestamp(column)` → `Column`; `str_to_map(column, pair_delim=None, key_value_delim=None)` → `Column`; `arrays_overlap(left, right)`, `arrays_zip(left, right)` → `Column`; `explode_outer(column)`, `array_agg(column)` → `Column`. Note: `transform_keys` and `transform_values` require Expr and are Rust-only for now.
  - **Phase 22**: `curdate()`, `now()`, `localtimestamp()` → `Column`; `date_diff(end, start)`, `dateadd(column, n)`, `datepart(column, field)`, `extract(column, field)`, `date_part(column, field)` → `Column`; `unix_micros(column)`, `unix_millis(column)`, `unix_seconds(column)` → `Column`; `dayname(column)`, `weekday(column)` → `Column`; `make_timestamp(year, month, day, hour, minute, sec)`, `make_timestamp_ntz(...)` → `Column`; `make_interval(years, months, weeks, days, hours, mins, secs)` → `Column`; `timestampadd(unit, amount, ts)`, `timestampdiff(unit, start, end)` → `Column`; `days(n)`, `hours(n)`, `minutes(n)`, `months(n)`, `years(n)` → `Column`; `from_utc_timestamp(column, tz)`, `to_utc_timestamp(column, tz)`, `convert_timezone(source_tz, target_tz, column)` → `Column`; `current_timezone()` → `Column`; `to_timestamp(column)` → `Column`.
  - **Phase 24**: `rand(seed=None)` → `Column` (uniform [0, 1)); `randn(seed=None)` → `Column` (standard normal). Use in `with_column` or `with_columns` for one value per row (PySpark-like).

- **GroupedData**
  - `count()` → `DataFrame`
  - `sum(column: str)`, `avg(column: str)`, `min(column: str)`, `max(column: str)` → `DataFrame`
  - `agg(exprs: list[Column])` → `DataFrame` (exprs are aggregation expressions, e.g. `sum(col("x")), count(col("y"))`)
  - **Phase 19**: `any_value(column: str)`, `bool_and(column: str)`, `bool_or(column: str)`, `product(column: str)`, `collect_list(column: str)`, `collect_set(column: str)`, `count_if(column: str)`, `percentile(column: str, p: float)`, `max_by(value_column: str, ord_column: str)`, `min_by(value_column: str, ord_column: str)` → `DataFrame`

  - **Grouped vectorized UDFs (v1)**: Use `pandas_udf(f, return_type, function_type="grouped_agg")` at the module level to register a grouped aggregation UDF, then call it inside `group_by().agg([...])` (one scalar result per group). See `docs/UDF_GUIDE.md` for semantics and limitations (no mixing with built-in aggs in a single `agg` call; not available in SQL or non-grouped contexts).

## Data transfer

- **createDataFrame** / **create_dataframe**: Prefer `createDataFrame(data, schema=None)` for list of dicts (infer), list of tuples with column names, or explicit schema. `create_dataframe(data, column_names)` is a convenience for 3-tuples `(int, int, str)` and three column names only.
- **collect** / **to_pandas**: Both return a **list of Python dicts** (`list[dict[str, Any]]`), one dict per row (column name → value). Types: `None`, `int`, `float`, `bool`, `str` (and `int` for uint64). Use `pandas.DataFrame.from_records(df.to_pandas())` to obtain a pandas DataFrame. Unsupported Polars types raise a clear runtime error.

## Errors

Unsupported operations or invalid arguments raise Python exceptions (e.g. `RuntimeError`, `ValueError`, `TypeError`) with a clear message. Sparkless can catch these and fall back to another backend or fail with a user-friendly message.

## Running Python tests

From the repo root:

```bash
make check-full  # Full check: Rust (fmt, clippy, audit, deny, test) + Python lint (ruff, mypy) + Python tests
make test        # Rust tests, then Python tests (venv, maturin develop --features "pyo3,sql,delta", pytest)
make test-python # Python tests only (same venv/setup)
make lint-python # Python only: ruff format --check, ruff check, mypy
```

Or manually (with an activated virtualenv):

```bash
maturin develop --features "pyo3,sql,delta"
pytest tests/python/ -v
```

## Multiprocessing and pytest-xdist

robin-sparkless uses [Polars](https://www.pola.rs/), which is multithreaded (Rayon) and **not fork-safe**. When using pytest-xdist (`pytest -n N`) or Python `multiprocessing` with the default `fork` start method on Unix, worker processes may crash with "node down: Not properly terminated".

To reduce this risk:

1. **Call `_configure_for_multiprocessing()` early** — before any SparkSession or DataFrame operations. For pytest, add to your `conftest.py`:

   ```python
   import robin_sparkless as rs
   rs._configure_for_multiprocessing()
   ```

2. **Or set the environment variable** before running:

   ```bash
   ROBIN_SPARKLESS_MULTIPROCESSING=1 pytest tests/ -n 4
   ```

3. **Use fewer workers or run serially** — e.g. `pytest -n 4` instead of `-n 12`, or `-n 0` for serial execution.

4. **For custom multiprocessing** — use `multiprocessing.get_context("spawn")` instead of the default `fork`:

   ```python
   from multiprocessing import get_context
   with get_context("spawn").Pool() as pool:
       pool.map(my_function, args)
   ```

See [Polars multiprocessing docs](https://docs.pola.rs/user-guide/misc/multiprocessing/) and [GitHub issue #178](https://github.com/eddiethedean/robin-sparkless/issues/178).

## Out of scope (this repo)

- Full Sparkless backend implementation (lives in the Sparkless repo).
- Supporting every Sparkless operation (only the subset implemented in Rust and covered by parity tests).
- Full SQL DDL/DML; UDFs. Delta Lake is optional (see `delta` feature).
