# createDataFrame: Missing Features vs PySpark

Comparison of **robin-sparkless** `createDataFrame(data, schema=None, sampling_ratio=None, verify_schema=True)` with **PySpark** to identify gaps.

---

## Parameters (signature)

| Feature | PySpark | Robin-sparkless |
|--------|---------|------------------|
| `data` | RDD, list, pandas.DataFrame, numpy.ndarray, pyarrow.Table | ✅ list (dicts or list/tuple rows) only |
| `schema=None` | ✅ | ✅ |
| `samplingRatio=None` | ✅ (used for RDD inference) | ✅ accepted, ignored for list data |
| `verifySchema=True` | ✅ (validates each row) | ✅ accepted; validation is best-effort when building |

No missing parameters; optional args are present.

---

## Data input types (`data`)

| Input type | PySpark | Robin-sparkless |
|------------|---------|------------------|
| **List of dicts** | ✅ | ✅ |
| **List of list/tuple** | ✅ | ✅ |
| **List of Row** | ✅ (schema inferred from Row) | ⚠️ May work if Row is sequence-like (extracts as list); not explicitly supported. Dict-like Row (e.g. `row["name"]`) is not tried. |
| **List of namedtuple** | ✅ | ⚠️ May work if extracts as list/tuple. |
| **RDD** | ✅ | ❌ N/A (no RDD in robin-sparkless). |
| **pandas.DataFrame** | ✅ | ❌ Not accepted. User must convert: `df.to_dict("records")` or `list(df.itertuples(index=False))` then pass to createDataFrame. |
| **numpy.ndarray** | ✅ | ❌ Not accepted. Convert to list of rows or list of dicts first. |
| **pyarrow.Table** | ✅ (since 4.0) | ❌ Not accepted. |

**Summary:** The main functional gaps are **pandas.DataFrame**, **numpy.ndarray**, and **pyarrow.Table**. **Row** / **namedtuple** may work when they extract as sequences; explicit support would improve parity.

---

## Schema (`schema`)

| Schema form | PySpark | Robin-sparkless |
|-------------|---------|------------------|
| `None` (infer) | ✅ | ✅ (dict keys or `_1`,`_2`,…; types inferred) |
| List of column names | ✅ | ✅ |
| StructType / .fields | ✅ | ✅ |
| List of (name, type_str) | (via StructType) | ✅ |
| **DDL string** (flat) | ✅ e.g. `"name: string, age: int"` | ✅ (flat only; comma inside `struct<...>` not supported) |
| **DDL string** (nested) | ✅ e.g. `addr struct<street:string,city:string>` | ❌ Current parser splits on every comma. Use **spark-ddl-parser** Rust crate (see TODO) for full DDL. |
| **Single DataType (non-struct)** | ✅ Wraps as single column `"value"`, row as tuple | ❌ Not supported. We require a full struct (multiple named columns). |

**Summary:** Missing: (1) **nested DDL** (until Rust spark-ddl-parser is used), (2) **single-column schema** as a type (e.g. `schema="string"` or `schema=StringType()` with single column "value").

---

## Behavior

| Behavior | PySpark | Robin-sparkless |
|----------|---------|------------------|
| Empty data `[]` | Requires schema (no inference). | ✅ Accepts `[]` with or without schema. |
| verifySchema=True | Validates every row’s types against schema. | Parameter accepted; validation is best-effort during build (Rust side may raise on type mismatch). No per-row verification step. |
| Column order (dict rows) | Follows schema or insertion order. | ✅ Follows schema when given; else first row’s key order. |

**Summary:** Optional improvement: **strict per-row schema verification** when `verify_schema=True` (explicit type check and clear error messages).

---

## Recommended additions (priority)

| # | Addition | GitHub issue |
|---|----------|--------------|
| 1 | **Pandas DataFrame** – Accept `pandas.DataFrame` as `data`; convert to list of dicts (or rows) and call existing path. High value for Python users. | [#416](https://github.com/eddiethedean/robin-sparkless/issues/416) |
| 2 | **Row / namedtuple** – Explicitly support Row (and Row-like) and namedtuple: try dict-like (e.g. `get_item` / keys) then sequence-like so `[Row(a=1, b=2)]` works. | [#417](https://github.com/eddiethedean/robin-sparkless/issues/417) |
| 3 | **Full DDL** – Integrate **spark-ddl-parser** Rust crate so nested `struct<>`, `array<>`, `map<>` in DDL work (see `docs/TODO_SPARK_DDL_PARSER_RUST.md`). | [#418](https://github.com/eddiethedean/robin-sparkless/issues/418) |
| 4 | **Single-column schema** – Allow schema = single type (or string) and treat as one column named `"value"`, wrapping each row in a single-element tuple. | [#419](https://github.com/eddiethedean/robin-sparkless/issues/419) |
| 5 | **verify_schema** – When True, add an explicit per-row type check and raise with a clear message on mismatch. | [#420](https://github.com/eddiethedean/robin-sparkless/issues/420) |
| 6 | **PyArrow Table / numpy** – Lower priority; accept pyarrow.Table and numpy.ndarray as data for better parity. | [#421](https://github.com/eddiethedean/robin-sparkless/issues/421) |
