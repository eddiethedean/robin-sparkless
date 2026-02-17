# Logical Plan Format (Backend Contract)

This document defines the minimal **backend plan format** that robin-sparkless consumes for `execute_plan`. It is the contract for the plan interpreter and for optional coordination with Sparkless. If Sparkless emits a different format, a thin plan adapter can map it to this shape before execution.

---

## Plan structure

A logical plan is a **list of operations**. Each operation is an object:

```json
{ "op": "<op_name>", "payload": <op_specific> }
```

Operations are applied in order to an initial DataFrame built from `(data, schema)`. The result of each op is the input to the next.

---

## Operation payloads

### filter

- **payload**: A single **expression tree** (see [Expression tree](#expression-tree)). The expression must evaluate to a boolean (e.g. comparison, logical combination). Rows for which the expression is true are kept.

Example: `{"op": "filter", "payload": {"op": "gt", "left": {"col": "age"}, "right": {"lit": 30}}}`

### select

- **payload**: Either:
  - A list of column name strings: `["col1", "col2"]` (project those columns), or
  - An object `{"columns": [...]}` where the array is as above (Sparkless compatibility), or
  - A list of objects `{"name": "<output_col>", "expr": <expression tree>}` for computed columns.

Array items for column projection may be strings or objects with `"name"` (e.g. `{"type": "column", "name": "id"}`).

Example: `{"op": "select", "payload": ["name", "age"]}`  
Example (Sparkless): `{"op": "select", "payload": {"columns": [{"type": "column", "name": "id"}, {"type": "column", "name": "x"}]}}`

### withColumn

- **payload**: `{"name": "<column_name>", "expr": <expression tree>}`. Adds or replaces a column with the result of the expression.

Example: `{"op": "withColumn", "payload": {"name": "upper_name", "expr": {"fn": "upper", "args": [{"col": "name"}]}}}`

### limit

- **payload**: `{"n": <positive_integer>}`. Keeps at most the first `n` rows.

### offset

- **payload**: `{"n": <non_negative_integer>}`. Skips the first `n` rows.

### orderBy

- **payload**: `{"columns": ["a", "b", ...], "ascending": [true, false, ...]}`. Sorts by the given columns; `ascending[i]` applies to `columns[i]`. Optional: `"nulls_first": [true, false, ...]` (if omitted, backend default).

### groupBy

- **payload**: `{"group_by": ["col1", ...], "aggs": [...]}`.
  - `group_by`: list of grouping key column names.
  - Optional `aggs`: see **agg** below. When provided, `groupBy` + `agg` are executed in a single logical step.

### agg

- **payload**: `{"aggs": [...]}`. When used as a separate op, it is applied to the result of the previous **groupBy** (for backwards compatibility). New plans SHOULD prefer putting `aggs` inside the `groupBy` payload.

Each entry in `aggs` is one aggregation:

```json
{ "agg": "sum"|"count"|"avg"|"min"|"max", "column": "<col>", "alias": "<output_col>" }
```

- For `"count"`, `column` may be omitted or a column name.
- Optional `alias`: output column name (e.g. `"sum(v)"`, `"count(v)"` for PySpark-style names).

Grouped Python UDF aggregations (pandas_udf-style GROUPED_AGG) use a dedicated shape:

```json
{
  "agg": "python_grouped_udf",
  "udf": "<udf_name>",
  "args": [ <expression>, ... ],
  "alias": "<output_col>",
  "return_type": "<type_str>"
}
```

- `udf`: Name of a Python UDF registered on the session with `function_type="grouped_agg"`.
- `args`: Expression trees for the UDF arguments (typically column refs).
- `alias`: Output column name in the aggregated DataFrame.
- `return_type`: DDL-style type string (`"int"`, `"bigint"`, `"double"`, `"string"`, etc.).

### join

- **payload**: `{"other_data": [[...], ...], "other_schema": [{"name": "...", "type": "..."}, ...], "on": ["id"], "how": "inner"|"left"|"right"|"outer"}`. The right side is built from `other_data` and `other_schema`; join keys are `on`; `how` is the join type.

### union

- **payload**: `{"other_data": [[...], ...], "other_schema": [{"name": "...", "type": "..."}, ...]}`. Schema must match the current DataFrame (column order and types). Concatenates rows.

### unionByName

- **payload**: Same as **union**; columns are matched by name, allowing different order.

### distinct

- **payload**: `{}`. Deduplicates rows.

### drop

- **payload**: `{"columns": ["x", "y", ...]}`. Removes the listed columns.

### withColumnRenamed

- **payload**: `{"old": "<existing_name>", "new": "<new_name>"}`.

---

## Expression tree

Expressions are recursive structures used in **filter**, **select** (computed columns), and **withColumn**. They are JSON objects (or primitives for literals in some representations).

### Leaves

- **Column reference**: `{"col": "<column_name>"}`
- **Literal**:
  - `{"lit": <number>}` — integer or float
  - `{"lit": "<string>"}` — string
  - `{"lit": true|false}` — boolean
  - Null: `{"lit": null}` or equivalent

### Binary operators

- **Comparison**: `{"op": "eq"|"ne"|"gt"|"ge"|"lt"|"le", "left": <expr>, "right": <expr>}`
- **Null-safe equality**: `{"op": "eq_null_safe", "left": <expr>, "right": <expr>}`
- **Logical**: `{"op": "and"|"or", "left": <expr>, "right": <expr>}`

### Unary

- **Not**: `{"op": "not", "arg": <expr>}`

### Function calls

- **Form**: `{"fn": "<function_name>", "args": [<expr>, ...]}`

**when (two-arg form)**: `{"fn": "when", "args": [<condition_expr>, <then_expr>]}` — evaluates to `<then_expr>` where `<condition_expr>` is true, otherwise null. Chained when/then/otherwise can be represented by nesting further `when` in the else branch.

**Supported functions**: All scalar functions in robin-sparkless that are valid in filter/select/withColumn are supported. The expression interpreter in `src/plan/expr.rs` (in `expr_from_fn` and `expr_from_fn_rest`) is the single source of truth. Categories include:

- **String**: upper, lower, length, trim, ltrim, rtrim, btrim, substring, substr, concat, concat_ws, initcap, repeat, reverse, instr, position, ascii, format_number, overlay, char, chr, base64, unbase64, sha1, sha2, md5, lpad, rpad, translate, mask, substring_index, left, right, replace, startswith, endswith, contains, like, ilike, rlike, regexp, soundex, regexp_extract, regexp_replace, regexp_extract_all, regexp_like, regexp_count, split, split_part, find_in_set, format_string, lcase, ucase, and related.
- **Math/numeric**: abs, ceil, floor, round, bround, negate, sqrt, pow, power, exp, log, ln, sin, cos, tan, asin, acos, atan, atan2, degrees, radians, signum, sign, pmod, factorial, hypot, cosh, sinh, tanh, cbrt, expm1, log1p, log10, log2, rint, e, pi, etc.
- **Datetime**: year, month, day, dayofmonth, quarter, weekofyear, dayofweek, dayofyear, hour, minute, second, to_date, date_format, date_add, date_sub, datediff, last_day, trunc, add_months, months_between, next_day, unix_timestamp, from_unixtime, make_date, make_timestamp, make_timestamp_ntz, timestampadd, timestampdiff, current_date, current_timestamp, extract, date_part, etc.
- **Type/conditional**: cast, try_cast, coalesce, when (two-arg), nvl, nvl2, nullif, ifnull, greatest, least, typeof, try_divide, try_add, try_subtract, try_multiply, width_bucket, equal_null.
- **Binary/bit**: hex, unhex, bin, getbit, bit_and, bit_or, bit_xor, bit_count, bitwise_not, bit_length, octet_length, etc.
- **Array/list**: array_size, size, element_at, try_element_at, array_contains, array_join, array_sort, array_distinct, array_slice, array_compact, array_remove, explode, explode_outer, array_position, array_append, array_prepend, array_insert, array_except, array_intersect, array_union, arrays_overlap, arrays_zip, array_agg, array_sum.
- **Map/struct**: create_map, map_keys, map_values, get (map lookup).
- **Misc**: hash, shift_left, shift_right, version; JVM-style stubs (e.g. spark_partition_id, current_catalog, current_database, current_user, input_file_name) as zero-arg.
- **UDF**: `{"udf": "<udf_name>", "args": [<expr>, ...]}` or `{"fn": "call_udf", "args": [{"lit": "<udf_name>"}, <expr>, ...]}`. Resolved from session registry. Rust UDFs supported in filter/withColumn; Python UDFs in withColumn only (Python UDF in filter returns error).

Aggregates (sum, count, avg, min, max) are used in the **agg** op payload, not in expression trees. Sort-order functions (asc, desc) are used in **orderBy**, not in expressions.

---

## Schema format

Input and expected schemas use a list of column specs:

```json
[
  { "name": "id",   "type": "bigint" },
  { "name": "age",  "type": "bigint" },
  { "name": "name", "type": "string" }
]
```

**Supported type strings**: `bigint`, `int`, `double`, `string`, `boolean`, `date`, `timestamp` (and equivalents the backend maps to Polars dtypes).

---

## Case sensitivity

Column names in expressions and op payloads are resolved **case-insensitively** by default (PySpark parity). When `spark.sql.caseSensitive` is false (the default), `{"col": "ID"}` resolves to schema column `id` if present; `{"col": "id"}` and `{"col": "Id"}` also resolve to `id`. The SparkSession `create_dataframe_from_rows` and plan interpreter use `session.is_case_sensitive()` to set the DataFrame case-sensitivity flag. Sparkless adapters should ensure the session config propagates `spark.sql.caseSensitive` so that `col("ID")` when schema has `id` resolves correctly (issue #508).

---

## Supported operations (Sparkless parity)

Backends such as Sparkless can discover which plan operations robin-sparkless supports by calling:

```python
import robin_sparkless as rs
ops = rs.supported_plan_operations()  # tuple[str, ...]
# e.g. "filter" in ops
```

The returned tuple includes all op names that `_execute_plan` accepts: `filter`, `select`, `limit`, `offset`, `orderBy`, `withColumn`, `withColumnRenamed`, `groupBy`, `join`, `union`, `unionByName`, `distinct`, `drop`. This allows Sparkless to avoid raising "Operation 'filter' is not supported" when the Robin backend does support filter (see issue #202).

---

## Plan fixture format

For tests, a plan fixture JSON has:

- **input**: `{ "schema": [{"name": "...", "type": "..."}, ...], "rows": [[...], ...] }`
- **plan**: `[ {"op": "...", "payload": ...}, ... ]`
- **expected**: `{ "schema": [...], "rows": [...] }`

For **join** (and **union**), the left side is the current DataFrame; the right side is provided inside the op payload (`other_data`, `other_schema`). Fixtures that need two top-level inputs can define **input** as the left and put the right in the first join op’s payload.
