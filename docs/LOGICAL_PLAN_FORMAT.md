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
  - A list of objects `{"name": "<output_col>", "expr": <expression tree>}` for computed columns.

Example: `{"op": "select", "payload": ["name", "age"]}`

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

- **payload**: `{"group_by": ["col1", ...]}`. Marks a grouping; the next operation in the plan should be **agg**.

### agg

- **payload**: `{"aggs": [{"agg": "sum"|"count"|"avg"|"min"|"max", "column": "<col>"}, ...]}`. Applied to the result of the previous **groupBy**. For `count`, `column` may be omitted or a column name.

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

**Supported functions (MVP / parity subset)**:

| Function   | Args (typical)        | Notes                    |
|-----------|------------------------|--------------------------|
| upper     | [col/expr]             | String to uppercase      |
| lower     | [col/expr]             | String to lowercase      |
| coalesce  | [expr, expr, ...]      | First non-null           |
| when      | (see below)            | Conditional; may be nested with then/otherwise |

**when/otherwise**: Can be represented as `{"fn": "when", "args": [<condition_expr>, <then_expr>]}` with a follow-up convention for chained when/then/otherwise, or as a single object with `condition`, `then`, `otherwise` (the latter being an expr that may itself be a when). Document exact shape when implemented.

Additional functions (length, trim, substring, concat, year, month, etc.) can be added to this table as the expression interpreter is extended.

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

## Plan fixture format

For tests, a plan fixture JSON has:

- **input**: `{ "schema": [{"name": "...", "type": "..."}, ...], "rows": [[...], ...] }`
- **plan**: `[ {"op": "...", "payload": ...}, ... ]`
- **expected**: `{ "schema": [...], "rows": [...] }`

For **join** (and **union**), the left side is the current DataFrame; the right side is provided inside the op payload (`other_data`, `other_schema`). Fixtures that need two top-level inputs can define **input** as the left and put the right in the first join op’s payload.
