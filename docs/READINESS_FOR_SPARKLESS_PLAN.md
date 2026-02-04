# Robin-Sparkless Readiness for Post-Refactor Merge

While Sparkless implements the [refactor plan](SPARKLESS_REFACTOR_PLAN.md) (serializable logical plan and optional `materialize_from_plan`), we can prepare on the robin-sparkless side so that when the new contract lands, integration is a thin adapter rather than a large translation layer. This document lists **concrete steps we can take here** in parallel.

---

## 1. Plan Interpreter: Execute a Serialized Op List

**Goal**: Add an entry point that takes `(data, schema, logical_plan)` and returns rows, using only robin-sparkless’s existing DataFrame API.

- **Rust**: Add a function (e.g. in `src/session.rs` or a new `src/plan.rs`) such as `execute_plan(session, data, schema, plan: &[PlanOp]) -> Result<DataFrame>` where `PlanOp` is a struct that mirrors the serialized format (op name + payload as serde-friendly types). The implementation loops over the plan and calls existing `filter`, `select`, `join`, etc. based on op name.
- **Python**: Expose it, e.g. `robin_sparkless.execute_plan(data, schema, logical_plan)` returning a list of dicts (or a DataFrame that can be collected). Sparkless’s `RobinMaterializer.materialize_from_plan(data, schema, logical_plan)` would then be a one-liner that calls this and converts rows to Sparkless `Row`.
- **Format**: Start with a minimal schema we control (see §2). When Sparkless publishes their format, we either align to it or add a small adapter that maps their plan to our `PlanOp` shape.

**Outcome**: Sparkless backend becomes “call `execute_plan` and convert to Row”; no Column-tree translation in Python.

---

## 2. Propose a Minimal Logical Plan Schema (Optional Coordination)

**Goal**: So that Sparkless and robin-sparkless don’t diverge, we can publish a minimal “backend plan format” we’re willing to consume.

- **Op list**: List of `{"op": "filter"|"select"|"limit"|... , "payload": ...}`. Payload is op-specific.
- **Payload shapes** (minimal set we need):
  - `filter`: expression tree (see below).
  - `select`: `["col1", "col2"]` or list of expression trees for computed columns.
  - `withColumn`: `{"name": "x", "expr": <expression tree>}`.
  - `join`: `{"other_data": [...], "other_schema": [...], "on": ["id"], "how": "inner"}` (other side as data + schema so it’s serializable).
  - `union`: same idea, other as data + schema.
  - `orderBy`: `{"columns": ["a","b"], "ascending": [true, false]}`.
  - `limit` / `offset`: `{"n": 10}`.
  - `groupBy`: `{"group_by": ["a"], "aggs": [{"agg": "sum", "column": "b"}, ...]}`.
  - `distinct`: `{}`.
  - `drop`: `{"columns": ["x"]}`.
  - `withColumnRenamed`: `{"old": "a", "new": "b"}`.
- **Expression tree**: Recursive structure we can interpret, e.g. `{"col": "age"}`, `{"lit": 30}`, `{"op": "gt", "left": {"col": "age"}, "right": {"lit": 30}}`, `{"op": "and", "left": {...}, "right": {...}}`, and for function calls `{"fn": "upper", "args": [{"col": "name"}]}`. Document the set of ops and functions we support so Sparkless can serialize to that subset (or we extend our interpreter).

**Action**: Add a short doc (e.g. `docs/LOGICAL_PLAN_FORMAT.md`) that defines this schema. Sparkless refactor can target it; if they choose a different format, we add a thin “plan adapter” that converts their plan to ours before execution.

**Outcome**: Clear contract; less risk of incompatible plans at merge time.

---

## 3. Expression Interpreter from Structured Form

**Goal**: Our plan interpreter must evaluate “expression trees” (filter conditions, withColumn exprs, select exprs) that are already in serialized form (dict/list/primitives).

- **Rust**: Add a module that turns a serialized expression (e.g. `{"op": "gt", "left": {"col": "age"}, "right": {"lit": 30}}`) into a Polars `Expr` (or our `Column`). Recursively handle `col`, `lit`, comparison ops, logical ops, and function calls. **Done**: The expression interpreter in `src/plan/expr.rs` now supports **all scalar functions** in robin-sparkless that are valid in filter/select/withColumn (string, math, datetime, type/conditional, binary/bit, array/list, map/struct, misc), delegating to `crate::functions` and `Column`; see [LOGICAL_PLAN_FORMAT.md](LOGICAL_PLAN_FORMAT.md).
- **Coverage**: Full. Any function available in `functions.rs` / `Column` for scalar expressions can be used in plan expression trees.
- **Python**: No need to expose the expression interpreter directly; it’s used internally by `execute_plan`.

**Outcome**: We can run plans that include filter/select/withColumn with structured expressions without any Python-side Column translation.

---

## 4. Tests Against Fixture Plans

**Goal**: Lock in behavior of our plan interpreter and catch regressions.

- **Fixture files**: Add JSON fixtures under e.g. `tests/fixtures/plans/` that represent a logical plan (schema + input rows + op list + expected output rows). Same structure as existing parity fixtures but with a “plan” instead of “operations” in the current robin format.
- **Test**: Load fixture, call `execute_plan` (or the Rust equivalent in tests), assert output schema and rows match expected. **Done**: Three plan fixtures — `filter_select_limit.json`, `join_simple.json`, `with_column_functions.json` (withColumn with upper, when); `plan_parity_fixtures` test in `tests/parity.rs`.
- **Reuse**: When Sparkless publishes sample serialized plans (e.g. from their Phase 1 tests), we can add those as fixtures and run them here to ensure we stay aligned.

**Outcome**: Safe refactors; clear compatibility with whatever plan format we commit to.

---

## 5. Python API: Flexible DataFrame Creation from Rows

**Goal**: Sparkless will pass `data` as list of dicts and `schema` as a list of (name, type) or similar. Our current Python `create_dataframe` only accepts 3-tuple rows and three column names.

- **Extend**: Add an API that accepts a list of dicts (or list of lists) plus a schema description (e.g. list of `(name, dtype_string)` or a single schema dict). Implement by building a Polars DataFrame from the rows and schema, then wrapping in our `DataFrame`. This allows Sparkless to call `materialize_from_plan(data, schema, plan)` with arbitrary schemas and have us create the initial DataFrame without coercing everything to (i64, i64, str).
- **Backward compatibility**: Keep existing `create_dataframe(data: list of 3-tuples, column_names: list of 3 str)`; add e.g. `create_dataframe_from_rows(data: list[dict], schema: list[tuple[str, str]])` or similar.

**Outcome**: Robin backend can handle any schema Sparkless sends, not only the PoC’s 3-column case.

---

## 6. Summary Table

| Item | Owner | Delivers |
|------|--------|----------|
| Plan interpreter (Rust + Python) | robin-sparkless | `execute_plan(data, schema, plan)` → rows |
| Minimal plan schema doc | robin-sparkless | `docs/LOGICAL_PLAN_FORMAT.md` (optional coordination with Sparkless) |
| Expression interpreter (from dict tree) | robin-sparkless | Filter/select/withColumn exprs from serialized form |
| Plan-based fixtures and tests | robin-sparkless | Regression tests for plan execution |
| Flexible `create_dataframe_from_rows` (or equivalent) | robin-sparkless | Arbitrary schema + list of dicts → DataFrame |

Doing these in parallel with Sparkless’s refactor means that when they add `materialize_from_plan` and emit a logical plan, we already have an interpreter and tests; the Sparkless robin backend then just wires their plan into our `execute_plan` and converts results to `Row`.
