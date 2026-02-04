# Sparkless Refactor Plan: Backend Readiness for Robin-Sparkless

This document is a **refactor plan for the Sparkless project**. The goal is to leave Sparkless **functionally the same** (same API, same behavior, same tests) while **preparing the codebase** so that a future **robin-sparkless** engine can be added as a backend with minimal glue code. It is intended for Sparkless maintainers and contributors.

**Context**: Robin-sparkless is a Rust/Polars execution engine exposed as a Python module (`robin_sparkless`). Today, Sparkless backends receive **Python object payloads** (Column, ColumnOperation, DataFrame, etc.), which forces a robin backend to reimplement a large expression translator. A refactor that introduces a **serializable or backend-agnostic operation representation** would allow robin (and other backends) to interpret the same logical plan without depending on Sparkless’s internal types.

---

## 1. Goals and Non-Goals

| Goals | Non-Goals |
|-------|-----------|
| Keep all existing Sparkless behavior and tests passing. | Change the public PySpark-compatible API. |
| Introduce a representation of the lazy operation queue that backends can consume without depending on Column/ColumnOperation trees. | Migrate Sparkless to a different execution model (e.g. full SQL). |
| Allow the Polars backend to continue to work unchanged during and after the refactor (dual path or gradual migration). | Implement the full robin backend in this refactor (that remains in robin-sparkless / Sparkless integration). |
| Document touch points and phases so the refactor can be done incrementally. | Require robin_sparkless to be a dependency of Sparkless. |

---

## 2. Current Architecture (Relevant Parts)

- **Lazy queue**: Each DataFrame has `_operations_queue: List[Tuple[str, Any]]` where each element is `(op_name, payload)`. The queue is built by `_queue_op(op_name, payload)` (e.g. from `dataframe.py`, `transformation_service.py`, `join_service.py`, etc.).
- **Materialization**: When an action (e.g. `collect()`, `count()`) is needed, `LazyEvaluationEngine.materialize(df)` is called. It uses `BackendFactory.get_backend_type(df.storage)` and `BackendFactory.create_materializer(backend_type)`, then calls `materializer.materialize(df.data, df.schema, df._operations_queue)`.
- **Payloads today**: Payloads are **live Python objects**—Column, ColumnOperation, Literal, WindowFunction, other DataFrames, etc. The Polars backend uses `PolarsExpressionTranslator` to turn Column/ColumnOperation into Polars expressions and `PolarsOperationExecutor` to apply each op.

**Key files**:

- `sparkless/dataframe/dataframe.py` – `_queue_op`, `_operations_queue`, `_materialize_if_lazy`
- `sparkless/dataframe/lazy.py` – `queue_operation`, `materialize`, optimizer
- `sparkless/dataframe/services/transformation_service.py` – select, filter, withColumn, etc., call `_queue_op`
- `sparkless/dataframe/services/join_service.py` – join, union, call `_queue_op`
- `sparkless/backend/polars/materializer.py` – consumes `operations` and applies them via `operation_executor` and `expression_translator`
- `sparkless/backend/protocols.py` – `DataMaterializer.materialize(data, schema, operations)`

---

## 3. Target State: Backend-Agnostic Operation Representation

The refactor aims to introduce a **serializable (or at least backend-agnostic) representation** of each operation so that:

1. **Polars backend** can either keep using the current payloads (during transition) or consume the new representation and translate it to Polars the same way it does today.
2. **Robin backend** (future) can consume the new representation and interpret it (e.g. build robin_sparkless API calls or send a serialized plan to the engine) **without** walking Sparkless Column/ColumnOperation trees.

Options for that representation:

- **Option A – Serialized expression strings**: Each op payload includes an expression in a small DSL or SQL-like string (e.g. for filter: `"age > 30"`, for select: `["name", "age"]` or `["id", "age * 2"]`). Backends parse and execute. Pro: easy to log, debug, and send across process boundaries. Con: requires a parser and a well-defined DSL; complex expressions (window, nested structs) need careful design.
- **Option B – Structured JSON-like payloads**: Each op has a payload that is dicts/lists/primitives only (e.g. filter: `{"op": "gt", "left": {"col": "age"}, "right": {"lit": 30}}`). Pro: no parser; backend walks the tree. Con: large surface area; every expression form must have a schema.
- **Option C – Dual representation**: Keep current `(op_name, payload)` for Polars (or derive Polars input from the new form). Add a parallel **serializable plan** (e.g. `df._logical_plan` or payloads that implement a `to_serializable()` method). Materializer contract is extended (or a new method) so backends can ask for the serialized plan and execute it. Pro: incremental; Polars unchanged until ready. Con: two representations to maintain until migration is complete.

**Recommendation**: Phase 1 can follow **Option C** (dual representation): keep existing queue for Polars, add a way to produce a serializable view of the same operations (e.g. a “logical plan” struct or list of dicts). That minimizes risk and allows robin to be implemented against the new format while Polars continues to use the current path. Later phases can move Polars to consume the serialized form only and deprecate passing raw Column trees.

---

## 4. Phased Refactor Plan

### Phase 1: Define and Produce a Serializable Logical Plan (No Backend Change)

- **Goal**: Introduce a formal “logical plan” or “serialized op list” that describes the same operations as `_operations_queue` but in a backend-agnostic form.
- **Actions**:
  1. Define a **schema** for one serialized op (e.g. op name + payload as dict/list/primitives). Document it (e.g. in `docs/internal/logical_plan_format.md`).
  2. Add a function or method that, given `(data, schema, operations)` (current queue), **produces** the serialized plan (e.g. `LazyEvaluationEngine.to_logical_plan(df)` or `materializer.get_serializable_operations(operations)`). For ops that already have simple payloads (limit, offset, drop, withColumnRenamed), serialization is trivial. For filter, select, withColumn, join, union, orderBy, groupBy, the implementation will **walk** the current Column/ColumnOperation/DataFrame payloads and emit the chosen format (e.g. expression trees as dicts, or expr strings).
  3. Do **not** change the materializer contract or any backend yet. Polars backend continues to receive and use the existing `operations` list.
  4. Add **tests** that (a) build a DataFrame with a few ops, (b) produce the serialized plan, (c) assert the structure (e.g. op names and presence of expected keys). Optionally, add a **round-trip** test: serialize then deserialize and compare with original (if deserialization is implemented for tests).
- **Outcome**: Sparkless can emit a logical plan; behavior and all existing tests unchanged.

### Phase 2: Extend the Materializer Contract (Optional Path for Backends)

- **Goal**: Allow a backend to optionally receive the serialized plan instead of (or in addition to) the raw operations.
- **Actions**:
  1. Extend `DataMaterializer` (or add a parallel protocol) with e.g. `materialize_from_plan(self, data, schema, logical_plan) -> List[Row]`, where `logical_plan` is the serialized op list. Document that backends that implement this method can be used when the session is configured to “use logical plan” (e.g. a config or backend capability).
  2. In `LazyEvaluationEngine.materialize(df)`, when creating the materializer, check if the backend supports the new method (e.g. `getattr(materializer, 'materialize_from_plan', None)`). If so, and if a config flag is set (e.g. for future robin backend), call `to_logical_plan(df)` and `materializer.materialize_from_plan(...)`. Otherwise, keep calling `materializer.materialize(data, schema, df._operations_queue)`.
  3. Polars backend does **not** implement `materialize_from_plan` in this phase (or implements it by converting logical plan back to Column trees for existing code paths—only if that’s low effort). So Polars behavior is unchanged.
  4. Add tests for the new code path using a **mock** backend that only implements `materialize_from_plan` and asserts the shape of the plan.
- **Outcome**: Backends can choose to work off the logical plan; Polars still uses the existing path.

### Phase 3: Complete Serialization for All Supported Ops

- **Goal**: Ensure every operation that the Polars materializer supports has a well-defined serialized form (filter with full expression tree, select with column/expr list, withColumn, join, union, orderBy, limit, offset, groupBy, distinct, drop, withColumnRenamed).
- **Actions**:
  1. Implement serialization for the remaining ops (especially filter, select, withColumn, join, union, orderBy, groupBy). Expression trees (Column, ColumnOperation, Literal, WindowFunction, etc.) must map to a recursive dict/list structure or to an expression string DSL.
  2. Document the format for each op and for expressions (e.g. comparison, logical and/or, arithmetic, function calls, column ref, literal).
  3. Add tests per op type: build a DataFrame with that op, call `to_logical_plan`, assert the serialized form contains expected keys and structure.
  4. Handle edge cases: window specs, struct field access, nested expressions. Document any limitations (e.g. “window expressions serialized as opaque blob until Phase 4”).
- **Outcome**: Logical plan is complete enough for a future robin backend to implement an interpreter for all supported ops.

### Phase 4: (Optional) Migrate Polars Backend to Logical Plan

- **Goal**: Simplify Sparkless by having the Polars backend consume only the logical plan, removing the need to pass Column/ColumnOperation trees into the materializer.
- **Actions**:
  1. Implement a **Polars interpreter** that takes the logical plan and builds Polars expressions and operations (effectively moving the logic that today lives in expression_translator + operation_executor into a “from logical plan” path).
  2. Switch `PolarsMaterializer.materialize` to (a) call `to_logical_plan` (or use a precomputed plan if the caller already did that), (b) run the Polars interpreter. Deprecate or remove the path that walks Column trees directly (after a release or two).
  3. Ensure all existing tests and parity tests still pass.
- **Outcome**: Single representation of the logical plan; Polars and any future robin backend both consume the same format.

---

## 5. Touch Points (Summary)

| Area | Files / Components | Change |
|------|--------------------|--------|
| Logical plan format | New module or `dataframe/lazy.py` / `dataframe/logical_plan.py` | Define schema; implement `to_logical_plan(df)` or `serialize_operations(operations, schema)`. |
| Expression serialization | New module or under `functions/` | Walk Column/ColumnOperation/Literal/WindowFunction and emit dict or string. |
| Materializer protocol | `backend/protocols.py` | Optional: add `materialize_from_plan` (or equivalent). |
| Lazy materialization | `dataframe/lazy.py` | In `materialize()`, optionally produce plan and call `materialize_from_plan` when backend supports it. |
| Polars materializer | `backend/polars/materializer.py` | Phase 1–3: no change. Phase 4: add plan-based path and eventually use it only. |
| Queue construction | `dataframe/dataframe.py`, `dataframe/services/*.py`, `dataframe/transformations/operations.py`, etc. | No change to how ops are queued; serialization is done at materialization time from existing queue. |

---

## 6. Testing Strategy

- **Existing tests**: Must remain green after every phase. Run the full Sparkless test suite (unit, parity, integration) on every change.
- **New tests**:
  - Phase 1: Test `to_logical_plan` (or equivalent) for a small set of ops (e.g. filter, select, limit); assert keys and types in the output.
  - Phase 2: Test that a mock backend implementing only `materialize_from_plan` is invoked when configured, and that Polars backend is still invoked when the new path is disabled.
  - Phase 3: Test serialization for each op type; optionally test round-trip (serialize → deserialize → compare).
  - Phase 4: Regression tests; consider reusing existing parity fixtures and asserting same results when Polars runs from the plan.
- **Alternate backends**: Not part of this refactor; once the logical plan is stable, any backend that implements `materialize_from_plan` can add tests that feed the same plan format.

---

## 7. Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Serialization does not cover all expression forms (e.g. some UDFs or window cases). | Phase 3 explicitly documents limitations; complex cases can remain “Polars-only” until the format is extended. Optionally, unsupported forms in the plan cause fallback to “raw” materialize for Polars. |
| Performance: extra work to build the logical plan. | Plan is built only at materialization time; optional path so Polars can skip it until Phase 4. If needed, plan can be cached on the DataFrame. |
| Two representations get out of sync. | Tests that compare behavior (same result when running from queue vs from plan) once Phase 4 or a robin backend exists. |

---

## 8. References

- **Sparkless repository**: [github.com/eddiethedean/sparkless](https://github.com/eddiethedean/sparkless) – codebase this plan applies to. All file paths and module names in this document are relative to that project.

---

## 9. Document History

- **Created**: Standalone refactor plan for Sparkless maintainers. The document is independent and does not rely on other docs; it can live in the Sparkless repo (e.g. `docs/roadmap/` or `docs/internal/`) or be shared elsewhere.
