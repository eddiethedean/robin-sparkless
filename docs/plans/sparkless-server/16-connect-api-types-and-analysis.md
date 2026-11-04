---
search:
  exclude: true
---

> **Archive / historical record:** this page describes a past snapshot or a plan.
> Use the [current support contract](../../SUPPORT_MATRIX.md) for release behavior.

# SS-16: Expanded Connect DataFrame API, types, and analysis

> **Status: planned; owner unassigned.** Phase: 7; target 4.23.0.
> [Index](README.md) · [Roadmap](ROADMAP.md) · [Decisions](DECISIONS.md) · [Validation](VALIDATION.md)

## Outcome and scope

Expand the default Polars Connect backend to **all existing implemented local `sparkless.sql` native features**, as required by [SS-22](22-local-sql-feature-parity.md) and its [named baseline](LOCAL_SQL_FEATURE_BASELINE.md), beyond the frozen 4.18 subset. The operations below are starting slices, not a final whitelist. Cover general comparisons and compound boolean/null predicates, arithmetic and checked casts, all existing local built-ins and overloads, including string/date/JSON/collection/hash/statistical/approximate functions, `selectExpr`, wildcard projections, column add/rename/drop, joins, grouped/distinct aggregates, sorting, windows, and set operations (`union`, `unionByName`, intersect, except, and their distinct/all forms). Freeze exact local signatures, type rules and supported window frames before implementation; pivot/cube/rollup, locally implemented grouping sets, reshaping/NA/statistics/sampling, generators, current/session functions, unbounded batch-window endpoints and all other implemented baseline entries are required. Universal Spark parity is a separate scope.

Enable input, analysis, execution and exact remote output for every locally implemented type, including BYTE, FLOAT, BINARY, TIMESTAMP_NTZ, arrays, maps, nested structs and the actual local interval/variant/char/varchar representations. Include Row/type/schema utilities and collection/Arrow/Pandas/iterator conversions in client/bridge acceptance; type names alone do not imply richer semantics than the local implementation. Preserve decimal precision, timestamp semantics, nested metadata, nullability, duplicate labels, and typed empty results. Do not infer Connect support from an embedded engine mapping alone.

Add truthful public `explain()`/tree representations and the analysis requests needed by the expanded API. Document native plan output as native output; do not fabricate Catalyst plans or statistics. Add explicitly enumerated mutable runtime settings with session isolation and analysis/execution consistency. Arbitrary Spark configurations remain unsupported unless individually enabled.

## Complete local SQL and client surface

Complete SS-22 LSQL-04: every locally supported SQL query/mutation statement and expression on HTTP and Connect, using shared parser/engine semantics and versioned exact encodings where needed. Cover UPDATE/DELETE, CTAS/replace, SHOW/DESCRIBE, CTE/subquery/generator/window combinations and all locally supported DDL options. Integrate worker/storage/connector SQL rows in their owning Phases 5/6/8. Include session/config/catalog/views, actions/display/conversions, error aliases and client-side builders/aliases/operators. Standard Connect gaps receive the explicit Sparkless compatibility bridge defined in D21; they cannot silently remove a baseline feature.

## Dependencies and ordered delivery

Reuse SS-08–10 schema, execution, resource, and lifecycle contracts. Phase 4's DuckDB matrix is an input, not evidence that Polars supports the same nodes. Python workers and Delta/UC are optional and are not required to use this phase's native DataFrame API.

| Slice | Deliverable | Exit evidence |
| --- | --- | --- |
| 423-01 | Freeze D16/D21 complete local API/SQL/type/config/analysis registry and client/reference pins (LSQL-01) | Extend the LSQL-01 registry established before Phase 5; actual unmodified-client fixtures, backend differences, exact function/window/set semantics |
| 423-02 | Extend scalar and nested input/result types | Analysis/Arrow agreement, boundary/null/empty cases, bounded nested/dictionary/decoded expansion |
| 423-03 | Implement every baseline native function/operator/relation and client utility, plus HTTP/Connect local SQL (LSQL-04) | Reference values and schemas for compositions, joins, grouping, ordering, windows, and set multiplicity |
| 423-04 | Implement expanded analysis and mutable settings | Honest native plans, scoped configuration changes, invalidation rules, unsupported-setting failures |
| 423-05 | Complete V39/V40 and applicable V56–V58 baseline gates, default/optional backend regressions and distribution | Exact candidate/public artifact checks at coordinated 4.23.0 |

## Acceptance

- **V39:** run the entire enabled type/expression/relation matrix against the pinned Spark reference through the unmodified Connect client. Cover case/ANSI/null behavior, decimal/overflow, NaN policy, nested transformations, ordering ties, window frames, set duplicates, and empty input. Reject unsupported descendants before effects.
- **V40:** schema and public analysis agree with execution; supported configuration changes affect only their session and cannot race an operation's semantic snapshot. Explain output is identified accurately. Measure memory/queue limits, cancellation, replay, concurrent sessions, and health progress for expensive operators; spill support must have an explicit bounded disk policy.
- Advertise backend-specific capabilities and rerun applicable V18–V24/V26. No automatic engine fallback or change to previously shipped semantics.

Large artifact-backed inputs, file/JDBC connectors, caching/checkpointing, and crash recovery are assigned to SS-17/18. Existing bounded local RDD/Python callback execution belongs to SS-12/22; metadata-only SparkContext compatibility belongs to the client bridge. Distributed RDD/SparkContext execution, distributed execution, and unrestricted Spark compatibility remain outside this roadmap.
