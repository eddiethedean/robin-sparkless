# SS-16: Expanded Connect DataFrame API, types, and analysis

> **Status: planned; owner unassigned.** Phase: 7; target 4.23.0.
> [Index](README.md) · [Roadmap](ROADMAP.md) · [Decisions](DECISIONS.md) · [Validation](VALIDATION.md)

## Outcome and scope

Expand the default Polars Connect backend beyond the frozen 4.18 subset. Cover general comparisons and compound boolean/null predicates, arithmetic and checked casts, selected string/date functions, `selectExpr`, wildcard projections, column add/rename/drop, joins, grouped/distinct aggregates, sorting, windows, and set operations (`union`, `unionByName`, intersect, except, and their distinct/all forms). Freeze exact supported functions and window frames before implementation; this is a declared compatibility matrix, not universal Spark parity.

Enable Connect input, analysis, execution, and Arrow output for BYTE, FLOAT, BINARY, TIMESTAMP_NTZ, arrays, maps, and nested structs. Preserve decimal precision, timestamp semantics, nested metadata, nullability, duplicate labels, and typed empty results. Do not infer Connect support from an embedded engine mapping alone.

Add truthful public `explain()`/tree representations and the analysis requests needed by the expanded API. Document native plan output as native output; do not fabricate Catalyst plans or statistics. Add explicitly enumerated mutable runtime settings with session isolation and analysis/execution consistency. Arbitrary Spark configurations remain unsupported unless individually enabled.

## Dependencies and ordered delivery

Reuse SS-08–10 schema, execution, resource, and lifecycle contracts. Phase 4's DuckDB matrix is an input, not evidence that Polars supports the same nodes. Python workers and Delta/UC are optional and are not required to use this phase's native DataFrame API.

| Slice | Deliverable | Exit evidence |
| --- | --- | --- |
| 423-01 | Freeze D16 API/type/config/analysis matrix and client/reference pins | Actual unmodified-client request fixtures, backend capability differences, exact function/window/set semantics |
| 423-02 | Extend scalar and nested input/result types | Analysis/Arrow agreement, boundary/null/empty cases, bounded nested/dictionary/decoded expansion |
| 423-03 | Implement expressions and relational operations | Reference values and schemas for compositions, joins, grouping, ordering, windows, and set multiplicity |
| 423-04 | Implement expanded analysis and mutable settings | Honest native plans, scoped configuration changes, invalidation rules, unsupported-setting failures |
| 423-05 | Complete V39/V40, default/optional backend regressions and distribution | Exact candidate/public artifact checks at coordinated 4.23.0 |

## Acceptance

- **V39:** run the entire enabled type/expression/relation matrix against the pinned Spark reference through the unmodified Connect client. Cover case/ANSI/null behavior, decimal/overflow, NaN policy, nested transformations, ordering ties, window frames, set duplicates, and empty input. Reject unsupported descendants before effects.
- **V40:** schema and public analysis agree with execution; supported configuration changes affect only their session and cannot race an operation's semantic snapshot. Explain output is identified accurately. Measure memory/queue limits, cancellation, replay, concurrent sessions, and health progress for expensive operators; spill support must have an explicit bounded disk policy.
- Advertise backend-specific capabilities and rerun applicable V18–V24/V26. No automatic engine fallback or change to previously shipped semantics.

Large artifact-backed inputs, file/JDBC connectors, caching/checkpointing, and crash recovery are assigned to SS-17/18. RDD/SparkContext, distributed execution, and unrestricted Spark compatibility remain outside this roadmap.
