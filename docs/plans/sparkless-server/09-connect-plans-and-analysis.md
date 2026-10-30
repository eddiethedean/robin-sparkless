---
search:
  exclude: true
---

> **Archive / historical record:** this page describes a past snapshot or a plan.
> Use the [current support contract](../../SUPPORT_MATRIX.md) for release behavior.

# SS-09: Spark Connect plan translation and analysis

> **Status: planned; owner unassigned.** Phase: 2.
> [Index](README.md) · [Epic contract](EPIC.md#spark-connect-execution-and-streaming-contract) · [Decisions](DECISIONS.md) · [Validation](VALIDATION.md)

## Outcome

Translate a documented subset of actual Connect relations/expressions into native operations.

The [4.18 implementation plan](RELEASE_4_18.md) refines this work into 418-01–418-03, integrated with Arrow/replay slices 418-04–418-06. Its [Apache source review](CONNECT_PROTOCOL_RESEARCH.md) pins Spark 4.0.5 and distinguishes public protocol behavior from the Sparkless subset. The actual client encoders and config/lifecycle requests are captured in the 418-01 fixtures; the 4.18 candidate conformance gates passed as recorded in the [cut evidence](RELEASE_4_18.md#4180-cut-evidence). Connect SQL commands, writes, grouped aggregates, artifact-backed inputs, and additional nodes remain outside the advertised subset.

## Dependencies and delivery slices

SS-01/03 pinned surface, SS-04 runtime, SS-07 sessions, SS-08 logical schema/lazy execution, SS-10 operation transport contract.

Plan numbers are workstream identifiers, not a strict execution order. Only the applicable phase slice blocks a milestone; later extensions must not delay an earlier gate.

## Implementation sequence

1. Implement explicit protobuf relation/expression dispatch with node/depth/size validation before expensive processing; do not blindly forward JSON execute_plan.

2. Cover range/local relation, projections, string/column filters, supported literals/functions, limits, and initial aggregates observed in SS-03.

3. Preserve column/plan identity, aliases, logical schema, and session semantic settings; reject unresolved/unsupported nodes with actionable protocol errors.

4. Implement required Config and AnalyzePlan operations against real capabilities. Preserve valid optional/default configuration lookups, including batched reads performed before local-relation encoding; never acknowledge an ineffective unsupported setting change or return fabricated analysis. Supporting a SQL relation alone does not implement the public `spark.sql()` command/result path.

5. Bound local-relation decoded size, including expansion from compression. Test when the client switches to artifact-backed relations and refuse that path explicitly until supported.

6. Connect actions to the shared operation registry, schema/Arrow encoder, and cancellation service; avoid implementing a second execution engine in the transport.

7. Create differential tests against the exact reference Spark release/settings and publish the RPC/node/type/config matrix.

## Acceptance criteria

- [ ] The public pinned client succeeds with its normal encoders for each enabled operation.
- [ ] Unsupported plans/config/artifact paths fail clearly without executing unrelated work.
- [ ] Analysis schema matches results for aliases, empty inputs, expressions, aggregates, and enabled null/ANSI semantics.
- [ ] Normal client configuration/rename/schema parsing paths and actual inline-to-cached local-relation boundaries are tested without changing client defaults.
- [ ] No Python/JVM is used in native plan execution.

## Decisions and implementation touchpoints

Resolve or explicitly defer D01, D02, D04, D11 in the [decision register](DECISIONS.md). Record evidence and immutable revisions in the [validation matrix](VALIDATION.md); writing this plan does not complete a gate.

- [src/lib.rs](https://github.com/eddiethedean/robin-sparkless/blob/main/src/lib.rs)
- [crates/robin-sparkless-core/src/expr.rs](https://github.com/eddiethedean/robin-sparkless/blob/main/crates/robin-sparkless-core/src/expr.rs)
- [docs/LOGICAL_PLAN_FORMAT.md](../../LOGICAL_PLAN_FORMAT.md)

## Scope notes

The default Phase 2 translator targets the existing Polars backend. Plan/schema interfaces should permit the later [DuckDB execution backend](15-duckdb-connect-backend.md), but implementing it is Phase 4 work after the [Phase 3 source integration](14-duckdb-data-source.md). Neither DuckDB phase is a Phase 2 release prerequisite.

SS-09 and SS-10 share an interface design and can develop against agreed fixtures; their release gates require both implementations.

Follow-on [SS-16 / Phase 7](16-connect-api-types-and-analysis.md) assigns expanded default-backend operations, scalar/nested types, truthful analysis, and mutable session settings to 4.23. [SS-17 / Phase 8](17-connect-sources-artifacts-and-persistence.md) assigns public I/O/table paths, artifact-backed data, and caches/checkpoints to 4.24. These plans do not expand the frozen 4.18 contract before their implementation and acceptance evidence.
