# SS-09: Spark Connect plan translation and analysis

> **Status: planned; owner unassigned.** Phase: 2.
> [Index](README.md) · [Epic contract](EPIC.md#spark-connect-execution-and-streaming-contract) · [Decisions](DECISIONS.md) · [Validation](VALIDATION.md)

## Outcome

Translate a documented subset of actual Connect relations/expressions into native operations.

## Dependencies and delivery slices

SS-01/03 pinned surface, SS-04 runtime, SS-07 sessions, SS-08 logical schema/lazy execution, SS-10 operation transport contract.

Plan numbers are workstream identifiers, not a strict execution order. Only the applicable phase slice blocks a milestone; later extensions must not delay an earlier gate.

## Implementation sequence

1. Implement explicit protobuf relation/expression dispatch with node/depth/size validation before expensive processing; do not blindly forward JSON execute_plan.

2. Cover range/local relation, projections, string/column filters, supported literals/functions, limits, and initial aggregates observed in SS-03.

3. Preserve column/plan identity, aliases, logical schema, and session semantic settings; reject unresolved/unsupported nodes with actionable protocol errors.

4. Implement required Config and AnalyzePlan operations against real capabilities. Never acknowledge unsupported config or return fabricated analysis.

5. Bound local-relation decoded size, including expansion from compression. Test when the client switches to artifact-backed relations and refuse that path explicitly until supported.

6. Connect actions to the shared operation registry, schema/Arrow encoder, and cancellation service; avoid implementing a second execution engine in the transport.

7. Create differential tests against the exact reference Spark release/settings and publish the RPC/node/type/config matrix.

## Acceptance criteria

- [ ] The public pinned client succeeds with its normal encoders for each enabled operation.
- [ ] Unsupported plans/config/artifact paths fail clearly without executing unrelated work.
- [ ] Analysis schema matches results for aliases, empty inputs, expressions, aggregates, and enabled null/ANSI semantics.
- [ ] No Python/JVM is used in native plan execution.

## Decisions and implementation touchpoints

Resolve or explicitly defer D01, D02, D04, D11 in the [decision register](DECISIONS.md). Record evidence and immutable revisions in the [validation matrix](VALIDATION.md); writing this plan does not complete a gate.

- [src/lib.rs](https://github.com/eddiethedean/robin-sparkless/blob/main/src/lib.rs)
- [crates/robin-sparkless-core/src/expr.rs](https://github.com/eddiethedean/robin-sparkless/blob/main/crates/robin-sparkless-core/src/expr.rs)
- [docs/LOGICAL_PLAN_FORMAT.md](../../LOGICAL_PLAN_FORMAT.md)

## Scope notes

SS-09 and SS-10 share an interface design and can develop against agreed fixtures; their release gates require both implementations.

[SS-14](14-minilake-interoperability.md) adds a separately certified MiniLake Jobs client route after standalone conformance. Its Python runner does not require SS-12. Named-table/file-source reads and write commands for shared Delta remain explicitly enabled SS-13/14 capabilities; neither the initial range/filter/count witness nor standard Connect support establishes Delta Connect extension compatibility.
