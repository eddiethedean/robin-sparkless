# SS-03: Early Spark Connect feasibility

> **Status: planned; owner unassigned.** Phase: 0A only; experimental endpoint.
> [Index](README.md) · [Epic contract](EPIC.md#phase-0a-contracts-and-feasibility) · [Decisions](DECISIONS.md) · [Validation](VALIDATION.md)

## Outcome

Resolve protocol and native API uncertainty before broad plan translation. This is not the Phase 2 product release.

## Dependencies and delivery slices

SS-01 selects client/protos; SS-02 supplies minimal scaffolding. Use a bounded prototype of SS-04/SS-10 lifecycle concepts.

Plan numbers are workstream identifiers, not a strict execution order. Only the applicable phase slice blocks a milestone; later extensions must not delay an earlier gate.

## Implementation sequence

1. Generate the actual service from pinned protobuf sources and record imports, compiler versions, provenance, and licenses.

2. Trace the unmodified client's initialization/configuration, analysis, execution, completion, result release, and session release using its default reattachment behavior.

3. Translate only the native operations required for range(10).filter("id > 5").count(); prove the result is 4 from the Rust engine.

4. Return an empty result with schema and introduce a controlled stream disconnect after a response so the client resumes through ReattachExecute.

5. Exercise operation/response/session identity and release handling without patching the client or disabling its default reattachment.

6. Document required RPCs, expression forms (including string filters), error metadata, unsupported plans/types, and engine gaps. Convert the findings into SS-08/09/10 tasks.

## Acceptance criteria

- [ ] The unmodified pinned client executes the witness and decodes an empty typed result.
- [ ] A trace shows reconnect/release semantics and no plan reexecution on reattach.
- [ ] Results and blockers are recorded; the launcher does not expose this experimental endpoint as supported Spark Connect.

## Decisions and implementation touchpoints

Resolve or explicitly defer D01, D02, D07 in the [decision register](DECISIONS.md). Record evidence and immutable revisions in the [validation matrix](VALIDATION.md); writing this plan does not complete a gate.

- [src/lib.rs](https://github.com/eddiethedean/robin-sparkless/blob/main/src/lib.rs)
- [src/session.rs](https://github.com/eddiethedean/robin-sparkless/blob/main/src/session.rs)
- [src/dataframe.rs](https://github.com/eddiethedean/robin-sparkless/blob/main/src/dataframe.rs)
- [docs/LOGICAL_PLAN_FORMAT.md](../../LOGICAL_PLAN_FORMAT.md)

## Scope notes

Small bounded inputs are acceptable for feasibility. They do not establish scalable lazy execution, production replay, or complete protocol coverage.
