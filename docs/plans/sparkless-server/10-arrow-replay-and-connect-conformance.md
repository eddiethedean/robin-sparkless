# SS-10: Arrow streaming, replay, and Connect conformance

> **Status: planned; owner unassigned.** Phase: 2, informed by 0A feasibility.
> [Index](README.md) · [Epic contract](EPIC.md#spark-connect-execution-and-streaming-contract) · [Decisions](DECISIONS.md) · [Validation](VALIDATION.md)

## Outcome

Implement the actual execution stream lifecycle, including disconnection, retained responses, release, and cancellation.

## Dependencies and delivery slices

SS-03 protocol traces, SS-04 operation accounting, SS-07 session lifecycle, SS-08 schema/IPC, SS-09 native plans.

Plan numbers are workstream identifiers, not a strict execution order. Only the applicable phase slice blocks a milestone; later extensions must not delay an earlier gate.

## Implementation sequence

1. Define stable session generations, operation IDs, ordered response IDs, offsets, and completion markers. Validate client-observed session identity.

2. Emit client-compatible Arrow batches and schema-bearing empty results with row/byte limits; reject oversize cells rather than silently truncating.

3. Retain bounded unacknowledged responses for reattachment. Resume after the acknowledged response without rerunning the plan or duplicating mutation effects.

4. Implement release-prefix/all, session release, interrupt, error-detail behavior, and duplicate execute identity checks with idempotent cleanup.

5. Choose replay memory/spool budgets and retention policy. Bound detached operations/errors too; backpressure or explicit expiry/failure must replace silent eviction.

6. Keep compute permits until native producers finish. Detach on a reattachable stream disconnect; do not apply HTTP read-only cancellation rules blindly.

7. Run the public client lifecycle, multi-batch, empty/type, reconnect, saturation, concurrency, and restart suite before exposing the launcher's Connect URL.

## Acceptance criteria

- [ ] Reattach resumes after one or more delivered batches with correct offsets and no reexecution.
- [ ] Duplicate execute, repeated release, release/reattach races, detached interruption, expired operations, and changed sessions have tested outcomes.
- [ ] Replay capacity exhaustion produces explicit bounded behavior and resource accounting returns to baseline.
- [ ] The JVM-free public client witness and complete supported-surface suite pass before Phase 2 is declared ready.

## Decisions and implementation touchpoints

Resolve or explicitly defer D07, D08, D11 in the [decision register](DECISIONS.md). Record evidence and immutable revisions in the [validation matrix](VALIDATION.md); writing this plan does not complete a gate.

- [src/dataframe.rs](https://github.com/eddiethedean/robin-sparkless/blob/main/src/dataframe.rs)
- [crates/robin-sparkless-polars/src/dataframe/mod.rs](https://github.com/eddiethedean/robin-sparkless/blob/main/crates/robin-sparkless-polars/src/dataframe/mod.rs)

## Scope notes

Keep result/operation ownership explicit so the later DuckDB source and execution phases can reuse this protocol without silently inheriting Polars' ephemeral-catalog semantics. [SS-14](14-duckdb-data-source.md) must preserve source snapshots during scans/replay; [SS-15](15-duckdb-connect-backend.md) must prove database transaction outcomes and persistent-data restart behavior separately. Phase 2 conformance remains scoped to its implemented backend.

Transport end-of-stream is not necessarily query completion. Client defaults must remain enabled throughout conformance tests.
