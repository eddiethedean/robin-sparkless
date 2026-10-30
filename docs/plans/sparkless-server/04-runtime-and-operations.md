---
search:
  exclude: true
---

> **Archive / historical record:** this page describes a past snapshot or a plan.
> Use the [current support contract](../../SUPPORT_MATRIX.md) for release behavior.

# SS-04: Admission, operation lifecycle, and shutdown

> **Status: planned; owner unassigned.** Phase: Minimum production controls in 0B; replay/mutation extensions in 1–2.
> [Index](README.md) · [Epic contract](EPIC.md#runtime-limits-deadlines-and-execution-ownership) · [Decisions](DECISIONS.md) · [Validation](VALIDATION.md)

## Outcome

Own compute admission and actual native work independently of the lifetime of an HTTP response or gRPC stream.

For Phase 1, follow [417-03/04](RELEASE_4_17.md): bounded writer waiting, catalog/snapshot/staging accounting, and retained mutation outcomes have distinct ownership from compute and response buffers. Session close or a response deadline cannot discard a commit receipt or release a still-running producer's lease.

## Dependencies and delivery slices

SS-01/02 define contracts/configuration; SS-03 informs later operation protocol needs.

Plan numbers are workstream identifiers, not a strict execution order. Only the applicable phase slice blocks a milestone; later extensions must not delay an earlier gate.

## Implementation sequence

1. Create bounded admission for active work, waiting requests, sessions, input/decoded bytes, streams, and retained output. Validate limits at startup and reserve capacity before expensive decoding/planning.

2. Move CPU-bound planning, schema inference, execution, and encoding to a bounded compute scheduler. Account for Polars' internal workers; keep health and network servicing responsive.

3. Implement queued/running/cancellation-requested/succeeded/failed/cancelled states with transport attachment/deadline status separate. Define transitions before adding protocol adapters.

4. Hold compute permits until work and producers actually stop. Cancellation during noninterruptible work must not release capacity or falsely report native termination.

5. On shutdown stop admission, signal cooperative cancellation, drain to a deadline, and report remaining work. Coordinate forced owned-process termination with SS-06.

6. Instrument queue wait, active work, delayed cancellation, retained bytes, failures, and drain state. Document enforced bounds versus cooperative budgets.

## Acceptance criteria

- [ ] Saturation rejects promptly without an unbounded semaphore wait queue; health requests progress during compute load.
- [ ] A timed-out native task retains its permit until completion; queued work can be cancelled before starting.
- [ ] Shutdown and disconnect tests show no false rollback/termination claims or unaccounted producers.
- [ ] No claim of hard per-query CPU/RSS isolation is made for in-process execution.

## Decisions and implementation touchpoints

Resolve or explicitly defer D07, D08 in the [decision register](DECISIONS.md). Record evidence and immutable revisions in the [validation matrix](VALIDATION.md); writing this plan does not complete a gate.

- [crates/robin-sparkless-core/src/engine/cancellation.rs](https://github.com/eddiethedean/robin-sparkless/blob/main/crates/robin-sparkless-core/src/engine/cancellation.rs)
- [crates/robin-sparkless-core/src/engine/dataframe.rs](https://github.com/eddiethedean/robin-sparkless/blob/main/crates/robin-sparkless-core/src/engine/dataframe.rs)
- [docs/EXECUTION_CANCELLATION.md](../../EXECUTION_CANCELLATION.md)

## Scope notes

Native worker processes are a separate design decision if hard isolation becomes a requirement; they are not implicitly part of the initial in-process service.
