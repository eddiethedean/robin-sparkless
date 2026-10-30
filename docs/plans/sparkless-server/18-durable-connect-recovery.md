---
search:
  exclude: true
---

> **Archive / historical record:** this page describes a past snapshot or a plan.
> Use the [current support contract](../../SUPPORT_MATRIX.md) for release behavior.

# SS-18: Durable Connect sessions, operation outcomes, and replay

> **Status: planned; owner unassigned.** Phase: 9; target 4.25.0; optional persistent recovery mode.
> [Index](README.md) · [Roadmap](ROADMAP.md) · [Decisions](DECISIONS.md) · [Validation](VALIDATION.md)

## Outcome and scope

Add explicitly configured crash/restart recovery for eligible sessions, operation outcomes, and retained result streams. Define `allow_reconnect=true`, client/server session identity continuity, generation fencing, durable response cursors, and restart discovery through the pinned unmodified client. Keep the existing ephemeral mode and its truthful stale-handle errors.

Persistent tables alone do not establish durable execution. Bind each durable receipt and response to a canonical backend/database/session/semantic identity. Specify which reads, writes, artifacts, configurations, and session-local objects can be restored; reject nonrecoverable state explicitly. Never recreate a lost operation under the same handle and silently rerun it.

## Dependencies and ordered delivery

Reuse SS-04/07/10 operation coordination and persistent storage/transactions from the enabled backend, plus SS-17 artifact/checkpoint lifetimes where referenced. Freeze D18's durable store, commit ledger, identity and compatibility format before implementation. A release can advertise recovery only for backends/sinks that pass the complete matrix; native ephemeral deployments must remain usable without the durable store.

| Slice | Deliverable | Exit evidence |
| --- | --- | --- |
| 425-01 | Freeze durable state, protocol compatibility, ownership, and recovery invariants | Client reconnect traces; backend-specific atomic receipt/data boundary; crash-unknown policy |
| 425-02 | Implement transactional outcome ledger and producer fencing | Recovery at every data/receipt/acknowledgement crash boundary without duplicate effects |
| 425-03 | Implement durable response spool/cursors and eligible session restoration | Client reattaches after process restart; replay never reruns the producer |
| 425-04 | Implement expiry, quotas, schema/version migration, and recovery cleanup | Bounded disk retention, corrupt/missing state failures, stale owner exclusion, rollback/upgrade policy |
| 425-05 | Complete V44/V45 and persistent/ephemeral packaged installs | Coordinated 4.25.0 publication with exact per-backend recovery guarantees |

## Acceptance

- **V44:** inject process crashes before/after data commit, durable receipt commit, and acknowledgement for every enabled write path. Prove atomic effects/receipt ownership or report unresolved outcomes and prohibit automatic retry. Cross-process fencing and repeated/conflicting IDs cannot create a second committed effect. Exactly-once claims require evidence for the entire named sink boundary; external JDBC/filesystem effects are not assumed atomic with the local ledger.
- **V45:** recover pinned-client sessions and replay after zero/partial/all delivered batches and prefix release. Verify `allow_reconnect=true`, release tombstones, expiry, permissions, generation fencing, store corruption/unavailability, disk exhaustion, retention/garbage collection, and compatible/incompatible upgrades. Lost or expired results fail explicitly rather than reexecute. Recovery of mutable-source reads must use retained results or a proved durable snapshot.
- Measure recovery time and memory/disk budgets and rerun prior cancellation, shutdown, isolation, and artifact lifecycle regressions. Document unsupported backends and temporary/session state.

This is single-node restart recovery, not distributed consensus, transparent failover, or a promise to preserve arbitrary interpreter/worker state. Streaming progress and sink guarantees are separately tested in SS-19.
