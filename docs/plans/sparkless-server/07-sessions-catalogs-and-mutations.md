# SS-07: Session ownership, shared catalogs, and mutations

> **Status: planned; owner unassigned.** Phase: Bounded read-only sessions in 0B; catalogs/writes in 1.
> [Index](README.md) · [Epic contract](EPIC.md#sessions-catalogs-and-mutations) · [Decisions](DECISIONS.md) · [Validation](VALIDATION.md)

## Outcome

Separate logical session state from shared saved objects and make commit/cleanup/retry semantics explicit.

## Dependencies and delivery slices

SS-04 operation ownership; SS-01 identity/adapter contracts. Complete mutation work before enabling any remote DDL/DML.

Plan numbers are workstream identifiers, not a strict execution order. Only the applicable phase slice blocks a milestone; later extensions must not delay an earlier gate.

## Implementation sequence

1. First implement bounded creation, idle TTL, close, and active-operation references for read-only sessions. Define reserved sql-warehouse lifetime and cleanup behavior.

2. Record the HTTP trust scope: session/kind/code contains no caller identity and warehouses share sql-warehouse. Do not invent per-user isolation from session IDs.

3. Introduce a catalog service owning saved tables/database metadata; keep temp views/configuration session-local. Avoid exposing the current process-global temp-view static.

4. Add native integration hooks for reads and coordinated mutations. Specify reader snapshot/visibility rules and serialize initial catalog mutations across sessions.

5. Choose ephemeral versus durable catalog storage. Shared objects survive session close; restart persistence requires metadata/storage recovery tests before advertising it.

6. Add stable operation/idempotency identity to mutation-capable HTTP integration. Reject same-ID/different-request submissions; do not retry ambiguous writes without deduplication.

7. Preserve commit outcomes across disconnect/result serialization failures. Document the restart limits of in-memory deduplication and avoid durable exactly-once claims.

## Acceptance criteria

- [ ] Phase 0B TTL/close tests cannot destroy resources held by active work or leak state between distinct declared sessions.
- [ ] Phase 1 tables are shared according to the catalog scope; temp views remain isolated; global views are rejected until scoped.
- [ ] Concurrent writes, duplicate submissions, close/disconnect near commit, and restart boundaries have deterministic tested outcomes.
- [ ] Read-only Phase 0 remains blocked from mutation paths until these gates pass.

## Decisions and implementation touchpoints

Resolve or explicitly defer D03, D09, D10 in the [decision register](DECISIONS.md). Record evidence and immutable revisions in the [validation matrix](VALIDATION.md); writing this plan does not complete a gate.

- [src/session.rs](https://github.com/eddiethedean/robin-sparkless/blob/main/src/session.rs)
- [crates/robin-sparkless-polars/src/session/mod.rs](https://github.com/eddiethedean/robin-sparkless/blob/main/crates/robin-sparkless-polars/src/session/mod.rs)
- [crates/robin-sparkless-core/src/engine/external_catalog.rs](https://github.com/eddiethedean/robin-sparkless/blob/main/crates/robin-sparkless-core/src/engine/external_catalog.rs)
- [docs/EXTERNAL_CATALOGS.md](../../EXTERNAL_CATALOGS.md)

## Scope notes

A read resolver is not a shared mutation coordinator. Client-supplied UserContext/session IDs are not authentication.
