---
search:
  exclude: true
---

> **Archive / historical record:** this page describes a past snapshot or a plan.
> Use the [current support contract](../../SUPPORT_MATRIX.md) for release behavior.

# SS-05: Read-only HTTP SQL and typed results

> **Status: planned; owner unassigned.** Phase: 0B.
> [Index](README.md) · [Epic contract](EPIC.md#http-statement-agent-contract) · [Decisions](DECISIONS.md) · [Validation](VALIDATION.md)

## Outcome

Expose actual read-only native SQL through the existing agent envelope with exact positional typed results.

The [4.16 implementation plan](RELEASE_4_16.md#416-05-explicit-read-only-sql-and-result-contract) defines the first release's explicit SQL allowlist, scalar encodings, and golden corpus. Its schema/positional export work is required before HTTP release; the current name-keyed JSON APIs are not sufficient for duplicate labels.

The [4.17 extension](RELEASE_4_17.md) adds only its enumerated managed-table reads/mutations and session commands after SS-07 coordination and HTTP identity/status gates pass. Legacy mutation requests remain rejected. Keep the 4.16 exact scalar/result and bounded-response behavior as regression requirements.

## Complete local SQL expansion — Phase 7 / SS-22

The initial 4.16/4.17 allowlists remain historical release boundaries. [SS-22](22-local-sql-feature-parity.md) now requires all existing locally implemented `SparkSession.sql` statements, expressions/functions and result types on HTTP as well as Connect. Complete native query/DDL/DML coverage in Phase 7 / SS-16 (including UPDATE/DELETE, CTAS/replace, SHOW/DESCRIBE and supported CTE/subquery/window/generator combinations); worker/storage/connector SQL integrates in its owning Phase 5/6/8. Reuse shared parser/engine capabilities instead of a permanently narrower HTTP function list. Preserve identity/snapshots/admission, version the result schema where needed for exact nested/other local types, and retain configured I/O/credential policy. D21 and V56/V58 require local/HTTP/Connect regression coverage before Phase 11 certification; no current-support expansion is claimed by this plan.

## Dependencies and delivery slices

SS-01 standalone typed-result contracts; SS-02 transport; SS-04 runtime; SS-07 minimum sessions; SS-08 minimum scalar/schema repairs.

Plan numbers are workstream identifiers, not a strict execution order. Only the applicable phase slice blocks a milestone; later extensions must not delay an earlier gate.

## Implementation sequence

1. Implement health, capabilities, statements, and idempotent close routes. Share session/operation services; do not create one unmanaged engine per request.

2. Validate bounded JSON input, identifiers, kind, SQL AST, and the explicit read-only allowlist before calling engine code. Reject writes, Python, unsupported types/functions, and client-directed file/network access.

3. Execute using native SQL and cancellation APIs. Do not use Python/PyO3 as a service implementation.

4. Encode Spark-compatible logical schemas with positional rows, nulls, exact integers and decimal strings, declared date/timestamp formats, and empty schemas/results. Never use name-keyed rows to lose duplicate labels.

5. Enforce rows, bytes, single-value size, and deadlines during production. Buffer only within declared HTTP response limits and fail before sending a successful envelope if limits are exceeded.

6. Use status:error envelopes with HTTP 200 for valid requests that fail execution/unsupported-kind checks. Document transport/oversize/admission HTTP failures separately.

7. Run native/direct HTTP tests and fresh packaged consumers; validate exact values and schemas. Optional external route certification is supplemental.

## Acceptance criteria

- [ ] SELECT 1, large BIGINT, decimal, typed null, date/time, and empty result fixtures return exact schemas and values.
- [ ] Invalid input, unsupported operations, output-limit and queue-limit errors are explicit; successful truncation is impossible.
- [ ] Remote SQL remains read-only in Phase 0B; capabilities match enabled paths.
- [ ] Direct HTTP and packaged consumers verify exact positional values and logical schemas.

## Decisions and implementation touchpoints

Resolve or explicitly defer D02, D03, D04, D08 in the [decision register](DECISIONS.md). Record evidence and immutable revisions in the [validation matrix](VALIDATION.md); writing this plan does not complete a gate.

- [src/session.rs](https://github.com/eddiethedean/robin-sparkless/blob/main/src/session.rs)
- [src/lib.rs](https://github.com/eddiethedean/robin-sparkless/blob/main/src/lib.rs)
- [crates/robin-sparkless-polars/src/sql/mod.rs](https://github.com/eddiethedean/robin-sparkless/blob/main/crates/robin-sparkless-polars/src/sql/mod.rs)

## Scope notes

DATABRICKS_SPARK_CONNECT_URL is the HTTP agent address. It must not be described as a Spark Connect gRPC endpoint.
