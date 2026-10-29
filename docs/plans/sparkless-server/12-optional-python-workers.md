# SS-12: Optional isolated Python Jobs, notebooks, and UDFs

> **Status: planned; owner unassigned.** Phase: 5; target 4.21.0; optional and separately packaged.
> [Index](README.md) · [Epic contract](EPIC.md#acceptance-and-release-gates) · [Decisions](DECISIONS.md) · [Validation](VALIDATION.md)

## Outcome

Design out-of-process Python execution without introducing a Python dependency into native SQL/Connect deployments.

Phase 5 now explicitly includes scalar Python UDFs and scalar/grouped/iterator Pandas UDF APIs through Connect. Freeze the exact forms and type matrix under D12; workers support only enabled input/output types, with later SS-16 type expansion gated independently. Jobs/notebook execution and UDF execution require separate positive/negative evidence before either is advertised.

## Dependencies and delivery slices

Stable SS-04 operations, SS-06 ownership/control, SS-07 catalogs/identities, and SS-11 core release evidence.

Plan numbers are workstream identifiers, not a strict execution order. Only the applicable phase slice blocks a milestone; later extensions must not delay an earlier gate.

## Implementation sequence

1. Specify supported Jobs/notebook operations, run/session identity, code trust, environment delivery, parameters, secrets policy, and explicit unsupported behavior.

2. Choose a separate worker package/image and versioned RPC contract. Python interpreters live only in worker processes; no Python callbacks run on Tokio threads.

3. Define OS/process isolation, CPU/memory/time quotas, permitted filesystem/network access, and credential scope before accepting arbitrary code.

4. Implement bounded stdout/stderr/result/error transport with redaction, lifecycle supervision, cancellation, and forced process-tree cleanup.

5. Specify how workers access native sessions/catalogs and how commit outcomes survive worker crashes or lost responses.

6. Test real emulator Job/notebook routes, failures, secret redaction, parallel isolation, and disabled-feature behavior.

7. Implement public-client UDF registration/serialization and declared Pandas/Arrow execution forms through a versioned worker RPC. Specify function/environment distribution, determinism, null/decimal/time semantics, row/group/batch boundaries, type mismatch errors, and bounded code/result transport. Do not depend on general AddArtifacts support scheduled for Phase 8; freeze and bound the selected client's required UDF payload path explicitly.

8. Test UDF results and schemas against the pinned Spark reference, worker crash/retry/cancel behavior, oversized groups/results, session isolation, and cleanup. Retry must not imply exactly-once external side effects from arbitrary user code. No Python imports or callbacks execute inside the native server.

## Acceptance criteria

- [ ] Core images and builds remain Python-free when the feature is disabled.
- [ ] Unsupported kind=python remains explicit until the full worker capability is enabled.
- [ ] Enabled workers have tested resource isolation, process cleanup, and truthful run/mutation outcomes.
- [ ] V27 separately certifies the declared scalar/Pandas UDF API and type matrix, bounded Arrow/group execution, errors/cancellation, and disabled-feature failures through the unmodified client.

## Decisions and implementation touchpoints

Resolve or explicitly defer D10, D12 in the [decision register](DECISIONS.md). Record evidence and immutable revisions in the [validation matrix](VALIDATION.md); writing this plan does not complete a gate.

- [docs/EXECUTION_CANCELLATION.md](../../EXECUTION_CANCELLATION.md)

## Scope notes

No Python-worker implementation is required for Phase 0–4 SQL/DataFrame, DuckDB source, or DuckDB Connect gates. This phase is scheduled after the DuckDB phases without an inherent engine dependency. Separate interpreters alone do not constitute a security sandbox.
