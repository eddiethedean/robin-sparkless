# SS-12: Optional isolated Python Jobs and notebooks

> **Status: planned; owner unassigned.** Phase: 3; optional and separately packaged.
> [Index](README.md) · [Epic contract](EPIC.md#acceptance-and-release-gates) · [Decisions](DECISIONS.md) · [Validation](VALIDATION.md)

## Outcome

Design out-of-process Python execution without introducing a Python dependency into native SQL/Connect deployments.

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

## Acceptance criteria

- [ ] Core images and builds remain Python-free when the feature is disabled.
- [ ] Unsupported kind=python remains explicit until the full worker capability is enabled.
- [ ] Enabled workers have tested resource isolation, process cleanup, and truthful run/mutation outcomes.

## Decisions and implementation touchpoints

Resolve or explicitly defer D10, D12 in the [decision register](DECISIONS.md). Record evidence and immutable revisions in the [validation matrix](VALIDATION.md); writing this plan does not complete a gate.

- [docs/EXECUTION_CANCELLATION.md](../../EXECUTION_CANCELLATION.md)

## Scope notes

No Python-worker implementation is required for Phase 0/1/2 SQL or DataFrame gates. Separate interpreters alone do not constitute a security sandbox.
