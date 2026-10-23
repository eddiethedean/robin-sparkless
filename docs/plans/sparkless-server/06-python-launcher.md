# SS-06: Python launcher and binary packaging

> **Status: source implemented; lifecycle/platform evidence pending.** Phase: 0B; supported Spark Connect URL in 2.
> [Index](README.md) · [Epic contract](EPIC.md#python-lifecycle-api) · [Decisions](DECISIONS.md) · [Validation](VALIDATION.md)

## Outcome

Add optional sync/async lifecycle support in the existing sparkless package for an owned native executable.

## Dependencies and delivery slices

SS-02 owned control/packaging and SS-04/05 readiness/drain contracts; SS-10 release gate before enabling gRPC URL.

Plan numbers are workstream identifiers, not a strict execution order. Only the applicable phase slice blocks a milestone; later extensions must not delay an earlier gate.

## Implementation sequence

1. Specify the launcher/server control handshake separately from package/protocol versions. Include per-launch identity, capabilities, actual bound addresses, and bounded readiness data.

2. Implement package-resource binary discovery and explicit executable override; never download, invoke Cargo, or trust arbitrary PATH fallback during import/start.

3. Expose start/stop and async_start/async_stop, context managers, pid, is_running, supported endpoint properties, and bounded sanitized logs. Audit parent-package import behavior.

4. Serialize lifecycle calls. Repeated start while running and repeated stop are idempotent; same-object nested context entry is rejected. Restart creates a fresh child and handshake.

5. Let Rust allocate ephemeral ports. Detect early exit, startup timeout, collisions, and binary mismatch; continuously drain logs and reap failed children.

6. Use cross-platform graceful control, bounded drain/wait, then forced termination and reap. Async task cancellation must finish cleanup without blocking the event loop.

7. Package the opt-in precompiled server binary extra; test launcher Python >=3.8 independently from the pinned Connect client's Python range.

## Acceptance criteria

- [ ] Sync and async normal/exception exits, cancelled startup/exit, early crash, timeout, noisy logs, and multiple instances leave no owned child behind.
- [ ] Unsupported/not-running endpoint access gives typed descriptive errors; gRPC remains unavailable before Phase 2.
- [ ] An inherited launcher object cannot terminate its parent's child; ownership and Windows/POSIX behavior are documented.
- [ ] No import-time server startup, PySpark import, build, or download occurs.

## Decisions and implementation touchpoints

Resolve or explicitly defer D05, D06 in the [decision register](DECISIONS.md). Record evidence and immutable revisions in the [validation matrix](VALIDATION.md); writing this plan does not complete a gate.

- [python/pyproject.toml](https://github.com/eddiethedean/robin-sparkless/blob/main/python/pyproject.toml)
- [python/sparkless/sparkless/__init__.py](https://github.com/eddiethedean/robin-sparkless/blob/main/python/sparkless/sparkless/__init__.py)

## Scope notes

Async subprocess lifecycle does not make the synchronous PySpark DataFrame API asynchronous. GC/atexit are fallback cleanup only. The current binary wheel workflow produces platform wheels on its configured runners; launcher lifecycle acceptance remains unverified.
