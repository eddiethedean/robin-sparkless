---
search:
  exclude: true
---

> **Archive / historical record:** this page describes a past snapshot or a plan.
> Use the [current support contract](../../SUPPORT_MATRIX.md) for release behavior.

# SS-06: Python launcher and binary packaging

> **Status: planned; owner unassigned.** Phase: 0B; supported Spark Connect URL in 2; optional `SparklessConnect` in 3 / 4.19.
> [Index](README.md) · [Epic contract](EPIC.md#python-lifecycle-api) · [Decisions](DECISIONS.md) · [Validation](VALIDATION.md)

## Outcome

Add optional sync/async lifecycle support in the existing sparkless package for an owned native executable.

The [4.18 implementation plan](RELEASE_4_18.md) assigns supported endpoint work to 418-07 after standalone conformance: independent HTTP/gRPC binds, actual ephemeral endpoint readiness, negotiated control/capability validation, installed-platform lifecycle checks, and V24. Update the current validator that rejects Connect advertisement; disabled/experimental builds continue to reject `spark_connect_url`. Starting the native child must not import PySpark.

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

Async subprocess lifecycle does not make the synchronous PySpark DataFrame API asynchronous. GC/atexit are fallback cleanup only.

## 4.19.0 — Optional SparklessConnect context manager

Add `from sparkless.server import SparklessConnect` so existing pyspark-client code can obtain a real PySpark Connect `SparkSession` backed by an owned `SparklessServer`:

```python
from sparkless.server import SparklessConnect

with SparklessConnect() as spark:
    assert spark.range(10).filter("id > 5").count() == 4
```

`__enter__` starts `SparklessServer`, waits for its supported Connect endpoint, creates a session using pyspark-client, and returns that actual session. It must not return a Sparkless session, proxy, or wrapper. `__exit__` calls `spark.stop()` before stopping/reaping the server, on both normal and exceptional exits. This supplies an explicit session cleanup context manager without requiring the selected released pyspark-client package to provide one.

The refined [4.19 helper contract](RELEASE_4_19.md#sparklessconnect-helper-contract) proposes `SparklessConnect(*, server_options=None)`, copied options forwarded to the existing launcher (including its actual `executable_path` keyword), lazy `pyspark-client==4.0.5` loading, and a fresh `.create()` session. It defines pre-launch validation, idle/entering/active/exiting states, sequential reuse after successful cleanup, nested/fork behavior, exception precedence, and clean base/server/connect install checks. Use the existing package Python minimum and supported server platform set; additional compatibility requires evidence.

### Delivery slices

- **419-06 — API and optional packaging:** implement the proposed keyword-only server-options API, expose the public import, and add `sparkless[connect]` with `pyspark-client==4.0.5` and the exactly matching engine-version server binary dependency. Test its Python/platform requirements against the supported Connect contract. Keep both dependencies optional; default installs and `sparkless[server]` must not require pyspark-client. Import PySpark lazily on context entry before starting the child and report a missing dependency with an actionable install command.
- **419-07 — Owned lifecycle:** use an independently owned Connect session rather than reusing/stopping an unrelated global session; choose the client construction API accordingly. Clean up the server if session construction fails. Always attempt server shutdown even if `spark.stop()` fails; preserve a body/startup exception and expose cleanup failures without masking it. Specify repeated use and reject same-instance nested entry; separate instances must be safe to nest. Manual session stop inside the block must still allow safe exit. Async support is separate future scope.
- **419-08 — Acceptance and documentation:** test real packaged pyspark-client sessions against SparklessServer, including `range`, a transformation, and an action; verify the returned object's PySpark Connect type. Cover normal exit, body exception, missing dependency, server startup failure, session creation failure, stop failure, manual stop, repeated use, and nested independent contexts. Assert no owned process/session remains and unrelated sessions remain usable. Document installation, the example above, ownership, cleanup errors, and supported client/Python versions. Verify fresh installs with and without the extra and no import-time PySpark import or server startup.

These slices join the 4.19 release acceptance alongside SS-14 and performance slices 419-09–419-11; they do not require DuckDB and do not extend the earlier 4.18 release gate. Component versions and publication follow the current release procedure.
