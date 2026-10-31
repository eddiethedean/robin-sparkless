---
search:
  exclude: true
---

> **Archive / historical record:** this page describes a past snapshot or a plan.
> Use the [current support contract](../../SUPPORT_MATRIX.md) for release behavior.

# Rust-native sparkless-server epic

> **Status: planned.** Imported from [issue #1726](https://github.com/eddiethedean/robin-sparkless/issues/1726) on 2026-10-23; source issue updated 2026-10-23T13:23:06Z. These are requirements, not claims of implemented support.
>
> Start with the [plan index](README.md). This file preserves the revised issue body as the baseline; workstream plans add implementation sequencing. Update this baseline and affected workstream/decision/validation documents together when requirements change, and synchronize the GitHub epic deliberately.

## Goal and architecture

Add **`sparkless-server`**, an independently distributable native Rust library/executable in this Cargo workspace, exposing the existing Robin Sparkless Rust/Polars engine through:

1. **HTTP SQL statements** with exact typed results for standalone clients.
2. **Real Spark Connect gRPC** for an unmodified JVM-free `pyspark-client`.

Both transports use the same native execution, session, catalog, and result services. After supported Connect, Phase 3 adds DuckDB table sources feeding Polars, and Phase 4 adds an optional DuckDB execution backend behind Connect. The default backend remains Robin Sparkless/Polars. The initial service is single-node and intended for trusted local development and tests. Sail and real Spark remain independent compatibility/reference backends.

**Non-negotiable:** execute through native server/backend code: Robin's Rust/Polars engine by default, with the later optional DuckDB backend using native database libraries. The service execution path must not import Python, wrap the PyO3 extension, require Java/Py4J, or run an Apache Spark server. Arbitrary `kind=python` statements and notebooks return explicit unsupported errors until separately packaged, isolated Python workers exist. Those workers remain optional and out of process.

This epic covers the complete direction. Each phase has its own supported surface and release gate; a passing smoke query does not establish general Spark or Databricks compatibility.

The 2026-10-29 scope extension schedules remaining Connect API gaps in Phases 7–10 and adds scalar/Pandas UDFs to Phase 5. Optional UDF workers and JDBC connector processes remain separately packaged; the core native process/image never embeds Python or a JVM. RDD/SparkContext and distributed Spark execution remain outside this single-node roadmap.

## Delivery order and scope

| Stage | Deliverable | Required dependency / gate |
| --- | --- | --- |
| **Phase 0A** | Versioned contracts, engine gap inventory, typed HTTP contracts, and a narrow native Spark Connect feasibility test | Verify typed HTTP result decoding and the real client's execution lifecycle before expanding transport implementation |
| **Phase 0B** | Read-only HTTP SQL service, minimum bounded runtime/session management, sync/async Python launcher, native image | Exact values and schemas through direct HTTP and fresh packaged consumer environments |
| **Phase 1** | Shared catalog ownership, controlled mutations, richer logical types, lazy execution improvements | Visibility, concurrency, mutation outcomes, type fidelity, and resource-accounting tests |
| **Phase 2** | Supported Spark Connect release with Arrow streaming and resumable executions | Pinned unmodified client, lifecycle/conformance suite, with optional external route certification outside the standalone release gate |
| **Phase 3** | Optional DuckDB data source feeding Polars execution | Supported Connect first; V35/V36 typed scans, source snapshots, database ownership, and bounded resources |
| **Phase 4** | Optional DuckDB execution backend through Connect | Phase 3 first; V37/V38 Spark semantics, backend lifecycle, persistent transaction outcomes, and shared-table interoperability |
| **Phase 5** | Optional isolated Python Jobs/notebooks and scalar/Pandas UDF workers | Separate trust, packaging, resource, UDF semantics, and lifecycle contract; V27 |
| **Phase 6** | Optional Delta Lake / Unity Catalog | Verified native transactions and interoperability |
| **Phase 7** | Expanded default Connect DataFrame API, types, analysis, and session configuration | D16; V39/V40 client/reference conformance and resource/lifecycle evidence |
| **Phase 8** | Connect file/optional JDBC I/O, artifacts, caches, and DataFrame checkpoints | D17; V41–V43 source/sink effects, transfer boundaries, storage ownership and quotas |
| **Phase 9** | Optional durable Connect operation/session recovery | D18; V44/V45 atomic receipt/crash matrix and replay without reexecution |
| **Phase 10** | Optional single-node Structured Streaming | D19; V46/V47 event-time/state/sink semantics and checkpoint/recovery/resource evidence |

Phase 0 means **0A and 0B are complete**. Admission control, bounded queues/results, session ownership, cancellation accounting, and shutdown are Phase 0 prerequisites, not later hardening.

Phase 0B accepts an explicitly enumerated **read-only SQL subset**. Mutating SQL, client-selected file/network readers, UDF execution, and arbitrary Python are rejected before execution. Phase 1 enables selected mutations only after their catalog and retry semantics are implemented.

## Workspace, build, and distribution

- Package/executable/CLI: **`sparkless-server`**.
- Location: **`crates/sparkless-server/`**, added to root `[workspace].members`.
- Dependency: `robin-sparkless = { version = "<workspace release>", path = "../..", features = ["sql"] }`, following existing publication conventions.
- Local invocation: `cargo run -p sparkless-server -- serve`; independent `cargo install sparkless-server` distribution when published.
- Follow the root edition/MSRV/toolchain conventions; currently edition 2024, MSRV 1.93, pinned toolchain 1.93.1.
- Keep Tokio/HTTP/gRPC dependencies in the server crate, outside the embedded engine and Python extension dependency paths. Keep HTTP and Spark Connect features separately testable.
- Prefer Tokio, axum/hyper, tonic/prost, Serde, and tracing. Validate and pin compatible dependencies against the actual Polars/Arrow stack.
- Polars uses `polars-arrow`; do not assume its Rust types are interchangeable with `arrow-rs`. Choose and test an IPC/conversion boundary before adding another Arrow implementation.
- Release native binaries and a minimal runtime image containing no Python/JRE. Publish a tested OS/architecture/libc matrix; add Windows and other targets only with passing lifecycle/build tests.
- Verify the binary's runtime dependencies and feature graph. A Rust executable alone is not proof that its image is Python/JVM-free.

Suggested structure:

```text
crates/sparkless-server/
  Cargo.toml
  README.md
  Dockerfile
  src/
    main.rs
    lib.rs
    config.rs
    error.rs
    capabilities.rs
    engine/       # direct Rust calls, planning, logical schema, results
    sessions/     # logical sessions and catalog ownership
    operations/   # execution state, cancellation, replay/release
    protocols/
      http/
      spark_connect/
    runtime/      # admission, scheduling, limits, shutdown, metrics
  proto/          # pinned sources, provenance/licenses, generation instructions
  tests/
```

## Phase 0A: contracts and feasibility

### Pin compatibility before claiming support

- [ ] Choose **one exact PySpark Connect client release** and matching Spark protocol tag/commit. Start evaluation with the Spark 4.0 protocol family reviewed for this epic; record the exact patch, Python requirement, protobuf/gRPC/PyArrow dependencies, and lockfile at the 0A gate.
- [ ] Record the engine compatibility profile separately from wire compatibility. A 4.0 client target must not silently inherit the engine's default 3.5/ANSI-disabled profile.
- [ ] Specify ANSI behavior, case sensitivity, session timezone, timestamp interpretation, decimal rules, and supported configuration keys. Reject unsupported configuration changes rather than acknowledging ineffective settings.
- [ ] Pin the standalone client, protocol, and reference Spark/Sail environments used in CI.
- [ ] Record separate versions for the launcher control protocol, HTTP agent envelope, binary package, Spark Connect protocol, and engine semantics. Do not equate these version numbers or advertise a Spark version as the native engine's product version.
- [ ] Proto generation must be reproducible, with imports, compiler versions, source provenance, and required license notices included.

### Standalone typed-result contract

- [ ] Freeze the native HTTP schema and positional result envelope.
- [ ] Use in-repository decoder fixtures for exact BIGINT/DECIMAL, ordered duplicate labels, typed nulls, and schema-bearing empty results.
- [ ] Verify real HTTP output and packaged consumers without external application changes.
- [ ] Optional external adapters need independent value/schema/lifecycle evidence before being advertised; their development and acceptance cannot block Sparkless.

### Narrow Spark Connect feasibility test

- [ ] Establish the minimum server-crate scaffolding needed for this test, generate the real service from pinned protocol sources, and connect the selected **unmodified client with its default behavior**.
- [ ] Trace initialization/config requests, analysis, execute, result completion/release, session release, and one forced disconnect/reattach.
- [ ] Execute `range(10).filter("id > 5").count()` through the native Rust engine and verify **4**; also return an empty typed result.
- [ ] Exercise operation/session/response identifiers and the client's default reattachable execution path. Do not patch the client or disable its default reattachment to make the test pass.
- [ ] Record the minimum required RPC/plan/type surface, unsupported cases, and measured engine/API gaps as implementation work.
- [ ] Keep this feasibility endpoint experimental and unavailable through the launcher's public `spark_connect_url` until Phase 2 passes.

## HTTP statement-agent contract

The internal request is:

```json
{"session":"sql-warehouse","kind":"sql","code":"SELECT 1 AS result"}
```

The standalone HTTP endpoint is `http://sparkless-server:8099`; its contract is independent of Spark Connect gRPC and external adapter configuration.

### Endpoints and envelopes

- `GET /health`: readiness after listeners and a real native engine startup check succeed. Report draining/unavailable states. Keep liveness separate and cheap; do not execute a SQL query on every health poll or treat ordinary queue saturation as a reason to restart.
- `POST /statements`: bounded, validated statement execution.
- `POST /close`: idempotent cleanup of the named logical session, subject to active-operation ownership rules.
- `GET /capabilities`: versioned actual SQL/type/protocol capabilities and limit semantics. Never advertise an unimplemented endpoint or operation.
- Successful query output uses `status: "ok"` and `data["application/json"] = {"schema": ..., "data": [[...]]}`.
- The schema is an explicitly specified Spark-compatible logical schema. Do not serialize the core Rust schema enum directly and assume it matches Spark's JSON schema.
- Return an empty row array **with schema** for zero-row queries. Cells are positional so duplicate output labels cannot overwrite values.
- For valid statement requests, unsupported SQL/kinds and execution failures use the agent's `status: "error"`, `ename`, and `evalue` envelope with HTTP 200, matching the adapter's execution-error path. Malformed transport input, oversized bodies, and admission rejection use documented HTTP statuses and bounded error bodies.
- Never return successful placeholders, omit unknown `status`, or silently truncate a result into apparent success.

### Phase 0B SQL and type boundary

- Publish an allowlist of supported read-only statements, expressions, and functions. Validate parsed statements before calling any engine path that can execute eager work.
- Initial scalar targets: BOOLEAN, INT/SMALLINT/BIGINT, finite DOUBLE, STRING, DATE, DECIMAL with precision 1–38 and scale 0–precision, and a documented UTC/microsecond TIMESTAMP representation; support typed nulls.
- Reject unsupported physical/logical types explicitly, including FLOAT32/BYTE/NTZ or nested types until their mappings are implemented. Do not silently widen or stringify them through a lossy schema conversion.
- JSON encoding: integers remain exact; decimals are exact base-10 strings with declared precision/scale; dates/timestamps have a specified ISO representation and timezone interpretation; null remains JSON null. Specify or reject non-finite floats.
- Bound input bytes, output rows, individual values, serialized bytes, and queue wait. Check limits during production/serialization. JSON response limits are enforced before sending a successful envelope.
- A response deadline requests cooperative cancellation and ends waiting; it does not certify that native computation has stopped.

## Native engine and type prerequisites

Reuse `SparkSession`, `DataFrame`, `ExprIr`, backend traits, SQL cancellation APIs, and bounded batch facilities where their semantics fit. Extend the native engine when needed.

- Existing JSON `execute_plan` begins with supplied data/schema. Build an explicit Spark Connect relation/expression translator; it is not a complete Connect executor.
- Audit each mapping for eager work. The current `SparkSession::range()` allocates its range immediately and `DataFrame::count()` collects before counting. Implement lazy native range/aggregate paths or documented bounded alternatives before claiming lazy large-query execution.
- Planning, schema inference, local-relation decoding, and serialization can consume CPU/memory too. Include them in admission and compute scheduling, not only final actions.
- Preserve logical schema metadata through planning and execution. Current core schema/conversion loses distinctions including Float32 versus Double, Timestamp versus TimestampNTZ, and nested element nullability; it also reconstructs fields as nullable.
- Add the required logical type/nullability/metadata representation before enabling corresponding capabilities. Minimum scalar/schema fixes needed for the HTTP gate belong to Phase 0B; richer type support is Phase 1/2 work. Maintain agreement among AnalyzePlan schema, HTTP schema, and Arrow payloads.
- Test decimal boundaries, timestamp units/timezones/NTZ, nullability, array/map/struct representations, binary values, duplicate labels, and unsupported-type errors. Nested/map support is gated separately from scalar support.
- Arrow batches must conform to the selected client, including an appropriate schema-bearing batch for empty results. Enforce row and encoded-byte limits, including a single oversized value.
- Account for decode expansion/compression and retained buffers. A small protobuf/IPC message is not necessarily a small decoded relation.
- Avoid unnecessary copies, but measure allocations/copies before making zero-copy or memory-efficiency claims.

## Runtime limits, deadlines, and execution ownership

### Phase 0 guarantees

- Tokio owns nonblocking network I/O. A bounded compute scheduler owns CPU-bound planning/execution/encoding; Polars' internal pool is included in concurrency sizing.
- Enforce finite active-operation, queue, session, stream, input, serialized-output, and retained-result limits. Reject excess work promptly; bounding worker threads alone does not bound a waiting queue.
- Reserve capacity before accepting expensive work. Keep permits and resource accounting until native work and its producers actually finish, even if a response timed out or a connection disappeared.
- Track execution states such as queued, running, cancellation requested, succeeded, failed, and cancelled. Track transport attachment/deadline status separately.
- Report terminal cancellation only after a supported safe point establishes that outcome. A late cancellation must not replace a committed mutation's real result.
- Shutdown stops admission, requests cancellation, drains for a bounded interval, and reports unfinished work. The launcher may then terminate its owned process; this does not imply rollback.

### Explicit limits of in-process execution

Current cancellation is cooperative; some Polars work cannot be interrupted mid-call. A started blocking task cannot be killed by aborting its Tokio handle. Result batching/backpressure does not impose a hard bound on internal joins, sorts, aggregations, or all allocations.

Therefore:

- **Enforced bounds:** admission/queue counts, validated inputs, protocol result/replay buffers, and response-wait deadlines.
- **Cooperative or measured budgets:** native CPU time and intermediate memory. State the limitations in capabilities/docs and test behavior when cancellation is delayed.
- Do not claim per-query hard CPU/RSS isolation for the in-process backend. If required, design separately supervised **native Rust workers** with OS resource controls; they remain Python/JVM-free. Whole-container limits protect the host but do not isolate individual queries.
- Non-reattachable read-only requests may discard results and request cancellation on disconnect, while retaining accounting until computation stops. Spark Connect reattachable operations follow the retention contract below.

## Sessions, catalogs, and mutations

### Define ownership instead of inheriting incidental engine state

The current engine has session-local saved tables/database registrations and process-global temporary views. Neither cloning one session nor calling `new_session()` alone implements the desired service catalog model.

- **Phase 0B:** session-local configuration/state with bounded session creation, idle TTL, explicit close, and operation references. No remote mutations or durability promise.
- **Phase 1:** one explicit catalog service per configured trusted service scope owns shared saved tables and database metadata. Session-local temp views/configuration remain separate.
- Saved objects survive session close/TTL within the service lifetime. Restart durability is a separate capability requiring a selected storage/metadata model and restart tests.
- Process-global temporary views are unsupported until their namespace and cleanup can be scoped to the owning service/catalog. Do not expose the current global static accidentally.
- Define visibility across HTTP and gRPC logical sessions through the catalog service. Add native catalog hooks where needed; an external read resolver alone does not implement mutation coordination.
- Close/TTL stops new work and coordinates active operations; it must not destroy resources still in use or delete shared catalog objects. The lifetime of a shared service session is explicit; do not silently evict state expected by a client.

### Trust and identity

The existing HTTP adapter supplies only session/kind/code and uses **one `sql-warehouse` session for all warehouses**. It does not convey per-user or per-connection identity. Preserve that documented compatibility behavior; do not promise isolation the wire contract cannot represent.

Within an authenticated future deployment, session keys must incorporate the trusted principal/service scope, protocol, and logical session ID. Client-supplied session IDs or Spark `UserContext` are not authentication. Any richer HTTP isolation requires an explicit authentication and trust contract.

### Mutations, enabled in Phase 1

- Enumerate supported DDL/DML, visibility, atomicity, and failure behavior. Initially serialize mutations within a shared catalog scope and define reader snapshots/visibility, rather than relying on separate session locks.
- Associate operations with stable identities. Repeated live operation IDs must not execute the same mutation twice; reject ID reuse with different requests.
- Extend the HTTP adapter with an operation/idempotency identity before claiming safe mutation retries. Without it, do not automatically retry a possibly committed statement or claim exactly-once behavior.
- Preserve commit outcomes independently from client connectivity and result serialization. A lost response or result-limit failure after commit does not imply a failed/rolled-back write.
- Document restart behavior: in-memory deduplication does not provide durable exactly-once semantics. Require durable coordination before advertising that guarantee.
- Test concurrent append/update/DDL, close during work, disconnect before/after commit, duplicate submissions, and failure/restart boundaries.

## Spark Connect execution and streaming contract

Expose the actual pinned Spark Connect gRPC service, with loopback CLI default port 15002 and ephemeral ports available to the launcher. Clients use **`sc://`**, not `spark://`.

### Initial supported surface

- Session/configuration handling and required analysis requests.
- Range and bounded local relations.
- Projection, filter, explicit supported expressions, limit, and initial aggregations.
- Required action, error, Arrow schema/result, and execution/session cleanup behavior for the selected client.
- An explicit RPC/plan/type matrix. Unsupported nodes, artifact-backed large relations, UDFs, RDD/SparkContext, streaming, and distributed APIs fail clearly until individually enabled.
- Local-relation size thresholds must be tested against actual client behavior; do not assume every `createDataFrame` remains inline or silently bypass required artifact RPCs.

### Reattachment, release, and operation state

The reviewed Spark 4.0 client enables reattachable execution by default. Implement and verify the selected client's requirements for `ExecutePlan`, `AnalyzePlan`, `Config`, `ReattachExecute`, `ReleaseExecute`, `ReleaseSession`, and `Interrupt`, plus `FetchErrorDetails` behavior where used. The [public-contract research](CONNECT_PROTOCOL_RESEARCH.md) records source-backed retry, release, configuration, and IPC details; production pins and interoperability tests remain open. Excluded UDFs/Structured Streaming/artifact paths are Sparkless scope choices, not general Spark Connect limitations.

- Maintain stable session generation, operation IDs, ordered response IDs, and Arrow offsets. Validate client-observed server session IDs.
- Response IDs are opaque UUIDs ordered by an internal sequence; each Arrow response payload must be independently readable by the pinned client's IPC decoder. Configuration optional/default lookups preserve their normal missing/default outcomes without claiming support for ineffective settings.
- Send the protocol's completion marker and required metadata; transport end-of-stream alone must not falsely signal query completion.
- Reattachment resumes retained responses after the acknowledged response; it must not rerun the plan or a mutation.
- Release acknowledged prefixes and complete operations according to the protocol. Cleanup is idempotent and safe under concurrent release/reattach.
- A stream disconnect detaches a reattachable operation. It is **not automatically cancellation or permission to delete unacknowledged results**.
- Bound replay bytes, active/detached operations, retained errors, and retention time. Pause production where safe and fail/expire explicitly when budgets are exhausted; never silently evict required replay data or truncate successful results.
- Define expiration, released-session behavior, and server-restart behavior. A lost session/operation must not be silently recreated and reexecuted.
- Test reconnect after at least one batch, duplicate execute, repeated release, interrupt during detached execution, retention expiry/capacity exhaustion, and stop/release races.
- Also test request loss before acceptance and response loss before the first response. The default client can retry `ExecutePlan` after a missing handle at that boundary; abandoned identity/history handling must prevent reexecution within the declared retention scope. Connect duplicate errors and session-release behavior are distinct from HTTP receipt replay and session recreation.

Optional third-party proxy certification follows standalone conformance. Transport, authentication, session handling, and streaming require their own evidence before advertising that route; external changes or acceptance cannot block standalone Sparkless releases.

## DuckDB phases after supported Connect

Follow the [SS-14 source plan](14-duckdb-data-source.md) in Phase 3 / 4.19, then the [SS-15 execution backend plan](15-duckdb-connect-backend.md) in Phase 4 / 4.20. The first phase reads DuckDB tables through bounded native Arrow batches while Polars computes; the second executes supported SQL/DataFrame plans and enumerated writes inside DuckDB. Each has its own decisions, implementation slices, and release gates. Neither expands the Phase 2 release prerequisite.

Live access to independently written DuckDB tables requires a proven shared database owner and matching catalog/snapshot semantics. Independent writable processes cannot be assumed to share embedded database files. Keep sources read-only until the execution phase proves its write/outcome contract. Use self-contained DuckDB writer/reader fixtures; external application changes are not prerequisites. See D14/D15 and V35–V38 for the required ownership, semantic, memory, lifecycle, and interoperability evidence.

## Python lifecycle API

Expose **`from sparkless.server import SparklessServer`** from the existing Python package as an optional launcher for an owned native executable. It must support synchronous and asynchronous context managers and explicit `start()/stop()`, `async_start()/async_stop()`.

Phase 0B usage:

```python
from sparkless.server import SparklessServer

with SparklessServer() as server:
    print(server.http_url)

async def use_server():
    async with SparklessServer() as server:
        print(server.http_url)
```

Phase 2 usage:

```python
from sparkless.server import SparklessServer
from pyspark.sql import SparkSession

with SparklessServer() as server:
    spark = SparkSession.builder.remote(server.spark_connect_url).getOrCreate()
    try:
        assert spark.range(10).filter("id > 5").count() == 4
    finally:
        spark.stop()
```

Before Phase 2 is enabled, `spark_connect_url` raises a descriptive unsupported error. Endpoint properties also reject use when the owned process is not ready/running. Async lifecycle does not make the PySpark DataFrame API asynchronous.

Phase 3 / 4.19 adds optional `SparklessConnect`: `with SparklessConnect() as spark:` returns a real `SparkSession` from pyspark-client backed by an owned `SparklessServer`, then stops the session before shutting down the server, including exceptional exits. The proposed `sparkless[connect]` extra keeps pyspark-client optional. See [SS-06 slices 419-06–419-08](06-python-launcher.md#4190-optional-sparklessconnect-context-manager) for ownership, failure cleanup, packaging, and acceptance gates. This helper is independent of DuckDB.

### Discovery and distribution

- [ ] Importing the launcher must not import PySpark, download/build code, invoke Cargo, or start a process. Audit the existing parent-package import behavior; no embedded Python engine executes service requests.
- [ ] Provide an opt-in `sparkless[server]` extra backed by a version-compatible precompiled platform binary package, such as `sparkless-server-bin`. Discover its exact executable through package resources.
- [ ] Support explicit `executable="/path/to/sparkless-server"` and a documented manual Cargo build for development. Do not fall back to arbitrary PATH binaries or automatic builds.
- [ ] Validate a versioned launcher/server handshake and supported package compatibility range; fail clearly on absent, incompatible, or unsupported binaries.
- [ ] Keep CLI/container installation independent of Python. Keep launcher Python support (currently >=3.8) separate from the selected PySpark client's Python requirements.

### Ownership and cleanup

- [ ] Launch and stop only the owned child; never adopt/terminate an unrelated server.
- [ ] Let Rust bind port 0, then report actual addresses. Support explicit ports with clear collision errors and per-instance owned temporary directories.
- [ ] Use a bounded machine-readable readiness/control channel associated with that child, including a per-launch identity, versions, capabilities, and bound endpoints. Readiness follows native startup verification and listener availability; no fixed sleeps or probes that could accept another process.
- [ ] Expose `http_url`, supported `spark_connect_url`, `pid`, `is_running`, and bounded sanitized diagnostics. Drain logs continuously without unbounded buffering or secret-bearing SQL dumps.
- [ ] Startup waits to a configurable deadline, detects early exit, and reaps/cleans up on failure. Repeated start on the same running object is idempotent; concurrent lifecycle calls are serialized. Re-entry into the same active context is rejected; distinct instances may nest.
- [ ] Stop requests graceful shutdown through a cross-platform owned control mechanism, waits to a deadline, then force-terminates and reaps if necessary. Repeated stop is idempotent; restart after stop creates a fresh child and handshake.
- [ ] Async creation/readiness/shutdown must not block the event loop. Cancellation during startup or context exit must complete owned-child cleanup before propagating cancellation.
- [ ] Document POSIX/Windows signal handling and fork/thread limitations. An inherited launcher object must not stop the parent's process. GC/atexit is a fallback, not the lifecycle guarantee.
- [ ] Test missing/mismatched binaries, crash, timeout, noisy logs, ephemeral/explicit ports, context-body errors, async cancellation, multiple instances, restart, and absence of orphan processes.

## Security and operational requirements

- `#![forbid(unsafe_code)]` in new server-owned code; audit dependencies/FFI separately.
- Validate request sizes, decoded sizes, plan depth/node counts, schemas, identifiers, and nested values before expensive work.
- Bind loopback by default. Cross-container deployment needs an explicit bind override and an isolated trusted network or authenticated TLS/reverse-proxy model; do not publish an unauthenticated engine endpoint by accident.
- File/network readers, connectors, extension points, artifacts, and configuration changes need explicit capability/policy checks. Default Phase 0 SQL has no client-directed filesystem/network access.
- Structured tracing and metrics cover queue wait, active native work, detached operations, retained bytes, delayed cancellation, failures, and shutdown.
- No claims of multi-tenant isolation, hard per-query memory enforcement, or speedups without implemented mechanisms and evidence.

## Acceptance and release gates

### Phase 0A

- [ ] Version/semantics/transport/type contracts are written and pinned for testing.
- [ ] Native typed HTTP mapping and exact-number handling have verified in-repository fixtures.
- [ ] Native Connect feasibility exercises the unmodified client's initialization, computation, empty result, reconnect, and release paths.
- [ ] Engine gaps are recorded as prerequisites, with unsupported surfaces explicit.

### Phase 0B

- [ ] Workspace crate, native CLI/image, HTTP transport, minimum bounded runtime/session manager, and complete sync/async launcher.
- [ ] Direct HTTP and fresh packaged consumers assert both values and schemas for: `SELECT 1 AS result`, BIGINT `9007199254740993`, a high-precision DECIMAL, typed null, and an empty result with schema.
- [ ] Verify column order/duplicate labels, date/timestamp encoding, invalid SQL/kinds, rejected mutations/Python/types, and row/byte/input/admission limits.
- [ ] Saturate compute while health requests continue making progress. Demonstrate that timed-out native work still occupies its permit until completion.
- [ ] Verify session TTL/close, isolation of declared session state, startup failure, graceful/forced shutdown, async cancellation, and no child-process leaks.
- [ ] SQL execution in the server image requires neither Python nor Java. The client/launcher run in a separate client environment.

### Phase 1

The [4.17 implementation plan](RELEASE_4_17.md) refines this phase into delivery slices, proposed ephemeral catalog and mutation contracts, type/execution checks, and coordinated candidate/publication gates. Its choices remain baselines to validate; none of the criteria below is completed by planning documentation.

- [ ] Catalog ownership and configured persistence model pass cross-session visibility and close/restart tests.
- [ ] Mutation concurrency, idempotency scope, commit outcomes, and failure boundaries are verified before exposing writes.
- [ ] Logical schema fidelity and lazy range/count/planning behavior meet the enabled capability matrix.
- [ ] Memory measurements include intermediate computation and retained/replay data, not just serialized batches.

### Phase 2

The [4.18 implementation plan](RELEASE_4_18.md) refines this phase into 418-01–418-10, freezes the Spark 4.0.5 client/protocol and API/type/resource boundaries, and defines V33/V34 candidate/public-artifact gates. The accompanying [source review](CONNECT_PROTOCOL_RESEARCH.md) establishes public protocol provenance, not implementation conformance. Earlier release prerequisites remain required.

- [ ] Unmodified pinned JVM-free client computes `spark.range(10).filter("id > 5").count() == 4` through Robin's engine.
- [ ] Multi-batch and empty results, supported type boundaries, schema analysis, expressions, null/ANSI behavior, and unsupported plans are tested against real Spark with the same semantic settings.
- [ ] Execute/reattach/release/interrupt/session-release lifecycle passes under disconnects, retries, capacity pressure, and restart.
- [ ] Multiple clients get independent correct results while health I/O progresses; unsupported features fail truthfully.
- [ ] The public launcher enables `spark_connect_url` only for a ready supported endpoint.
- [ ] Optional third-party gRPC routes need separate certification before advertising and do not gate standalone releases.

### Later phases after Connect

- **Phase 3:** DuckDB source integration, with read-only typed Arrow scans into Polars, stable multi-table snapshots, native database ownership, independent DuckDB writer visibility, and measured memory/cancellation behavior. Complete V35/V36 before the 4.19 release.
- **Phase 4:** DuckDB execution behind the Connect endpoint, with explicit backend selection, reference-semantic and schema agreement, persistent transactional write outcomes, Connect lifecycle, and independent client read/write visibility. Complete V37/V38 before the 4.20 release; a source scan alone does not complete this phase.
- **Phase 5:** isolated Python worker packaging, code trust, quotas, environment/secrets handling, output/errors, Jobs/notebook behavior, scalar/Pandas UDF forms and type semantics, and worker cleanup. V27 covers each declared surface separately; core SQL/Connect images remain free of Python.
- **Phase 6:** native Delta/UC integration with independent-reader verification of schema and transactions, decimal preservation, and explicit rejection of unsupported writes/DDL.
- **Phase 7 / SS-16 / 4.23:** broaden the default Connect backend's expressions, joins/grouping/windows/set operations, scalar/nested types, truthful analysis, and mutable session settings. D16 freezes the exact matrix; V39/V40 require client/reference, lifecycle, memory, and isolation evidence.
- **Phase 8 / SS-17 / 4.24:** add CSV/JSON/Parquet and optional JDBC read/write APIs, public table/view paths, bounded artifacts/cached local relations, cache/persist/unpersist and DataFrame checkpoints. D17 and V41–V43 verify effects, policy, expansion limits, optional connector cleanup, and storage lifetimes.
- **Phase 9 / SS-18 / 4.25:** optional durable receipts, eligible session recovery including `allow_reconnect=true`, and response replay across restart. D18 and V44/V45 require transactional/fenced crash recovery; persistent data alone does not establish exactly-once effects or durable replay.
- **Phase 10 / SS-19 / 4.26:** optional native micro-batch Structured Streaming with public query management, selected sources/sinks, event-time watermarks/stateful windows, and coordinated checkpoints. D19 and V46/V47 require reference results, per-sink delivery guarantees, crash recovery, and bounded long-running state/disk resources.

Each new phase preserves earlier enabled capabilities and passes coordinated candidate/public installation checks. Optional Python/JDBC/Delta/DuckDB components are dependencies only for the APIs selecting them. Full Spark compatibility, RDD/SparkContext, distributed execution/failover, and continuous streaming are not implied by this extension.

### Common release checks

- Cargo format/clippy/tests using the pinned toolchain; feature-isolation/build checks and supported platform tests.
- Targeted unit/property/fuzz coverage for decoders, plan/type mappings, state transitions, and limit/error paths.
- Golden contracts use the actual SDK/client encoders and decoders. Differential tests pin oracle versions and settings.
- Benchmark correctness, cold start, throughput, p50/p95 latency, peak RSS, CPU, and streaming behavior at 1/2/4/8+ clients. Record hardware, versions, workloads, warmup, and commands; separate I/O responsiveness from CPU parallelism.
- Publish supported SQL/RPC/type/configuration matrices, resource-limit semantics, persistence behavior, installation instructions, and limitations.
- Explicit limitations: single-node; partial Spark compatibility; no Photon/Databricks Runtime; no arbitrary Python without optional workers.

## Child work items and dependency tracking

Split this epic into focused issues for:

1. Compatibility/version contracts and standalone typed-result fixtures.
2. Workspace crate, CLI, native packaging, and feature isolation.
3. Early Spark Connect client lifecycle feasibility.
4. Admission, operation state, cooperative cancellation accounting, and shutdown.
5. HTTP read-only SQL, exact typed results, and conformance.
6. Python launcher/control protocol, binary packaging, and lifecycle tests.
7. Catalog ownership, session isolation, and controlled mutation semantics.
8. Native logical schema fidelity and lazy range/count gaps.
9. Connect plan translation, analysis/configuration, and supported type/RPC matrix.
10. Arrow streaming, bounded replay, reattachment/release, and client conformance.
11. Standalone Connect release checks and reproducible benchmarks.
12. DuckDB data-source scans, source snapshots, database ownership, and bounded Arrow transfer into Polars.
13. DuckDB execution backend for Connect, semantic translation, persistent mutation outcomes, and shared DuckDB tables.
14. Optional isolated Python workers; optional Delta/UC interoperability.

Record cross-repository owners and immutable test revisions in those issues. Do not close this epic based only on crate scaffolding or the two smoke-query witnesses.

## References

- [Native facade](https://github.com/eddiethedean/robin-sparkless/blob/main/src/lib.rs), [session](https://github.com/eddiethedean/robin-sparkless/blob/main/src/session.rs), [DataFrame](https://github.com/eddiethedean/robin-sparkless/blob/main/src/dataframe.rs), [logical-plan format](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/LOGICAL_PLAN_FORMAT.md)
- [Core schema](https://github.com/eddiethedean/robin-sparkless/blob/main/crates/robin-sparkless-core/src/schema.rs), [schema conversion](https://github.com/eddiethedean/robin-sparkless/blob/main/crates/robin-sparkless-polars/src/schema_conv.rs), [native session ownership](https://github.com/eddiethedean/robin-sparkless/blob/main/crates/robin-sparkless-polars/src/session/mod.rs)
- [Emulator agent adapter](https://github.com/calvinchengx/databricks-emulator/blob/main/internal/spark/spark.go), [Statement Execution response](https://github.com/calvinchengx/databricks-emulator/blob/main/internal/server/sql.go), [gRPC proxy](https://github.com/calvinchengx/databricks-emulator/blob/main/internal/server/connect.go)
- [Family SQL agent envelope](https://github.com/calvinchengx/fabric-emulator/blob/main/python/spark_agent/sqlrun.py), [Databricks SDK SQL models](https://github.com/databricks/databricks-sdk-py/blob/main/databricks/sdk/service/sql.py)
- [Spark 4.0.5 protocol](https://github.com/apache/spark/blob/v4.0.5/sql/connect/common/src/main/protobuf/spark/connect/base.proto), [types](https://github.com/apache/spark/blob/v4.0.5/sql/connect/common/src/main/protobuf/spark/connect/types.proto), [reattachment client](https://github.com/apache/spark/blob/v4.0.5/python/pyspark/sql/connect/client/reattach.py)
- [Tokio blocking-task cancellation limits](https://docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html), [Go JSON number decoding](https://pkg.go.dev/encoding/json#Unmarshal)
