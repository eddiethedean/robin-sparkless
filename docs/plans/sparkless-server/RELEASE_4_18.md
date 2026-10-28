# 4.18.0 implementation and release plan

**Status: 418-01 contract frozen on 2026-10-28; no Phase 2 interoperability or release gates completed.** This plan refines **Phase 2**, following the [4.17 plan](RELEASE_4_17.md). Every first-party package in the [release inventory](ROADMAP.md#coordinated-version-and-publication-policy) publishes at **4.18.0**, including unchanged packages. Owners, implementation issues, and release evidence remain unassigned.

[Plan index](README.md) · [Epic](EPIC.md) · [Roadmap](ROADMAP.md) · [Protocol research](CONNECT_PROTOCOL_RESEARCH.md) · [Decisions](DECISIONS.md) · [Validation](VALIDATION.md)

## Outcome and prerequisites

An unmodified pinned `pyspark-client` connects to a native Rust service through `sc://`, analyzes and executes the declared DataFrame subset, decodes exact Arrow results, and reconnects/releases operations with its normal defaults. The required witness is `spark.range(10).filter("id > 5").count() == 4`. Multi-batch, empty, schema/type, negative, concurrent-client, and lifecycle cases are equally required. The service uses no Python or JVM; client and reference-engine processes run in separate environments.

The [public-contract research](CONNECT_PROTOCOL_RESEARCH.md) records the Apache Spark 4.0.5 compatibility pin and a byte-level comparison with the earlier 4.0.0 review. The pin selects the latest 4.0 maintenance patch while preserving this repository's existing Spark 4.0 semantic profile. It is a narrow compatibility target, not a claim of full Spark support. Product version 4.18.0 is independent of Spark protocol and semantic-profile versions.

Local work may start when the relevant interfaces settle. Shipping waits for a corrected 4.16-line release and completed 4.17 gates, followed by their applicable regressions on this candidate. Standalone Connect must pass before emulator gRPC certification. This release includes that separate proxy gate; an external blocker cannot be hidden by direct success. HTTP/SDK catalog and mutation behavior continues to pass its own suite.

## Ordered delivery slices

Slice IDs are local plan identifiers. Assign an owner, reviewable change, exact tested revision, and artifacts before advancing a slice.

| Slice | Deliverable | Dependencies | Exit evidence |
| --- | --- | --- | --- |
| **418-01 — complete** | Freeze public protocol/client pins, API/RPC/node/config/type matrix, shared transport interface, and proxy prerequisite | SS-03 traces; applicable 4.17 interfaces | D01–D04/D06–D11 baselines recorded; 27 actual client request fixtures and source comparison checked in; proxy prerequisite pinned. See [freeze record](#418-01-frozen-contract-record) and [fixture instructions](../../../tests/connect/README.md). |
| **418-02 — complete locally** | Generated service, bounded decoders, session identities, shared admission/operation ownership | 418-01; SS-04/07 | HTTP and Connect feature builds; public request decode, malformed identity, duplicate operation, message/depth/node/schema caps, release tombstones, and real gRPC wire-size tests; no second execution runtime. This does not pass V18–V26 or establish plan execution/interoperability. |
| **418-03** | Explicit plan/expression translation, configuration and analysis | 418-01/02; SS-08 | V19 public-client/oracle corpus, schema inference without actions, honest unsupported errors |
| **418-04** | Checked Connect schema/Arrow encoder and bounded inline local relations | 418-01/02; logical schema and native batches | Phase 2 V18/V20: PyArrow/client decoding, exact values, empty/multi-batch and byte boundaries |
| **418-05** | Replay, reconnect, prefix/all release, interrupt, expiry, session release/restart | 418-02; 418-03/04 agreed producer interface | V21/V22 deterministic races plus real network failures and measured accounting |
| **418-06** | Standalone integrated conformance and multiple real clients | 418-03–418-05 | V18–V23 and applicable V17; unmodified JVM-free client, health progress, native execution evidence |
| **418-07** | Public launcher readiness, CLI endpoints, packaged Connect feature | 418-06; SS-02/06 | V24 and launcher regressions across advertised platforms; HTTP-only build remains testable |
| **418-08** | Separate emulator gRPC route and concurrent-client benchmarks | 418-06; pinned external proxy; 418-07 for packaged launcher witnesses | V25/V26: proxy/session/auth/reconnect evidence and reproducible 1/2/4/8+ client measurements |
| **418-09** | Capability/docs freeze, regression and complete candidate distribution | 418-01–418-08; prior releases complete | V33: all pre-publication gates and exact 4.18.0 inventory pass on one integrated candidate |
| **418-10** | Coordinated publication and fresh public-artifact verification | 418-09 | V34: every public artifact installed, exercised, and matched to the candidate |

Translation and Arrow work can proceed independently against the interface frozen in 418-01; their integrated exit requires replay and the actual client. No individual workstream establishes protocol support alone.

## 418-01–418-03: public surface and shared execution boundary

### 418-01 frozen contract record

This record freezes the implementation input, not the behavior of an unbuilt server. The standalone client and matching reference are separate, hash-locked environments. The client fixture environment targets CPython 3.11; the published client metadata permits Python 3.9 and newer, but no other interpreter line is certified by these fixtures. The reference runs with Java 17 and explicitly sets `spark.sql.ansi.enabled=true`, `spark.sql.caseSensitive=false`, `spark.sql.session.timeZone=UTC`, and `spark.sql.execution.arrow.useLargeVarTypes=false`. Setting UTC removes the reference engine's host-timezone default from comparisons. Keep the real local-relation cache threshold at Spark's 64 MiB default; the one-byte fixture is only a deterministic branch probe.

| Contract | Frozen value |
| --- | --- |
| Python client and protocol package | `pyspark-client==4.0.5`; PyPI sdist SHA-256 `8ec7bd9afde36e48e65545271833e64c77b86e47d44fcc8660bfba948fca32e0`; Python `>=3.9` package metadata; fixture host CPython 3.11 |
| Reference Spark Connect server | Apache `spark-4.0.5-bin-hadoop3-connect.tgz`; SHA-512 `4cc2e43959b5e0a67b7d895e1aa1fb3ab05fa6ad4a5912de76236bb9b70f9557b20837b05ee63474798e927130e72ed6d1fadcd273a71e65c70eac18771c0d6b`; Java 17; UTC, ANSI enabled, case-insensitive, `useLargeVarTypes=false` |
| Reference Python client | `pyspark[connect]==4.0.5`; sdist SHA-256 `a0d44b0e7faf0458ad83b7aa664e7ed8b96beed8a55d66ce3b0dfac2309dfca6` |
| Public protocol sources | Apache Spark tag `v4.0.5`, commit [`b472a3aa3c4b71b5a6ff068ef396f757bd662e59`](https://github.com/apache/spark/tree/b472a3aa3c4b71b5a6ff068ef396f757bd662e59); all ten public Connect `.proto` files in `sql/connect/common/src/main/protobuf/spark/connect/`; Apache-2.0 |
| Rust gRPC/protobuf generation and descriptor walk | `tonic=0.14.8`, `tonic-prost=0.14.8`, `tonic-prost-build=0.14.8`, `prost=0.14.4`, `prost-build=0.14.4`, `prost-types=0.14.4`, `prost-reflect=0.16.5`, `protoc-bin-vendored=3.3.0`; project Rust toolchain `1.93.1`; `prost-types` maps public `google.protobuf.Any`, and `prost-reflect` enforces D08 depth/node/schema bounds. Generated bindings and runtime dependencies belong in the server's `connect` feature, not engine or extension dependencies |
| Native Arrow boundary | Existing locked `polars-arrow=0.53.0`; encode each `ExecutePlanResponse.ArrowBatch.data` as an independently readable Arrow IPC stream, using checked logical-to-physical mappings rather than conversion through a second Arrow implementation |
| Dependencies and reproducibility | [Client/oracle lockfiles and generator](../../../tests/connect/README.md); each lock has package hashes. The committed fixtures record deterministic text/binary request SHA-256 values and exact protobuf message types. |

The 27 requests under [`tests/connect/fixtures/pyspark-client-4.0.5`](../../../tests/connect/fixtures/pyspark-client-4.0.5) are captured from normal `SparkSession`/`DataFrame` APIs plus the pinned client's own reattach/release request builder. They show that ordinary `ExecutePlan` is reattachable by default and carries an operation ID; a missing response is not permission to execute a second producer. `createDataFrame` first batches nine configuration keys, embeds ordinary local data as Arrow IPC in `LocalRelation`, and sends schema-only empty data as `LocalRelation.schema`. Its threshold-crossing path changes to `CachedLocalRelation` and requires artifact transfer; the checked-in one-byte probe replaces only that upload call with a fixed ID. Treat artifact-backed relations as explicitly unsupported until a bounded `AddArtifacts` path is implemented.

Slice 418-02 vendors the same ten public protocol sources, unchanged, under `crates/sparkless-server/proto/spark/connect/`; their hashes match the source review above. The `connect` feature generates `SparkConnectService` from those files and exposes an adapter on the shared server instance. Its descriptor-aware wire preflight runs before Prost allocates the request tree; it enforces a 4 MiB protobuf message cap, at most 64 message levels, 10,000 message nodes, and 256 schema fields. It derives a namespaced identity from `(user_id, session_id)`, tracks a generation-bearing native session lease and server-side session UUID, tombstones released Connect sessions, and reserves operation IDs in the server's shared admission/history registry. The Connect lifecycle lock serializes session admission, operation reservation, and release; HTTP rejects the reserved internal key prefixes to keep its IDs isolated. `user_id` partitions identity and is not authentication. Plan translation, analysis, configuration, result streaming, and replay remain in later slices; methods beyond this boundary return explicit gRPC statuses.

The observed RPC set for the initial subset is `ExecutePlan`, `AnalyzePlan`, `Config`, `ReattachExecute`, `ReleaseExecute`, `Interrupt`, and `ReleaseSession`, with `FetchErrorDetails` when returning the selected client's enriched remote error details. `AddArtifacts` and `ArtifactStatus` are known public service methods, not evidence that Sparkless implements artifacts; reject them and cached relations before any partial acceptance. The request corpus also records `spark.sql()` as a command/result relation, so it remains excluded from the first read-only subset even though HTTP SQL exists.

The cross-transport boundary is frozen as one server-owned session/catalog and operation coordinator with transport-specific adapters:

1. Resolve `SessionIdentity { namespace, user_id, client_session_id, observed_server_session_id }` to a generation-bearing native `SessionLease`. `namespace` separates Connect from HTTP; Connect's `user_id` is a partition key, not authentication. The server-issued session identity detects restart/replacement and is checked on subsequent requests.
2. Register an operation under `(namespace, session generation, client operation_id)` before native production. Return a single producer lease with shared admission, semantic snapshot, cancellation token, and completion owner. A duplicate retained Connect operation follows the pinned protocol error path; it never creates another producer and never uses the HTTP mutation-receipt replay path.
3. The shared native plan/schema/batch interface returns typed native results. Connect alone serializes and stores bounded response envelopes with opaque response IDs and internal monotonic sequence/row offsets. It retains the same serialized Arrow response for reattachment; HTTP keeps its existing response and receipt formats.
4. The Connect sender consumes retained envelopes outside native/catalog locks. Prefix release, release-all, session release, interrupt, expiry, and restart change Connect transport/operation state without creating a second catalog, executor, admission pool, or session manager. Native leases remain until the producer actually stops.

Freeze the separate Cargo feature names as `http` and `connect`, and the standalone gRPC URI as `sc://127.0.0.1:15002` by default. The launcher may advertise `spark_connect_url` only after the Connect listener is bound and its readiness identity/capability is verified. The emulator's internal backend URI remains `http://sparkless-server:15002`; it is not the public client's connection URI. The HTTP-only build remains independently available.

The proxy prerequisite is now concrete: MiniLake PR [#1](https://github.com/eddiethedean/minilake/pull/1), “Deferred prototype: Sparkless HTTP SQL adapter and operation recovery,” is open against `main` at head `870803fde5a869ca582a3ebe1fd18bad9947a227` on `sparkless-phase1-adapter`. Its tracked tree contains no Connect/gRPC paths. It therefore does not satisfy V25. A separate MiniLake gRPC proxy/session/auth change, immutable revision, and backend route evidence remain prerequisites for 418-08; no proxy certification is inferred from HTTP/SDK results.

Preserve unknown-field compatibility where decoding allows it, but reject an unknown semantic variant, extension, or option that would otherwise change execution. The captured public paths pin these encodings:

| Public API path | Captured RPC and emitted shape | 418-01 support boundary |
| --- | --- | --- |
| `range(0, 10, 1, 1).filter("id > 5").count()` | `ExecutePlan`; `Range → Filter(expression_string) → Aggregate(count(literal 1))`; normal reattach option and operation ID | Required end-to-end witness; string expression must use the checked expression parser |
| `range(10).select("id").limit(3).collect()`, `take(3)`, `head(3)` | `ExecutePlan`; `Project(unresolved_attribute)` and/or `Limit`; `take` and `head` both encode `Range → Limit` | Required, bounded; the plan has no ordering guarantee without an explicit ordering operation |
| Column comparison, arithmetic, alias | `ExecutePlan`; `unresolved_function(">=")`, `unresolved_function("+")`, `unresolved_attribute`, integer literals, `Alias` | Enable only the captured `>=` and `+` column expressions, integer literals, and aliases; reject casts, boolean/null predicates, stars, and other functions until added with fixtures |
| `toLocalIterator()` | `ExecutePlan`; a range is sent without an implicit `Limit` | Required bounded streaming action; output limits end in a visible error, never a successful prefix |
| `DataFrame.schema` and string schema in `createDataFrame` | `AnalyzePlan.Schema(plan)` and `AnalyzePlan.DDLParse(ddl_string)` | Required analysis operations; resolve schema without hidden collection or execution |
| `createDataFrame(rows, StructType)` and empty input with explicit schema | `Config.Get` followed by `ExecutePlan.LocalRelation(data=Arrow IPC, schema=StructType JSON)`; empty input has schema but no data | Required scalar input forms; enforce byte/row/schema limits and preserve nullability |
| Local relation at cache threshold | `ExecutePlan.CachedLocalRelation(hash)` after the normal public client crosses `spark.sql.session.localRelationCacheThreshold` and transfers the serialized relation through `AddArtifacts` | Explicitly unsupported in this slice; return a clear unsupported error before accepting an execution that references missing artifact state |
| Runtime configuration | Python `RuntimeConfig` emits `Get`, `GetWithDefault`, `GetAll`, `IsModifiable`, `Set`, and `Unset`; the protobuf `GetOption` branch is separately captured through the low-level request builder | Preserve supported reads and absent/default results; report unsupported settings as not modifiable and reject changes instead of silently acknowledging them |
| `spark.sql("SELECT 1 AS result")` | `ExecutePlan.Command(SqlCommand → Sql)` followed by a command-result path | Explicitly excluded; SQL HTTP support does not enable Connect SQL or commands |
| Reconnect and cleanup | `ReattachExecute`, `ReleaseExecute` prefix/all, `Interrupt` session/tag/operation, `ReleaseSession` | Required lifecycle RPCs; `FetchErrorDetails` is required when returning the pinned client's enriched error detail |

Generated protobuf coverage does not establish native support. All captured positive requests are under [`tests/connect/fixtures/pyspark-client-4.0.5`](../../../tests/connect/fixtures/pyspark-client-4.0.5); they contain client-produced request data only, not successful server responses.

### Frozen 418-01 API/RPC/node matrix

| Surface | Required 4.18 baseline | Explicit boundary |
| --- | --- | --- |
| Session and configuration | Standard remote initialization; effective UTC/ANSI/case profile; get/default/optional/list/modifiability behavior needed by enabled APIs | No successful unsupported semantic change; no process-global environment mutation |
| Range | Positive/negative steps, empty ranges, checked signed bounds; column `id` and BIGINT count | Validate `numPartitions` against reference semantics; no distributed execution claim |
| Inline local relation | Explicit scalar schema, empty/nonempty data, ordinary client Arrow encoder and required rename wrappers | Cached/artifact-backed data rejected until separately enabled; test real threshold crossing |
| Projection and expressions | Column references/plan IDs, aliases, integer literals, captured `>=` and `+` expressions | Casts, boolean/null predicates, stars, and other functions stay unsupported until individually added to the allowlist and fixture corpus |
| Filter | Column expressions and parsed string expressions such as `id > 5` | Same native checked expression semantics; no unchecked SQL forwarding |
| Limit and actions | Nonnegative limit, `collect`, bounded `toLocalIterator`, `count`; required `head`/`take` wrappers identified from traces | Preserve actual ordering guarantees only; bounded output may end in a visible error |
| Aggregate | Global `count(*)`, including empty input | `count(column)`, grouped aggregates, and additional functions require their own matrix extension |
| Analysis | Schema and other requests exercised by initialization and the enabled APIs, including explicit-schema parsing where used | No fabricated Catalyst plan, statistics, storage status, or unsupported analysis result |
| SQL/table access | Reject until the exact public API path and native allowlist are explicitly enabled | HTTP SQL/catalog support alone does not enable Connect SQL commands or table reads |

The baseline intentionally excludes joins, sorting, windows, general grouping, caching/checkpointing, file/network readers/writers, Connect DDL/DML, UDFs, RDD/SparkContext, Structured Streaming, and Python workers. Several are supported by Apache Spark Connect; these exclusions describe Sparkless scope. Supporting Arrow result streams does not enable Structured Streaming. Existing HTTP writes remain enabled only under their separately verified identity contract.

Generate the real `spark.connect.SparkConnectService` from the chosen public sources. Implement `ExecutePlan`, `AnalyzePlan`, `Config`, `ReattachExecute`, `ReleaseExecute`, `Interrupt`, `ReleaseSession`, and the error-detail behavior used by the pinned client. Return explicit unsupported errors for `AddArtifacts`, `ArtifactStatus`, and undeclared variants. Do not add HTTP-only required fields to standard Connect requests. The source-backed details and their limits are in the [research note](CONNECT_PROTOCOL_RESEARCH.md#service-and-stream-requirements).

Trace config reads even for excluded inference modes: the client may batch them with required values before producing a supported local relation. Advertise honest effective values, implement absent/default lookup semantics, and reject unsupported sets rather than acknowledging an ignored change. Treat SQL strings inside expressions and DDL/JSON local schemas as bounded parse inputs. `spark.sql()` has a command/result path; support cannot be inferred from a relation handler.

Use one server-owned runtime/catalog service, with a distinct transport namespace for Connect sessions and HTTP identities. Map Connect user/session identity plus server generation to native session leases; client-supplied identity is not authorization. Preserve shared saved-catalog ownership while keeping temporary views/configuration isolated. No accidental alias to `sql-warehouse`. Analysis uses the same resolver, logical schema, and semantic snapshot as execution, under admission for expensive work. Planning must not perform a hidden collect/count action.

## 418-04: logical types, physical Arrow, and local data

Required Connect types initially match the proven HTTP scalar set: BOOLEAN, SMALLINT, INT, BIGINT, finite DOUBLE, STRING, DATE, UTC microsecond TIMESTAMP, and DECIMAL precision 1–38 with valid nonnegative scale. Support typed nulls and schema-bearing empty results. These are required delivery targets, not currently enabled Connect types. PySpark 4.0.5 maps the initial logical types to these Arrow physical types:

| Spark logical type | Arrow physical type |
| --- | --- |
| BOOLEAN | `bool` |
| SMALLINT | `int16` |
| INT | `int32` |
| BIGINT | `int64` |
| finite DOUBLE | `double` |
| STRING | `string`, with `spark.sql.execution.arrow.useLargeVarTypes=false` |
| DATE | `date32[day]` |
| TIMESTAMP under the UTC profile | `timestamp[us, tz=UTC]` |
| DECIMAL(p, s), 1 ≤ p ≤ 38, 0 ≤ s ≤ p | `decimal128(p, s)` |

Check these physical types together with standard Connect protobuf logical descriptors and client-decoded values. Signed BYTE (`int8`), FLOAT (`float32`), binary, NTZ timestamps, arrays/structs/maps, intervals, untyped null, and non-finite floats stay disabled until input, literal, analysis, transformation, result, empty, and reconnect evidence exists. Native support or representation in the public protocol does not automatically enable remote values. If normal encoders introduce a disabled type for a required API, repair/enable that path explicitly or narrow the documented input form before freezing the matrix.

Reuse native `polars-arrow` batches through a checked IPC boundary; do not assume `arrow-rs` type interchangeability or require Sparkless private metadata to decode values. Preserve ordered/duplicate public labels, nullability, decimal scale, timestamp semantics, and field metadata through the declared transforms. Test null versus empty string, large signed integers, exact decimals, negative epochs, microseconds, duplicate aliases, and empty/null aggregates. Compare public schema with the reference; normalize only documented representation differences.

Encode each response's Arrow payload as a complete independently readable IPC stream. Assign opaque response IDs and internal sequence positions, track cumulative row offsets, validate row counts, and retain the serialized response for replay before handing it to a transport. Test an empty result through both PyArrow and the public client. Bound actual serialized bytes including schema/protobuf overhead, rather than relying only on a row estimate.

For inline inputs, enforce protobuf message, IPC data, decoded-buffer, schema-depth, row/column, and individual-value limits before expensive allocation where possible. Count compressed/dictionary expansion separately from wire size; reject unsupported IPC encodings/compression explicitly. Test schema-only empty local relations, DDL/JSON schema overrides, mismatched schemas, malformed IPC, deep nested rejected types, and cached-local-relation threshold crossing. Do not alter client defaults or raise its cache threshold to make conformance pass.

## 418-05: replay and lifecycle contract

Freeze D07 with separate execution state (`queued/running/completed/failed/cancel-requested`) and transport state (`attached/detached/released/expired`). Completion owns the real native producer lifetime; it is not inferred from an RPC closing. Prefix release, complete release, session release, and interruption have different effects. Persist no data across process restart in this baseline.

| Event | Required implementation behavior and witness |
| --- | --- |
| Execute accepted | Atomically register identity before native work; one producer owns admission, semantic snapshot, and results |
| Duplicate execute | An active duplicate operation ID returns `INVALID_HANDLE.OPERATION_ALREADY_EXISTS`; an abandoned/retained identity never starts a second producer and follows its pinned state-specific error path. Do not substitute HTTP receipt replay. |
| Stream loss | Detach reattachable execution; preserve unacknowledged responses and producer accounting; non-reattachable requests use their documented cancellation rule |
| Reattach | Validate session generation and cursor; resume retained serialized responses without reexecution; no UUID sorting or fabricated success for unavailable data |
| Prefix release | Reclaim only the acknowledged prefix, safely under concurrent sender/reattach ownership; tolerate required late/repeated cleanup behavior |
| Release-all | Request teardown of running work and free transport/replay ownership when safe; retain compute/snapshot leases until the producer stops |
| Interrupt | Support session-all, operation-ID and tag modes required by the pinned client; distinguish request acceptance from actual termination |
| Session release/TTL | Stop admission, cancel/drain owned work, clear local state after references end; default release tombstones the session, unlike HTTP ID recreation; reject `allow_reconnect=true` until separately enabled |
| Deadline/expiry/capacity | Explicit protocol-visible terminal outcome; no successful truncation or conversion of an abandoned execution into fresh work |
| Restart | New server/session generation, lost in-memory operations/catalog; stale observed identity and reattachment fail clearly |

Test first-request loss, acceptance followed by loss before the first response, loss after one and several batches, loss around completion, and asynchronous releases racing reattach. The default client may issue `ExecutePlan` again after a missing handle before its first response. Retain bounded abandoned-operation/session history or retire the generation before forgetting accepted identities; document the exact no-reexecution scope and rejection behavior in D07. A generic not-found error must not accidentally authorize a retry of forgotten work. No durable exactly-once guarantee follows.

Our product policy retains unacknowledged responses until release or explicit failure/expiry, which is stricter than Apache's configurable retry window. Bound response bytes, response-index metadata, pending sends, detached operations, tombstones, retained errors, and retained schema/snapshots. Reserve capacity before production; pause at safe points or fail explicitly. Reserve control capacity for release/interrupt/health so a full data buffer can drain. No lock may span expensive native computation or transport waits. A slow/detached client must not create an unbounded producer or occupy all control servicing.

### D08 frozen resource budgets to implement and measure

These are the selected Sparkless engineering defaults for the Connect implementation. They are not Apache defaults or hard RSS guarantees. Retain the earlier shared admission/session/catalog budgets and HTTP result limits. Slices 418-02–418-06 must implement and measure them; any changed value needs a recorded D08 revision before the client gate.

| Setting | Selected baseline | Validation |
| --- | --- | --- |
| Shared compute/pending | 2 active process-wide, 1 per session; 16 pending process-wide, 4 per session | HTTP and Connect share permits; waiters do not consume compute threads |
| Inbound protobuf / local IPC / decoded local buffers | 4 MiB / 1 MiB / 8 MiB | Bounded rejection, expansion accounting and client cache-boundary fixtures |
| Local input rows / individual scalar value | 10,000 rows / 512 KiB decoded value | Check before staging; serialized IPC/response caps independently apply |
| Plan/schema structure | Depth 64, 10,000 nodes, 256 columns; existing metadata depth/byte limits | Guard decoder recursion as well as post-decode traversal |
| Arrow response | At most 16,384 rows and 1 MiB complete serialized response | Split on actual bytes; fail a row/schema that cannot fit; no successful truncation |
| Connect result total | 1,000,000 rows and 64 MiB cumulative serialized responses per execution | Count replay only once toward logical output, but every live buffer toward retained memory |
| Replay bytes | 8 MiB per operation, 32 MiB process-wide | Include schema, completion/error and sender buffers; no hidden unbounded channel/spool |
| Retained operations / history | 128 operations; 1,024 history entries and 4 MiB history/error metadata process-wide | Reserve before accepting; reject capacity rather than discard retry protection; define generation retirement in D07 |
| Detached retention | 60 seconds from detachment; reaper interval at most 5 seconds | Independent from response/native deadlines; no indefinite extension by failed reconnect attempts |
| Compute deadline / replay stall | Existing 30-second acceptance deadline / 5-second no-progress stall | Explicit error and cooperative cancellation; keep leases through actual stop |
| Live stream senders | 32 process-wide with bounded separate control servicing | Reattach replaces or rejects an old sender according to pinned behavior; no sender leak |

Use smaller configured limits for deterministic tests and verify selected defaults separately. A stream may have delivered rows before an output-limit error; it must end visibly failed without a success completion marker. Measure native intermediates, buffers and RSS separately. No memory bound follows solely from IPC batching or an encoded input cap.

## 418-06–418-08: standalone, launcher, proxy, and measurements

Run the unmodified locked client in a fresh environment without Java/Spark jars against a separate native image without Python/JVM. Use normal Python imports, default retries and reattachment. Capture sanitized RPC traces and native producer-start counters to prove results come from the engine and reconnect never repeats work. Test errors through the client's normal exception path, including enriched error retrieval when advertised; do not fabricate JVM traces or Catalyst output.

For the launcher, add an explicitly configured gRPC listener with CLI loopback default port 15002 and launcher-allocated ephemeral ports. Keep the HTTP listener independently configurable. Readiness must include actual bound addresses, per-launch/server identity, and independently versioned capabilities/control framing. Update the current readiness validator that rejects any Connect advertisement. Enable `spark_connect_url` only when the owned child is ready and its packaged supported matrix has passed V24; disabled/experimental builds still raise a typed unsupported error. The launcher must not import PySpark to start the native service. Check restart, port collision, failed gRPC readiness, cancellation, sync/async cleanup, and drain across both listeners.

Propose server-only `http` and `connect` Cargo features, with release binary wheels/images enabling both after their gates pass. Test HTTP-only and Connect-only builds plus the supported combined build; keep tonic/prost/protobuf generation out of embedded engine and Python-extension dependency paths. Freeze the exact feature/CLI/control names in D05/D06 before packaging. Build flags alone must not advertise an experimental endpoint as supported.

After standalone conformance, test the pinned emulator with `DATABRICKS_SPARK_CONNECT_GRPC_URL=http://sparkless-server:15002`. This is its h2c backend address; the public client still uses the proxy's Connect connection contract. Verify real authentication handling, cluster routing, metadata/trailers, distinct client sessions, Arrow streaming, release/cancel, first-response and mid-stream reconnect, rejected stale identities, and shutdown. HTTP SDK evidence is separate. An unavailable proxy requires an explicit gate failure, not a broader support claim.

V26 measures correctness, cold start, throughput, p50/p95 latency, RSS, CPU, batch/replay behavior, and cleanup at 1/2/4/8+ client processes on fixed workloads/settings. Record hardware, native/client/proxy/reference versions, warmup, raw commands/results and numeric regression budgets before passing. Include slow/disconnected readers plus simultaneous HTTP/Connect saturation while health progresses. Keep producer-time, client collection memory, replay memory and native intermediates distinguishable. No unmeasured speedup, zero-copy, CPU parallelism, or hard memory claim.

## Validation corpus and release gates

| Scenario | Required assertion | Gate |
| --- | --- | --- |
| Public range/filter/count plus limit/projection/global count | Exact values, schema, aliases and empty/null semantics through native computation and matched reference | V17–V19 |
| Inline explicit-schema local data, normal config queries, cache threshold boundary | Ordinary encoders work; valid default/missing config behavior; unsupported artifacts fail truthfully | V19 |
| Exact BIGINT/DECIMAL/date/time/null and duplicate labels | Logical/protobuf/physical Arrow/public schema and values agree on empty and nonempty data | V18/V20 |
| Multi-batch, byte-sized splits, oversize cell/schema and total result limit | Each IPC payload decodes; offsets/IDs/completion correct; errors cannot appear as successful truncated output | V20 |
| Loss before request acceptance, before first response, after batches, around completion | At most one producer per retained identity; correct cursor continuation/default retries and duplicate errors | V21 |
| Prefix/all/session release, interrupt, TTL, restart and stale server/session ID | Safe concurrent teardown, default session tombstones and explicit rejection of undeclared `allow_reconnect`, no false compute completion, saved-catalog lifetime preserved | V21 |
| Full replay/history/error budgets, slow/detached clients and expiry | Explicit bounded failure, retry protection preserved, live leases held until native stop, resources reclaimed | V22 |
| Several client processes plus HTTP saturation, health and shutdown | Independent correct results, bounded queues/control progress, clean child/server cleanup | V23/V26 |
| Installed sync/async launcher and disabled-feature variants | Ready valid endpoint only; old HTTP capability regressions and binary/control mismatches checked | V24 |
| Actual emulator gRPC route | Authentication/routing/metadata/session/stream/reconnect behavior verified after standalone suite | V25 |

**Ready to tag/publish:** 418-01–418-09 complete, prior prerequisites/regressions pass, V18–V26 plus applicable V17 and **V33** pass on one candidate. V33 covers every 4.18.0 package, dependency, lock/runtime/binary version, launcher coupling, Rust archive, wheel/sdist, image and platform. Install outside the checkout without inherited root Cargo patches/local paths. Run HTTP/SDK and standalone/proxied Connect from candidate artifacts, not just development binaries. Verify protocol generation/provenance survives `cargo package`; keep a tested HTTP-only feature boundary. Extend the current release workflow for these new gates before publishing; its existing 4.17 jobs do not implement them.

**Released:** 418-10 and **V34** verify the complete public inventory, exact hashes/image digests, `v4.18.0` tag and fresh exact-version installs. Exercise HTTP/SDK regressions, standalone and proxied Connect witnesses, reconnect/release and owned launcher lifecycle from published artifacts. Partial publication is incomplete; immutable package fixes require a new coordinated patch. Publish supported API/RPC/node/type/config, Python/platform, limits, trust and lifetime matrices alongside the tested artifacts.

- [ ] 418-01: contracts, source review differences, pins and external prerequisite recorded.
- [x] 418-02 local implementation and focused checks complete; this does not pass the V18–V26 interoperability or release gates.
- [ ] 418-03–418-05: native translation/analysis, physical Arrow and lifecycle verified.
- [ ] 418-06: standalone default-client and concurrent-client conformance passes.
- [ ] 418-07–418-08: launcher, separate proxy route and benchmark evidence passes.
- [ ] 418-09: prior regressions and V33 complete candidate gates pass.
- [ ] 418-10: V34 public inventory verification completes 4.18.0.

## Current implementation boundary

The repository contains a native HTTP service and logical-schema/IPC/lazy execution work described in the 4.17 plan. Slice 418-02 adds an optional generated Spark 4.0.5 gRPC service adapter and a caller-bound listener method to `sparkless-server`; it shares the existing native session, catalog, admission, and operation registries. The listener is not yet wired into launcher readiness, and the launcher continues to advertise `spark_connect_supported: false` and reject Connect endpoint properties. Execute/analysis/config translation, Arrow results, replay, launcher packaging, proxy certification, release gates, and public artifact verification remain open.
