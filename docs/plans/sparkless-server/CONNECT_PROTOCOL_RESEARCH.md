# Spark Connect public-contract research for 4.18

**Reviewed: 2026-10-28. Scope: Apache Spark 4.0.5 public sources and documentation.** This is a protocol review, not a passing interoperability test or a claim to support every Spark client. The selected tag `v4.0.5` resolves to commit [`b472a3aa3c4b71b5a6ff068ef396f757bd662e59`](https://github.com/apache/spark/tree/b472a3aa3c4b71b5a6ff068ef396f757bd662e59). This exact client/protocol/oracle line is now frozen by D01; implementation still requires client conformance.

[4.18 plan](RELEASE_4_18.md) · [Decisions](DECISIONS.md) · [Validation](VALIDATION.md)

## What constitutes the public contract

Spark Connect has an Apache-published protobuf service and client implementation, built on gRPC and Arrow IPC. Treat these as the interoperability contract; there is no independent conformance certification established by this review. Spark's documented architecture sends unresolved plans, with results returned as Arrow batches. Neither the protocol nor Arrow specifies Robin's execution engine. A Rust implementation is an architectural fit, but interoperability still needs actual client tests. [Apache Spark overview](https://spark.apache.org/docs/4.0.5/spark-connect-overview.html), [gRPC lifecycle](https://grpc.io/docs/what-is-grpc/core-concepts/), [Arrow IPC specification](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format).

For a JVM-free Python client use the separately distributed `pyspark-client`, which supports remote `sc://` connections without Spark jars or a JRE. Its 4.0.5 package metadata requires Python 3.9 or newer. Do not confuse it with the `pyspark` distribution, which can start a local Spark server. These client requirements are separate from the launcher's Python compatibility. [Apache installation guide](https://spark.apache.org/docs/4.0.5/api/python/getting_started/install.html#python-spark-connect-client), [pyspark-client 4.0.5 on PyPI](https://pypi.org/project/pyspark-client/4.0.5/).

## Frozen 4.18 client and protocol baseline

The repository already runs its Spark semantic profile against the Spark 4.0 family (`tests/requirements-pyspark4.txt`). Spark 4.0.5 is the latest maintenance release on that family at this freeze, and [Apache recommends 4.0 users upgrade to it](https://spark.apache.org/releases/spark-release-4-0-5.html). The standalone client and reference Python client are `pyspark-client==4.0.5` and `pyspark[connect]==4.0.5`; their PyPI sdist SHA-256 values are respectively `8ec7bd9afde36e48e65545271833e64c77b86e47d44fcc8660bfba948fca32e0` and `a0d44b0e7faf0458ad83b7aa664e7ed8b96beed8a55d66ce3b0dfac2309dfca6`. The hash locks use pandas 2.x to match Spark's documented Connect dependency matrix. The matching server is Apache's `spark-4.0.5-bin-hadoop3-connect.tgz`, SHA-512 `4cc2e43959b5e0a67b7d895e1aa1fb3ab05fa6ad4a5912de76236bb9b70f9557b20837b05ee63474798e927130e72ed6d1fadcd273a71e65c70eac18771c0d6b`, with Java 17. Set UTC session timezone, ANSI mode enabled, case-insensitive identifiers, and `useLargeVarTypes=false` for the oracle. The hash-locked CPython 3.11 environments and actual client request corpus are in [`tests/connect`](../../../tests/connect/README.md). These captures do not certify other interpreter versions.

For the native generated service use Rust toolchain 1.93.1 and freeze `tonic`/`tonic-prost`/`tonic-prost-build` 0.14.8, `prost`/`prost-build`/`prost-types` 0.14.4, `prost-reflect` 0.16.5, and `protoc-bin-vendored` 3.3.0. Spark's ten Connect `.proto` sources under `sql/connect/common/src/main/protobuf/spark/connect/` are Apache-2.0; `tonic` crates are MIT and `prost` crates Apache-2.0. `prost-types` supplies public `google.protobuf.Any`; server-only `prost-reflect` drives a descriptor-aware wire preflight before Prost allocates generated messages and a second decoded-request check for Sparkless' D08 depth/node/schema limits. Keep generator and runtime dependencies server-only and feature-gated. The repo's existing native serialization dependency is `polars-arrow` 0.53.0; do not round-trip through a second Arrow Rust implementation. The checked-in sources and included Apache-2.0 license are under `crates/sparkless-server/proto/`.

### Source comparison against the earlier 4.0.0 review

Every public Connect `.proto` file in the Spark 4.0.0 and 4.0.5 commits has the same SHA-256. The generated Python protobuf and gRPC service modules are also byte-identical. Hashes below were fetched from the Apache Spark Git tags; equal hashes establish that this patch release did not change those protocol inputs.

| Apache Spark source | v4.0.0 SHA-256 | v4.0.5 SHA-256 |
| --- | --- | --- |
| `base.proto` | `e523f95a4124fed8129e3f082aae20befe12db10170426880a165b160e7b1996` | same |
| `catalog.proto` | `f4e4211e4b1b0905c7b1e1b05bb008e2c0354b1dec971188cb2d76ab654699da` | same |
| `commands.proto` | `d934c9fb02911866c38d4e7fe08b280a802e1f7deb2cc4d9524c2f904d4d4c53` | same |
| `common.proto` | `cb508096334b83f61e2020d7929e7b357647d67858d29fa7fe2aa8bcb8f5652a` | same |
| `example_plugins.proto` | `73c74dc164cc6c78ef7ce362ce2e0e0efa5b02aeaf1dce118e817f06a80b1291` | same |
| `expressions.proto` | `2ce3849fda7d48546b1ef5ea2ba30cede6a6348582bbe6ef8b14df57ff25cb9a` | same |
| `ml.proto` | `0f5372140417fb4888075c52416ab0ebcce410c895ccc4f92c684f2dd10141b6` | same |
| `ml_common.proto` | `237628488246b193c2711d05b2657d640b5e044c4bcffd786dcd2ae56224e8ba` | same |
| `relations.proto` | `8a82ffc93dabb34fcef721dd14842e808280926745cc487d320d3f40367caab8` | same |
| `types.proto` | `78d41d565a51bbf826cfc427b891e5e84e3b9c80a3876500ffcaeea4204d8256` | same |

The following selected Python client files were checked by full SHA-256. `reattach.py`, `session.py`, `plan.py`, `expressions.py`, `types.py`, `base_pb2.py`, and `base_pb2_grpc.py` are unchanged. `client/core.py` is the relevant source difference: 4.0.5 changes Python accumulator metric decoding to use `specialAccumulatorSer`, catches `pickle.UnpicklingError`, logs a warning, and skips that metric. That path is outside this read-only range/local-relation subset; no selected API encoding or reattachment code changed. This is the recorded delta from the earlier source review, not an assumption that all Spark 4.0 behavior is identical.

| Python client source | v4.0.0 SHA-256 | v4.0.5 SHA-256 |
| --- | --- | --- |
| `python/pyspark/sql/connect/client/reattach.py` | `2af4b6832531bac0e7c962fe3805861a28bf9e818819e3fccfb5b2968af9e77b` | same |
| `python/pyspark/sql/connect/client/core.py` | `106fc46318ec7091976a4371c3fc873bb09028c583e8504ab87d768b2f8470d0` | `b8f03d91f750d21ae8b55c45e779a654e30c3a4ca28286b52567e935904c015c` |
| `python/pyspark/sql/connect/session.py` | `29833e979ac7d7536f742d2dd996096a11f82b19344bc22b1248e25c8e396946` | same |
| `python/pyspark/sql/connect/plan.py` | `8606df63892caee177c0c3ef64b72e8fdc9dfda0f1fe02b155f0ec12111bef20` | same |
| `python/pyspark/sql/connect/expressions.py` | `17cff8563ac019e1e4b759b7c381fca0ddcbcbba0184c1cc0acbfd55b88e8f8a` | same |
| `python/pyspark/sql/connect/types.py` | `fcc5ec112eedeb9858403daa2ae67ba04bc4511ab407d7de034a9279075e2362` | same |
| `python/pyspark/sql/connect/proto/base_pb2.py` | `7889781c8c5e6fbe6b1f72b7fb751696b037e587471f9e8f2cc08ef5ded1c8db` | same |
| `python/pyspark/sql/connect/proto/base_pb2_grpc.py` | `fbf329fe504a08f9200a2e52342bd09e175d1f37f3388180b86a8aa4ce7cfd23` | same |

## Service and stream requirements

The published service includes `ExecutePlan`, `AnalyzePlan`, `Config`, `Interrupt`, `ReattachExecute`, `ReleaseExecute`, `ReleaseSession`, `FetchErrorDetails`, `AddArtifacts`, and singular `ArtifactStatus`. Reattachment continues after `last_response_id`; an unavailable cursor is an error. `ResultComplete` distinguishes completed results from a stream that needs continuation. Release-until covers its named response inclusively; release-all also interrupts a running execution. [Spark 4.0.5 base.proto](https://github.com/apache/spark/blob/v4.0.5/sql/connect/common/src/main/protobuf/spark/connect/base.proto).

Response IDs are opaque UUID strings. Use a separate internal sequence for ordering; do not increment or sort UUIDs. The Python client defaults to reattachable execution, reads each `arrow_batch.data` with `pyarrow.ipc.open_stream`, and checks supplied start offsets and row counts. Each response therefore needs independently readable IPC framing; successive chunks of one long IPC stream are insufficient. [Client implementation](https://github.com/apache/spark/blob/v4.0.5/python/pyspark/sql/connect/client/core.py).

Produce a schema-bearing zero-row Arrow result for empty relational output and test it with the public client. The Arrow streaming format begins with a schema and supports subsequent record/dictionary messages. Sparkless-specific logical-schema metadata cannot replace the physical schema that PyArrow consumes. [Arrow streaming format](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format).

## Retry and release details that change our implementation

The reattachment iterator sends asynchronous prefix releases. It also retries the original `ExecutePlan` when reattachment reports a missing operation/session **before any response has been returned**. After a response has been returned, the same missing-handle path raises a client error. Test response loss before the first batch as well as after it; checking only mid-stream reconnect misses a duplicate-execution boundary. [Reattachment iterator](https://github.com/apache/spark/blob/v4.0.5/python/pyspark/sql/connect/client/reattach.py).

Apache's execution manager rejects a duplicate active operation ID with `INVALID_HANDLE.OPERATION_ALREADY_EXISTS`; abandoned-operation tombstones prevent a retry from executing abandoned work again. This differs from the HTTP mutation adapter's receipt-replay contract. Keep those transport behaviors distinct even when they share native accounting. Bounded history does not create a durable exactly-once guarantee. [Execution manager](https://github.com/apache/spark/blob/v4.0.5/sql/connect/server/src/main/scala/org/apache/spark/sql/connect/service/SparkConnectExecutionManager.scala).

The reference reattach handler distinguishes an abandoned operation from one never found. Its response observer can discard old sent responses and explicitly errors when their cursor is unavailable. Our epic's retention-until-release policy is a stricter product policy; it is not a universal Spark requirement. Define finite expiry and capacity failure without silently replacing a lost operation. [Reattach handler](https://github.com/apache/spark/blob/v4.0.5/sql/connect/server/src/main/scala/org/apache/spark/sql/connect/service/SparkConnectReattachExecuteHandler.scala), [Response observer](https://github.com/apache/spark/blob/v4.0.5/sql/connect/server/src/main/scala/org/apache/spark/sql/connect/execution/ExecuteResponseObserver.scala).

The reference release handler tolerates an operation already removed while the session exists. Test asynchronous prefix release arriving after release-all. The protobuf comment describes unknown release-until positions as no-ops, while the response observer's unknown-ID lookup can raise an error; record oracle behavior for unknown IDs rather than inferring it from comments alone. Safe cleanup is required regardless of which error is returned. [Release handler](https://github.com/apache/spark/blob/v4.0.5/sql/connect/server/src/main/scala/org/apache/spark/sql/connect/service/SparkConnectReleaseExecuteHandler.scala), [Response observer](https://github.com/apache/spark/blob/v4.0.5/sql/connect/server/src/main/scala/org/apache/spark/sql/connect/execution/ExecuteResponseObserver.scala).

Session release normally tombstones the user/session pair. The public request also has `allow_reconnect`, which the reference handler passes to session cleanup. Reconnection after release is therefore an explicit option, not always forbidden by the protocol. Our proposed subset rejects that option until separately supported; it must not silently ignore it or treat reconnect as restoration of old session state. [Session-release handler](https://github.com/apache/spark/blob/v4.0.5/sql/connect/server/src/main/scala/org/apache/spark/sql/connect/service/SparkConnectReleaseSessionHandler.scala), [ReleaseSessionRequest](https://github.com/apache/spark/blob/v4.0.5/sql/connect/common/src/main/protobuf/spark/connect/base.proto).

## Plans, configuration, and local data

Range, local relation, projection, filter, limit, and aggregate are public relation variants. Local relation accepts Arrow IPC data and an optional DDL/JSON schema; schema-only empty input is also possible. Cached local relations use a session-scoped hash. Supporting inline data alone does not implement that cached/artifact path. [Relations schema](https://github.com/apache/spark/blob/v4.0.5/sql/connect/common/src/main/protobuf/spark/connect/relations.proto).

The client constructs plans with plan IDs and can add wrappers such as column renames. String SQL expressions use `expression_string`; do not treat a string filter as a column name or forward an arbitrary expression to an unchecked parser. The pinned range, expression, projection, and action encodings are captured; native parser/translator behavior remains to be implemented and checked. [Plan encoder](https://github.com/apache/spark/blob/v4.0.5/python/pyspark/sql/connect/plan.py), [Expression encoder](https://github.com/apache/spark/blob/v4.0.5/python/pyspark/sql/connect/expressions.py).

`createDataFrame` asks for a batch of configuration values covering timestamps, timezone, inference, Arrow conversion, and local-relation caching. Spark 4.0.5 sets `spark.sql.session.localRelationCacheThreshold` to `64 * 1024 * 1024` bytes; the client switches to `CachedLocalRelation` when the table's native byte size reaches the effective threshold. Wire bytes and native bytes are different measures. Spark's `spark.sql.execution.arrow.useLargeVarTypes` defaults to false. `spark.sql()` uses a SQL command and command-result relation handling, so accepting a SQL relation alone does not establish that public API. [Session implementation](https://github.com/apache/spark/blob/v4.0.5/python/pyspark/sql/connect/session.py), [SQL configuration defaults](https://github.com/apache/spark/blob/v4.0.5/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala).

The Config protobuf defines get, get-with-default, optional-get, list, modifiability, set, and unset operations. In Spark 4.0.5's Python `RuntimeConf`, the public methods emit `Get`, `GetWithDefault`, `GetAll`, `IsModifiable`, `Set`, and `Unset`; `GetOption` is a protocol branch that this Python API does not expose. The reference implements absent optional values and caller defaults as normal outcomes. Rejecting unsupported settings must not turn valid missing/default queries into unconditional errors. Silent sets have warning behavior to test. [Python RuntimeConf](https://github.com/apache/spark/blob/v4.0.5/python/pyspark/sql/connect/conf.py), [Config handler](https://github.com/apache/spark/blob/v4.0.5/sql/connect/server/src/main/scala/org/apache/spark/sql/connect/service/SparkConnectConfigHandler.scala).

## Types, limits, and scope boundaries

The client has standard Spark type descriptors, including field metadata. Its Arrow conversion uses signed integer widths, decimal128, date32, microsecond timestamps, and distinct timestamp/NTZ handling; arrays/maps carry child nullability. Use these physical mappings and the public decoded values in tests. Native schema-sidecar fidelity alone cannot establish Connect value support. [Connect type conversion](https://github.com/apache/spark/blob/v4.0.5/python/pyspark/sql/connect/types.py), [Spark/Arrow mappings](https://github.com/apache/spark/blob/v4.0.5/python/pyspark/sql/pandas/types.py).

Apache documents port 15002, an estimated Arrow batch budget, inbound message limits, metadata limits, and optional enriched error retrieval. These are Spark implementation settings, not fixed wire-format limits. Our replay bytes, deadlines, local-data limits, and stricter measured response caps must be labeled as Sparkless policies. [Spark 4.0.5 configuration](https://spark.apache.org/docs/4.0.5/configuration.html#spark-connect).

Spark Connect supports UDF and much of the streaming API, while SparkContext/RDD are unsupported. Excluding UDFs, Structured Streaming, joins, distributed facilities, artifacts, or extra types from 4.18 is our declared implementation scope. Arrow result streaming does not imply Structured Streaming support. Connect does not supply built-in authentication; trusted loopback or separately tested proxy authentication is a deployment contract. The emulator environment variables are project integration settings. [Apache supported surface and authentication](https://spark.apache.org/docs/4.0.5/spark-connect-overview.html#what-is-supported).

## Evidence still required

Source inspection establishes that the selected subset uses public protocol constructs. It does not prove our implementation interoperates. Run the unmodified pinned client with default retries/reattachment, compare enabled behavior with the matched Spark oracle, and certify the emulator separately. Resolve the source/comment cursor discrepancy, preserve optional configuration behavior, test before-first-response retry, and verify physical Arrow decoding in V18–V25. No gate is passed by this research.
