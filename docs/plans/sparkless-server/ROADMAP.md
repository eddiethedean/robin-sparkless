---
search:
  exclude: true
---

> **Archive / historical record:** this page describes a past snapshot or a plan.
> Use the [current support contract](../../SUPPORT_MATRIX.md) for release behavior.

# sparkless-server release roadmap: 4.16+

**Status: planned.** Each minor release below publishes the complete first-party package set at the same version: **4.16 → 4.16.0, 4.17 → 4.17.0, 4.18 → 4.18.0**, and so forth. These are scope and acceptance milestones, not calendar commitments or announcements of available features.

[Plan index](README.md) · [Epic](EPIC.md) · [Decisions](DECISIONS.md) · [Validation](VALIDATION.md) · [Release procedure](../../RELEASING.md)

**Standalone development policy (2026-10-25):** Sparkless milestones, CI, and release readiness depend on its own native/HTTP/Connect contracts, standard clients, and self-contained fixtures. Work in external applications is managed separately; their features, maintainer decisions, and merge timing cannot block Sparkless.

## Coordinated version and publication policy

Starting with **4.16.0**, every release publishes all first-party packages in the release inventory, including packages whose implementation did not change. All package manifests, first-party dependency requirements, lockfile entries, Python runtime version metadata, binary versions, and release artifact tags must agree on the release version.

| Package / artifact | 4.16.0 publication | Later releases |
| --- | --- | --- |
| `spark-sql-parser` | Publish crate at **4.16.0**, moving from its independent 0.x track | Publish at the shared release version |
| `robin-sparkless-core` | Publish crate at **4.16.0** | Publish at the shared release version |
| `robin-sparkless-polars` | Publish crate at **4.16.0** | Publish at the shared release version |
| `robin-sparkless` | Publish crate at **4.16.0** | Publish at the shared release version |
| `sparkless-server` | Introduce and publish crate, CLI binaries, and container at **4.16.0** | Publish at the shared release version |
| `sparkless` | Publish Python wheels/sdist at **4.16.0**, including the launcher | Publish at the shared release version |
| `sparkless-native` | Set the native extension crate to **4.16.0** and ship it inside the matching `sparkless` wheels | Keep its manifest and embedded extension aligned; a separate crates.io distribution is not required by the current packaging model |
| Server binary Python distribution, proposed `sparkless-server-bin` | Introduce platform wheels at **4.16.0** for the `sparkless[server]` extra | Publish at the shared release version |

New first-party packages introduced by later phases join this policy in their first release. Optional features remain opt-in even though their packages share the release version. Third-party clients, Spark protocol sources, and reference engines keep their own pinned versions; they do not adopt the project's release number.

The launcher control protocol, HTTP envelope, and Spark Connect protocol versions remain independent compatibility identifiers. A binary reporting version 4.18.0 does not imply Spark protocol version 4.18.

Use release tags `v4.16.0`, `v4.17.0`, and so on. If a maintenance patch is issued, use the same coordinated policy for that release, for example all first-party packages at **4.16.1**. Do not republish or overwrite an existing package version or tag.

This policy supersedes the earlier exception allowing `spark-sql-parser` to remain on its own version. Updating the release validator, dependency requirements, and publication workflows is a prerequisite for 4.16.0; this roadmap does not itself bump manifests or publish packages.

## Release map

| Release | Shared package version | Epic phase | Deliverable |
| --- | --- | --- | --- |
| **4.16** | **4.16.0** | 0A + 0B | Native read-only HTTP SQL service, exact typed HTTP results, bounded runtime, sync/async launcher, and complete package distribution |
| **4.17** | **4.17.0** | 1 | Shared catalog ownership, controlled mutations, richer logical schemas, and lazy native execution improvements |
| **4.18** | **4.18.0** | 2 | Supported JVM-free Spark Connect, typed Arrow streaming, replay/release lifecycle, and standalone unmodified-client conformance |
| **4.19** | **4.19.0** | 3, optional capability | DuckDB data source: typed Arrow table scans feeding the existing Polars engine; optional `SparklessConnect` context manager |
| **4.20** | **4.20.0** | 4, optional capability | DuckDB execution backend exposed through Sparkless's supported Connect server |
| **4.21** | **4.21.0** | 5, optional capability | Isolated Python Jobs/notebooks and scalar/Pandas UDF workers |
| **4.22** | **4.22.0** | 6, optional capability | Native Delta Lake / Unity Catalog interoperability |
| **4.23** | **4.23.0** | 7 | Expanded Connect DataFrame operations, scalar/nested types, analysis, and session configuration |
| **4.24** | **4.24.0** | 8, connectors opt-in | Connect CSV/JSON/Parquet and optional JDBC I/O, artifacts, caching, and DataFrame checkpoints |
| **4.25** | **4.25.0** | 9, optional capability | Durable operation outcomes, replay, and eligible session recovery across restart |
| **4.26** | **4.26.0** | 10, optional capability | Single-node Structured Streaming, state/checkpoints, and verified sink recovery |
| **4.27** | **4.27.0** | 11 | `sparkless-connect40`: certify the complete planned PySpark 4.0 server feature baseline after Phases 3–10 |
| **4.28** | **4.28.0** | 12 | `sparkless-connect35`: separate Spark 3.5 server compatibility profile and package |
| **4.29** | **4.29.0** | 13 | `sparkless-connect41`: Spark 4.1 server feature deltas, profile and package |
| **4.30** | **4.30.0** | 14 | `sparkless-connect42`: Spark 4.2 server feature deltas, profile and package |
| **4.31+** | **4.N.0** for release 4.N | Follow-on scope to be defined | Further conformance and measured performance improvements |

All rows are planned. Release scope is cumulative: each release retains and regression-tests previously shipped capabilities. Later scope cannot bypass an unfinished prerequisite. If an optional phase is deferred or reordered, explicitly revise this release map and its gates before assigning different work to that version. The two DuckDB phases are explicitly sequenced after supported Connect: source integration first, execution backend second. Python workers and Delta/UC move to Phases 5/6; neither has an inherent technical dependency on DuckDB, and Delta/UC does not require Python workers. Reordering requires an explicit roadmap revision, not an incidental dependency in the implementation.

**Scope extension (2026-10-29):** formerly unassigned Connect gaps are now scheduled below. The proposed feature-level choices and exclusions for Phases 4–10 are collected in [SS-20](20-connect-feature-scope.md); each phase decision register must freeze its matrix before implementation. The release numbers are scope targets without calendar commitments. Phases 7–10 retain native single-node execution; RDD/SparkContext, distributed executors, and universal Spark compatibility remain outside the roadmap. Optional workers/connectors/storage are required only for the surfaces that use them, not for the default native API.

## 4.16.0 — HTTP service, launcher, and unified distribution

Complete **Phase 0A before the Phase 0B release gate**. The early Connect experiment informs the architecture but remains experimental; it is not advertised as supported Spark Connect in 4.16.0.

The [detailed 4.16 implementation plan](RELEASE_4_16.md) specifies the SQL/type subset, proposed limits, ownership/control behavior, package-installation checks, and ordered delivery slices.

### Delivery sequence

| Slices | Result | Dependency to advance |
| --- | --- | --- |
| 416-01–416-03 | Frozen contracts, minimal crate, real Connect feasibility | Unmodified-client compute/empty/reconnect/release evidence; typed HTTP contracts fixed |
| 416-04–416-05 | Bounded execution/session ownership and exact scalar/SQL path | Native ownership, limits, logical schema, positional values, and allowlist checks |
| 416-06–416-07 | HTTP agent and sync/async owned-process launcher | Direct-agent plus lifecycle/platform gates |
| 416-08 | Direct HTTP/launcher checks and complete candidate distribution | V01–V14 and V29, all packages prepared at 4.16.0 |
| 416-09 | Coordinated publication | Fresh public installation/artifact verification in V30 |

Runtime/schema and packaging work can proceed independently after contracts/scaffolding; readiness requires their integrated native/HTTP/launcher evidence. Public support remains HTTP-only even though the early native Connect experiment is required.

### Scope

- Lock client/protocol/reference test versions and SQL semantics; prove exact typed HTTP mapping and BIGINT preservation with in-repository fixtures.
- Add the native server workspace crate, CLI, feature boundaries, tested binaries, and Python/JVM-free container.
- Implement read-only HTTP SQL, typed positional results, readiness/capabilities, explicit unsupported errors, and minimum session ownership/TTL/close behavior.
- Include bounded admission, queues, result production, cooperative cancellation accounting, and graceful shutdown.
- Repair scalar schema mappings required for exact HTTP results.
- Ship sync/async `SparklessServer` lifecycle and the precompiled binary extra. `http_url` is available after readiness; `spark_connect_url` rejects unsupported use.
- Publish **every package in the release inventory at 4.16.0** and verify fresh installations from the published artifacts.

Primary plans: [SS-01](01-compatibility-and-emulator.md), [SS-02](02-crate-cli-and-distribution.md), [SS-03](03-connect-feasibility.md), [SS-04](04-runtime-and-operations.md), [SS-05](05-http-sql-and-results.md), [SS-06](06-python-launcher.md), the early slices of [SS-07](07-sessions-catalogs-and-mutations.md) and [SS-08](08-native-types-and-lazy-execution.md), and [SS-11](11-proxy-benchmarks-and-release.md).

### Release gate

- [ ] Validation **V01–V14** passes, including native/direct HTTP result and schema checks.
- [ ] Direct HTTP and packaged consumer tests preserve `9007199254740993`, high-precision decimals, typed nulls, empty-result schema, and declared date/time semantics.
- [ ] Launcher failure, cancellation, multiple-instance, restart, and cleanup tests pass on every advertised platform.
- [ ] Timed-out native work retains its admission permit until it actually finishes; health traffic progresses under load.
- [ ] Metadata checks cover the parser, server, binary Python distribution, existing Rust packages, Python package, and native extension.
- [ ] **V29** verifies the complete 4.16.0 candidate inventory and isolated consumer installs without inheriting root Cargo patches or local paths.
- [ ] The candidate passes all pre-publication gates before tagging; publication verification is a separate completion step.
- [ ] All artifacts are published and independently verified at **4.16.0**. A passing direct-agent test or partially published package set does not complete this release.
- [ ] **V30** verifies fresh public installs, binary versions, image digests, and the complete release inventory after publication.

Remote mutations, arbitrary Python, and production Spark Connect remain unsupported in this release.

## 4.17.0 — Shared catalogs and native execution foundations

Build on the 4.16.0 service with **Phase 1** capabilities, then publish all packages again at **4.17.0**.

The [detailed 4.17 implementation plan](RELEASE_4_17.md) specifies the ephemeral catalog, statement snapshots, mutation allowlist, HTTP identity/status extension, bounded outcome retention, native type matrix, and lazy execution evidence. The completed release state and public verification evidence are recorded in that plan and the [validation log](VALIDATION.md#4170-final-public-release-verification-2026-10-28).

### Delivery sequence

| Slices | Result | Dependency to advance |
| --- | --- | --- |
| 417-01–417-02 | Frozen Phase 1 contracts, shared native catalog, isolated session state | V15 visibility/close/TTL/restart and snapshot ownership evidence |
| 417-03–417-04 | Atomic coordinated mutations and native/direct-HTTP identity/outcome integration | V16 concurrency, duplicate/disconnect/commit/restart evidence |
| 417-05–417-06 | Logical-schema repairs and lazy range/count/planning | Phase 1 V18 type/schema matrix and V17 allocation/scheduler evidence |
| 417-07 | Integrated candidate and complete 4.17.0 distribution | Prior applicable gates, V15–V18, and V31 |
| 417-08 | Coordinated publication | V32 fresh public installation/artifact verification |

### Scope

- Separate shared saved tables/database metadata from session-local temporary views/configuration.
- Implement declared catalog lifetime and persistence behavior, including close and restart semantics. The proposed baseline survives session close/TTL within one server instance and starts empty on restart; durability remains unadvertised.
- Enable only the mutations whose coordination, visibility, operation identity, retry, and commit-outcome contracts pass their tests.
- Extend logical type/nullability metadata and the native schema path; reject unsupported mappings explicitly.
- Improve lazy range/count and expensive planning behavior under the compute scheduler.

Primary plans: [SS-07](07-sessions-catalogs-and-mutations.md), [SS-08](08-native-types-and-lazy-execution.md), and accounting/release extensions in [SS-04](04-runtime-and-operations.md) and [SS-11](11-proxy-benchmarks-and-release.md).

### Release gate

- [x] 4.16 is complete, its applicable regression gates still pass, and **V15–V18** pass for the enabled Phase 1 surface; production Connect checks remain in 4.18.
- [x] Concurrent mutation, duplicate operation, disconnect near commit, and restart tests match the documented guarantees.
- [x] Versioned HTTP identity/status preserves outcomes through the complete native/direct-HTTP matrix; legacy requests remain read-only.
- [x] Catalog objects survive session close according to policy; temporary state remains isolated.
- [x] Schema fidelity and lazy execution are demonstrated, with intermediate-memory measurements.
- [x] **V31** verified every 4.17.0 candidate artifact and isolated consumer installation before tagging/publication.
- [x] All packages/artifacts are published and verified at **4.17.0**; post-publication checks passed through guarded recovery, with the original tag-run V32 job skipped (see [validation evidence](VALIDATION.md#4170-final-public-release-verification-2026-10-28)).

## 4.18.0 — Supported Spark Connect and Arrow execution

Complete **Phase 2**, with standalone client conformance, then publish all packages at **4.18.0**.

The [detailed 4.18 implementation plan](RELEASE_4_18.md) freezes the Spark 4.0.5 client/protocol baseline, public API/RPC/node/type/config matrix, shared execution boundary, physical Arrow payloads, replay/session lifecycle, resource budgets, and candidate/publication gates. Its [public-contract research](CONNECT_PROTOCOL_RESEARCH.md) compares the 4.0.5 source with the earlier 4.0.0 review. Actual interoperability and release evidence remain required.

### Delivery sequence

| Slices | Result | Dependency to advance |
| --- | --- | --- |
| 418-01–418-02 | Frozen public contracts, generated service, bounded session/admission boundary | Actual client fixtures and exact compatibility pins; prior native interfaces |
| 418-03–418-05 | Native plans/analysis/config, physical Arrow and replay/release lifecycle | Joint client/oracle and fault/capacity evidence; no independent compatibility claim |
| 418-06–418-07 | Standalone conformance and packaged launcher endpoint | V18–V24 plus applicable V17; unmodified default client |
| 418-08 | Separate proxy certification and concurrent-client benchmarks | Standalone conformance first; V25/V26 with pinned proxy |
| 418-09 | Complete integrated 4.18.0 candidate | Prior releases complete, applicable regressions and V33 |
| 418-10 | Coordinated publication | V34 fresh public-artifact verification |

### Scope

- Implement the supported relations, expressions, analysis/configuration, and actions of the pinned unmodified client.
- Stream correct Arrow schemas/batches, including empty results and bounded local-relation decoding.
- Support stable session/operation/response identity, completion markers, bounded replay, reattachment, release, interruption, expiry, and restart behavior.
- Enable `spark_connect_url` only for a ready, supported endpoint.
- Certify optional third-party routes separately; external changes or acceptance do not gate standalone Connect.

Primary plans: [SS-09](09-connect-plans-and-analysis.md), [SS-10](10-arrow-replay-and-connect-conformance.md), and endpoint/release work in [SS-06](06-python-launcher.md) and [SS-11](11-proxy-benchmarks-and-release.md).

### Release gate

- [ ] Prior supported surfaces still pass; **V18–V24** pass for the published standalone Connect matrix. V25 is supplemental for optional third-party routes.
- [ ] The unmodified JVM-free client computes `spark.range(10).filter("id > 5").count() == 4` through the native engine, alongside the complete declared conformance suite.
- [ ] Reconnection after delivered batches does not rerun the plan; release/expiry/capacity-pressure tests demonstrate bounded retention.
- [ ] Multiple real client processes produce independent correct results while health traffic progresses.
- [ ] **V26** records reproducible concurrent-client benchmarks and limitations, without unmeasured speedup claims.
- [ ] **V33** verifies the complete 4.18.0 candidate inventory, isolated installs, earlier regressions, and standalone/proxied Connect from packaged artifacts before tagging/publication.
- [ ] All packages/artifacts are published and verified at **4.18.0**.
- [ ] **V34** verifies fresh exact-version public installs, artifact hashes/digests, both supported transports, proxy route, and launcher/reconnect/release behavior after publication.

## 4.19.0 — Optional DuckDB data source and SparklessConnect

Complete **Phase 3**, after the supported Phase 2 Connect server. DuckDB supplies table data through native Arrow batches; the existing Polars backend performs the Sparkless query. This is a read-only source capability, not a new execution engine or a DuckDB write path.

Primary plans: [SS-14: DuckDB data source](14-duckdb-data-source.md), with ordered slices 419-01–419-05, and [SS-06: SparklessConnect](06-python-launcher.md#4190-optional-sparklessconnect-context-manager), with slices 419-06–419-08.

Add an optional `SparklessConnect` context manager that starts `SparklessServer`, yields a real `SparkSession` from pyspark-client, and stops the session and server on exit. Package pyspark-client as an optional dependency through the proposed `sparkless[connect]` extra; the helper is independent of DuckDB.

- Freeze **D14**: DuckDB library/driver pins, database ownership, names, source snapshots, schema mappings, Arrow batches, and measured resource budgets.
- Extend the engine-neutral external catalog contract with bounded scan/lifetime handling; current Delta descriptors and Arrow write streams are not sufficient by themselves.
- Support configured named tables, exact typed/empty results, and the declared filters/projections/joins/aggregates through unmodified Connect clients. Source writes fail explicitly.
- Select and prove standalone read-only access and a live shared-owner path for independently written DuckDB tables. Sharing database files between independent writable processes is not a supported shortcut; no export or synchronization is required in the selected live-owner mode.

### Release gate

- [ ] Phase 2 is complete; existing native/HTTP/Connect surfaces still pass.
- [ ] SS-06 slices **419-06–419-08** pass: real Connect session, optional dependency isolation, normal/error cleanup, session ownership, and fresh packaged-install checks.
- [ ] **V35** proves real DuckDB scans, enabled Sparkless computations through Polars, exact schemas/values, and unsupported-type/write rejection.
- [ ] **V36** proves multi-table snapshots, concurrent source changes, cancellation/disconnect/reattach, ownership cleanup, source failures, and numeric memory/queue limits. Independent DuckDB writer visibility uses one database owner.
- [ ] Native enabled/disabled builds and fresh packaged installs pass on advertised platforms; default dependencies remain lightweight and Python/JVM-free.
- [ ] All first-party packages/artifacts publish and pass fresh public verification at coordinated **4.19.0** after the complete phase exits pass. A standalone DuckDB file scan does not certify live shared-owner access.

## 4.20.0 — Optional DuckDB execution backend for Connect

Complete **Phase 4** after the Phase 3 source/ownership foundations. DuckDB now executes the advertised SQL/DataFrame plans and owns persistent table writes behind the existing Sparkless Connect endpoint. Polars remains the default backend; engine selection is explicit and fixed for a server instance's operations.

Primary plan: [SS-15: DuckDB Connect backend](15-duckdb-connect-backend.md), with ordered slices 420-01–420-05.

- Freeze **D15**: backend selection/interfaces, Spark semantic translation, supported SQL/plans/configuration, analysis/result schemas, transactional outcomes, restart behavior, and resource accounting.
- Reuse the supported Connect transport/session/replay infrastructure; prove it against the DuckDB backend independently of Polars.
- Translate supported Spark semantics deliberately. DuckDB SQL acceptance alone does not establish Spark compatibility, and unsupported operations never silently select another engine.
- Enable only enumerated writes with verified transaction/receipt behavior. Durable DuckDB data survives independently of the original ephemeral Polars catalog; no durable replay guarantee is assumed.
- Verify independent DuckDB SQL and Sparkless Connect clients read/write the same database through a shared owner and generic configuration, using self-contained fixtures.

### Release gate

- [ ] Phase 3 is complete; earlier Polars and optional source capabilities retain their tested behavior.
- [ ] **V37** proves the declared SQL/DataFrame/analysis/type matrix through unmodified Connect clients, pinned reference semantics, and explicit unsupported results.
- [ ] **V38** proves truthful transactional effects/receipts, duplicate/conflicting identities, deadline/commit/disconnect/cancel/restart cases, independent client read/write visibility, and numeric multi-client resource budgets.
- [ ] Applicable V19–V24 lifecycle checks pass for DuckDB; V25 remains supplemental for any advertised third-party route. Reattach must not reexecute a query or mutation.
- [ ] Optional dependency/build/platform boundaries, clean enabled/default installs, and public artifact verification pass at coordinated **4.20.0**. Neither server runtime requires Python or a JVM.

## 4.21.0 — Optional isolated Python workers

Target **Phase 5** as an explicitly enabled capability. Republish the complete existing package set and any new first-party worker distribution at **4.21.0**.

Primary plan: [SS-12](12-optional-python-workers.md).

- [ ] Resolve the worker security, packaging, API, and resource contract before enabling execution.
- [ ] Support only the declared Jobs/notebook subset with separate run/session identity, bounded output, truthful errors, and deterministic process-tree cleanup.
- [ ] Add scalar Python and scalar/grouped/iterator Pandas UDF forms under an explicit Connect API/type matrix; use separate workers and bounded Arrow batches, with no Python inside the native server.
- [ ] Pass **V27**, including Jobs/notebooks, UDF value/schema semantics, isolation, secret handling, quota/failure cases, and disabled-feature tests.
- [ ] Preserve Python/JVM-free core SQL/Connect images and rerun their regression gates.
- [ ] Publish and verify all participating packages at **4.21.0**; installation of optional worker support remains opt-in.

## 4.22.0 — Optional native Delta Lake / Unity Catalog

Target **Phase 6** using native storage/catalog integration. Publish the complete package set, including any new first-party integration distributions, at **4.22.0**.

Primary plan: [SS-13](13-optional-delta-and-unity-catalog.md).

- [ ] Pin native storage/catalog dependencies and document supported operations, credentials, conflict handling, and recovery semantics.
- [ ] Pass **V28** using an independent reader for schema, decimal precision, data files, and `_delta_log` transactions.
- [ ] Verify concurrent writers, cancellation/failure near commit, and explicit unsupported DDL/DML errors.
- [ ] Preserve previously shipped capabilities and keep Delta/UC optional; no Python worker dependency is introduced.
- [ ] Publish and verify all participating packages at **4.22.0**.

## 4.23.0 — Expanded Connect API, types, and analysis

Complete **Phase 7 / [SS-16](16-connect-api-types-and-analysis.md)** for the default Polars backend and publish backend-specific support matrices. Earlier DuckDB support does not establish default-backend parity.

- Freeze **D16** for comparisons/boolean/null predicates, arithmetic/casts/functions, column APIs, joins/grouping/distinct aggregates, sorting/windows, and union/intersect/except semantics.
- Enable BYTE, FLOAT, BINARY, TIMESTAMP_NTZ, arrays/maps/nested structs across local input, analysis, execution, and Arrow results.
- Add truthful explain/analysis and explicitly mutable session settings; unsupported settings/functions remain named errors.
- Pass **V39/V40** against the pinned unmodified client/reference, including nested/null/empty/boundary cases, expensive-operator resource budgets, session isolation, and earlier lifecycle regressions.
- Verify the complete candidate and fresh public artifacts at coordinated **4.23.0**. Native API coverage does not require optional Python, Delta, or JDBC deployments.

## 4.24.0 — Connect I/O, artifacts, caching, and checkpoints

Complete **Phase 8 / [SS-17](17-connect-sources-artifacts-and-persistence.md)** after the required Phase 7 API/type work.

- Freeze **D17** for CSV/JSON/Parquet and optional JDBC read/write APIs, source/sink options, catalog/view APIs, credentials/path policy, commit outcomes, and ownership.
- Implement bounded artifact upload/status and cached local relations using actual public-client threshold behavior.
- Implement cache/persist/unpersist, truthful storage status, and local/durable DataFrame checkpoints with explicit memory/disk/lifetime rules.
- Keep the core server Python/JVM-free; actual JDBC drivers may run only in a separately packaged optional connector process. Delta public APIs require the SS-13 capability and their own integration checks.
- Pass **V41–V43**, optional/default installs, existing regressions, and coordinated **4.24.0** candidate/public verification. Batch checkpoints do not imply durable operation replay or streaming support.

## 4.25.0 — Durable Connect recovery

Complete **Phase 9 / [SS-18](18-durable-connect-recovery.md)** as an optional persistent mode for explicitly certified backends/sinks.

- Freeze **D18** for durable receipts/response spools, eligible session state, generation fencing, reconnect identity, `allow_reconnect=true`, retention, and format migration.
- Coordinate data effects and receipts transactionally or reject automatic retries for unresolved outcomes. Reattachment must never silently rerun a lost producer.
- Pass **V44/V45** with crashes across commit/receipt/delivery boundaries, partial response recovery, expiry, corruption, disk exhaustion, and incompatible restarts.
- Verify persistent and ephemeral modes and publish the exact per-backend guarantees at coordinated **4.25.0**. Durable tables alone do not pass this phase; distributed failover is excluded.

## 4.26.0 — Optional single-node Structured Streaming

Complete **Phase 10 / [SS-19](19-structured-streaming.md)** using the enabled API, sources/sinks, and durable recovery contracts from Phases 7–9.

- Freeze **D19** for the `readStream`/`writeStream`/query-management API matrix, rate/file sources, approved sinks, triggers/output modes, offsets, state, and checkpoint/sink commits.
- Implement micro-batches, query progress/await/stop, event-time watermarks, deduplication, and declared stateful windowed aggregates.
- Pass **V46/V47** against the pinned reference, including late data, restart/crash recovery, per-sink delivery guarantees, backpressure, bounded long-running state/disk use, and cancellation/shutdown.
- Verify optional/default packages and coordinated **4.26.0** candidate/public artifacts. Continuous processing, distributed execution, and arbitrary connectors are outside the initial streaming scope.

## 4.27.0–4.30.0 — One server compatibility phase per Spark version

Follow [SS-21: versioned compatibility packages](21-versioned-connect-packages.md). Complete all planned server features in Phases 3–10 before Phase 11 certifies `sparkless-connect40` at 4.27.0. Then deliver one phase each for `sparkless-connect35` (Phase 12 / 4.28), `sparkless-connect41` (Phase 13 / 4.29), and `sparkless-connect42` (Phase 14 / 4.30). The owner confirmed 3.5, 4.0, 4.1 and 4.2 as the upstream targets; 3.6–3.9 are not released Spark lines.

Each package pins a tested upstream client patch and the exact matching Sparkless launcher/native binary, selecting an explicit server semantic/protocol profile. Complete the applicable feature matrix and differential/fault tests before certification. Shared code and earlier profiles must retain their behavior. Resolve **D20** for each line, then pass **V48/V49**, **V50/V51**, **V52/V53**, and **V54/V55**, respectively, including coordinated candidate and fresh public installs. Optional workers/backends/connectors stay opt-in; universal Apache Spark parity remains outside scope.

## Publication checklist for every release

1. Freeze the release inventory and implemented scope; record passing validation evidence and immutable source/client/emulator revisions.
2. Set all first-party package versions to the selected `4.N.0` (or coordinated patch version), update internal dependency requirements and lockfiles, and align runtime/binary metadata.
3. Build and test packaged artifacts in clean environments. Validate the combined release candidate, not only individual change branches.
4. Publish Rust packages in dependency order: parser/core before Polars, then facade, then server. Build the native Python extension from the same candidate and ship it in the matching `sparkless` distribution.
5. Publish compatible server-binary Python wheels before advertising/installing the matching `sparkless[server]` extra. Publish all declared binaries and container tags from the same candidate, with checksums/digests.
6. Verify fresh Cargo/Python installs and native binary/container execution from the published artifacts, with exact package versions checked.
7. Mark the release complete only when the whole inventory and gates pass. Registries do not publish atomically: resume a partial release only for missing verified artifacts; never overwrite existing versions. If published contents need correction, use a new coordinated patch release.

Before 4.16.0, extend the existing release version validator and CI workflows to enforce this inventory and dependency order. Record that implementation in [SS-02](02-crate-cli-and-distribution.md), [SS-06](06-python-launcher.md), and [SS-11](11-proxy-benchmarks-and-release.md). Keep platform and compatibility choices in [D05](DECISIONS.md); the shared package-version policy is fixed by this roadmap.
