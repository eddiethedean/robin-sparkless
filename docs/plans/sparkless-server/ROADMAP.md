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
| **4.19** | **4.19.0** | 3, optional capability | DuckDB data source: typed Arrow table scans feeding the existing Polars engine |
| **4.20** | **4.20.0** | 4, optional capability | DuckDB execution backend exposed through Sparkless's supported Connect server |
| **4.21** | **4.21.0** | 5, optional capability | Isolated Python worker support for the declared Jobs/notebook subset |
| **4.22** | **4.22.0** | 6, optional capability | Native Delta Lake / Unity Catalog interoperability |
| **4.23+** | **4.N.0** for release 4.N | Follow-on scope to be defined | Additional conformance, supported types/operations, and measured performance improvements |

All rows are planned. Release scope is cumulative: each release retains and regression-tests previously shipped capabilities. Later scope cannot bypass an unfinished prerequisite. If an optional phase is deferred or reordered, explicitly revise this release map and its gates before assigning different work to that version. The two DuckDB phases are explicitly sequenced after supported Connect: source integration first, execution backend second. Python workers and Delta/UC move to Phases 5/6; neither has an inherent technical dependency on DuckDB, and Delta/UC does not require Python workers. Reordering requires an explicit roadmap revision, not an incidental dependency in the implementation.

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

The [detailed 4.17 implementation plan](RELEASE_4_17.md) specifies the proposed ephemeral catalog, statement snapshots, initial mutation allowlist, HTTP identity/status extension, bounded outcome retention, native type matrix, and lazy execution evidence. These are implementation baselines to verify, not completed capabilities.

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

- [ ] 4.16 is complete, its applicable regression gates still pass, and **V15–V18** pass for the enabled Phase 1 surface; production Connect checks remain in 4.18.
- [ ] Concurrent mutation, duplicate operation, disconnect near commit, and restart tests match the documented guarantees.
- [ ] Versioned HTTP identity/status preserves outcomes through the complete native/direct-HTTP matrix; legacy requests remain read-only.
- [ ] Catalog objects survive session close according to policy; temporary state remains isolated.
- [ ] Schema fidelity and lazy execution are demonstrated, with intermediate-memory measurements.
- [ ] **V31** verifies every 4.17.0 candidate artifact and isolated consumer installation before tagging/publication.
- [ ] All packages/artifacts are published and verified at **4.17.0** through **V32** fresh public installs.

## 4.18.0 — Supported Spark Connect and Arrow execution

Complete **Phase 2**, with standalone client conformance, then publish all packages at **4.18.0**.

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
- [ ] All packages/artifacts are published and verified at **4.18.0**.

## 4.19.0 — Optional DuckDB data source

Complete **Phase 3**, after the supported Phase 2 Connect server. DuckDB supplies table data through native Arrow batches; the existing Polars backend performs the Sparkless query. This is a read-only source capability, not a new execution engine or a DuckDB write path.

Primary plan: [SS-14: DuckDB data source](14-duckdb-data-source.md), with ordered slices 419-01–419-05.

- Freeze **D14**: DuckDB library/driver pins, database ownership, names, source snapshots, schema mappings, Arrow batches, and measured resource budgets.
- Extend the engine-neutral external catalog contract with bounded scan/lifetime handling; current Delta descriptors and Arrow write streams are not sufficient by themselves.
- Support configured named tables, exact typed/empty results, and the declared filters/projections/joins/aggregates through unmodified Connect clients. Source writes fail explicitly.
- Select and prove standalone read-only access and a live shared-owner path for independently written DuckDB tables. Sharing database files between independent writable processes is not a supported shortcut; no export or synchronization is required in the selected live-owner mode.

### Release gate

- [ ] Phase 2 is complete; existing native/HTTP/Connect surfaces still pass.
- [ ] **V33** proves real DuckDB scans, enabled Sparkless computations through Polars, exact schemas/values, and unsupported-type/write rejection.
- [ ] **V34** proves multi-table snapshots, concurrent source changes, cancellation/disconnect/reattach, ownership cleanup, source failures, and numeric memory/queue limits. Independent DuckDB writer visibility uses one database owner.
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
- [ ] **V35** proves the declared SQL/DataFrame/analysis/type matrix through unmodified Connect clients, pinned reference semantics, and explicit unsupported results.
- [ ] **V36** proves truthful transactional effects/receipts, duplicate/conflicting identities, deadline/commit/disconnect/cancel/restart cases, independent client read/write visibility, and numeric multi-client resource budgets.
- [ ] Applicable V19–V24 lifecycle checks pass for DuckDB; V25 remains supplemental for any advertised third-party route. Reattach must not reexecute a query or mutation.
- [ ] Optional dependency/build/platform boundaries, clean enabled/default installs, and public artifact verification pass at coordinated **4.20.0**. Neither server runtime requires Python or a JVM.

## 4.21.0 — Optional isolated Python workers

Target **Phase 5** as an explicitly enabled capability. Republish the complete existing package set and any new first-party worker distribution at **4.21.0**.

Primary plan: [SS-12](12-optional-python-workers.md).

- [ ] Resolve the worker security, packaging, API, and resource contract before enabling execution.
- [ ] Support only the declared Jobs/notebook subset with separate run/session identity, bounded output, truthful errors, and deterministic process-tree cleanup.
- [ ] Pass **V27**, including isolation, secret handling, quota/failure cases, and disabled-feature tests.
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

## Publication checklist for every release

1. Freeze the release inventory and implemented scope; record passing validation evidence and immutable source/client/emulator revisions.
2. Set all first-party package versions to the selected `4.N.0` (or coordinated patch version), update internal dependency requirements and lockfiles, and align runtime/binary metadata.
3. Build and test packaged artifacts in clean environments. Validate the combined release candidate, not only individual change branches.
4. Publish Rust packages in dependency order: parser/core before Polars, then facade, then server. Build the native Python extension from the same candidate and ship it in the matching `sparkless` distribution.
5. Publish compatible server-binary Python wheels before advertising/installing the matching `sparkless[server]` extra. Publish all declared binaries and container tags from the same candidate, with checksums/digests.
6. Verify fresh Cargo/Python installs and native binary/container execution from the published artifacts, with exact package versions checked.
7. Mark the release complete only when the whole inventory and gates pass. Registries do not publish atomically: resume a partial release only for missing verified artifacts; never overwrite existing versions. If published contents need correction, use a new coordinated patch release.

Before 4.16.0, extend the existing release version validator and CI workflows to enforce this inventory and dependency order. Record that implementation in [SS-02](02-crate-cli-and-distribution.md), [SS-06](06-python-launcher.md), and [SS-11](11-proxy-benchmarks-and-release.md). Keep platform and compatibility choices in [D05](DECISIONS.md); the shared package-version policy is fixed by this roadmap.
