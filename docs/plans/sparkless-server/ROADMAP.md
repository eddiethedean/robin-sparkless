# sparkless-server release roadmap: 4.16+

**Status: planned.** Each minor release below publishes the complete first-party package set at the same version: **4.16 → 4.16.0, 4.17 → 4.17.0, 4.18 → 4.18.0**, and so forth. These are scope and acceptance milestones, not calendar commitments or announcements of available features.

[Plan index](README.md) · [Epic](EPIC.md) · [Decisions](DECISIONS.md) · [Validation](VALIDATION.md) · [Release procedure](../../RELEASING.md)

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

New first-party packages introduced by later phases join this policy in their first release. Optional features remain opt-in even though their packages share the release version. Third-party SDKs, the external emulator, Spark protocol sources, and reference engines keep their own pinned versions; they do not adopt the project's release number.

The launcher control protocol, HTTP envelope, and Spark Connect protocol versions remain independent compatibility identifiers. A binary reporting version 4.18.0 does not imply Spark protocol version 4.18.

Use release tags `v4.16.0`, `v4.17.0`, and so on. If a maintenance patch is issued, use the same coordinated policy for that release, for example all first-party packages at **4.16.1**. Do not republish or overwrite an existing package version or tag.

This policy supersedes the earlier exception allowing `spark-sql-parser` to remain on its own version. Updating the release validator, dependency requirements, and publication workflows is a prerequisite for 4.16.0; this roadmap does not itself bump manifests or publish packages.

## Release map

| Release | Shared package version | Epic phase | Deliverable |
| --- | --- | --- | --- |
| **4.16** | **4.16.0** | 0A + 0B | Native read-only HTTP SQL service, exact SDK results, bounded runtime, sync/async launcher, and complete package distribution |
| **4.17** | **4.17.0** | 1 | Shared catalog ownership, controlled mutations, richer logical schemas, and lazy native execution improvements |
| **4.18** | **4.18.0** | 2 | Supported JVM-free Spark Connect, typed Arrow streaming, replay/release lifecycle, and separately verified emulator gRPC route |
| **4.19** | **4.19.0** | 3, optional capability | Isolated Python worker support for the declared Jobs/notebook subset |
| **4.20** | **4.20.0** | 4, optional capability | Native Delta Lake / Unity Catalog interoperability |
| **4.21+** | **4.N.0** for release 4.N | Follow-on scope to be defined | Additional conformance, supported types/operations, and measured performance improvements |

All rows are planned. Release scope is cumulative: each release retains and regression-tests previously shipped capabilities. Later scope cannot bypass an unfinished prerequisite. If an optional phase is deferred or reordered, explicitly revise this release map and its gates before assigning different work to that version. Delta/UC has no technical dependency on Python workers even though it is scheduled after them here.

## 4.16.0 — HTTP service, launcher, and unified distribution

Complete **Phase 0A before the Phase 0B release gate**. The early Connect experiment informs the architecture but remains experimental; it is not advertised as supported Spark Connect in 4.16.0.

The [detailed 4.16 implementation plan](RELEASE_4_16.md) specifies the SQL/type subset, proposed limits, ownership/control behavior, package-installation checks, and ordered delivery slices.

### Delivery sequence

| Slices | Result | Dependency to advance |
| --- | --- | --- |
| 416-01–416-03 | Frozen contracts, minimal crate, real Connect feasibility | Unmodified-client compute/empty/reconnect/release evidence; external emulator prerequisite tracked |
| 416-04–416-05 | Bounded execution/session ownership and exact scalar/SQL path | Native ownership, limits, logical schema, positional values, and allowlist checks |
| 416-06–416-07 | HTTP agent and sync/async owned-process launcher | Direct-agent plus lifecycle/platform gates |
| 416-08 | Integrated SDK route and complete candidate distribution | Corrected pinned emulator, V01–V14 and V29, all packages prepared at 4.16.0 |
| 416-09 | Coordinated publication | Fresh public installation/artifact verification in V30 |

Runtime/schema work and the external adapter fix can proceed independently after contracts/scaffolding; final readiness requires their combined evidence. Public support remains HTTP-only even though the early native Connect experiment is required.

### Scope

- Lock client/protocol/SDK/emulator test versions and SQL semantics; resolve the emulator's typed-result mapping and BIGINT preservation prerequisite.
- Add the native server workspace crate, CLI, feature boundaries, tested binaries, and Python/JVM-free container.
- Implement read-only HTTP SQL, typed positional results, readiness/capabilities, explicit unsupported errors, and minimum session ownership/TTL/close behavior.
- Include bounded admission, queues, result production, cooperative cancellation accounting, and graceful shutdown.
- Repair scalar schema mappings required for exact HTTP results.
- Ship sync/async `SparklessServer` lifecycle and the precompiled binary extra. `http_url` is available after readiness; `spark_connect_url` rejects unsupported use.
- Publish **every package in the release inventory at 4.16.0** and verify fresh installations from the published artifacts.

Primary plans: [SS-01](01-compatibility-and-emulator.md), [SS-02](02-crate-cli-and-distribution.md), [SS-03](03-connect-feasibility.md), [SS-04](04-runtime-and-operations.md), [SS-05](05-http-sql-and-results.md), [SS-06](06-python-launcher.md), the early slices of [SS-07](07-sessions-catalogs-and-mutations.md) and [SS-08](08-native-types-and-lazy-execution.md), and [SS-11](11-proxy-benchmarks-and-release.md).

### Release gate

- [ ] Validation **V01–V14** passes, including the corrected pinned emulator and public SDK result/schema fields.
- [ ] Direct HTTP and SDK tests preserve `9007199254740993`, high-precision decimals, typed nulls, empty-result schema, and declared date/time semantics.
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

### Scope

- Separate shared saved tables/database metadata from session-local temporary views/configuration.
- Implement declared catalog lifetime and persistence behavior, including close and restart semantics.
- Enable only the mutations whose coordination, visibility, operation identity, retry, and commit-outcome contracts pass their tests.
- Extend logical type/nullability metadata and the native schema path; reject unsupported mappings explicitly.
- Improve lazy range/count and expensive planning behavior under the compute scheduler.

Primary plans: [SS-07](07-sessions-catalogs-and-mutations.md), [SS-08](08-native-types-and-lazy-execution.md), and accounting/release extensions in [SS-04](04-runtime-and-operations.md) and [SS-11](11-proxy-benchmarks-and-release.md).

### Release gate

- [ ] Prior 4.16 gates still pass; **V15–V18** pass for the enabled Phase 1 surface.
- [ ] Concurrent mutation, duplicate operation, disconnect near commit, and restart tests match the documented guarantees.
- [ ] Catalog objects survive session close according to policy; temporary state remains isolated.
- [ ] Schema fidelity and lazy execution are demonstrated, with intermediate-memory measurements.
- [ ] All packages/artifacts are published and verified at **4.17.0**.

## 4.18.0 — Supported Spark Connect and Arrow execution

Complete **Phase 2**, with standalone client conformance followed by the separate emulator proxy gate, then publish all packages at **4.18.0**.

### Scope

- Implement the supported relations, expressions, analysis/configuration, and actions of the pinned unmodified client.
- Stream correct Arrow schemas/batches, including empty results and bounded local-relation decoding.
- Support stable session/operation/response identity, completion markers, bounded replay, reattachment, release, interruption, expiry, and restart behavior.
- Enable `spark_connect_url` only for a ready, supported endpoint.
- Verify the emulator's gRPC transport/authentication/session route separately after standalone conformance.

Primary plans: [SS-09](09-connect-plans-and-analysis.md), [SS-10](10-arrow-replay-and-connect-conformance.md), and endpoint/release work in [SS-06](06-python-launcher.md) and [SS-11](11-proxy-benchmarks-and-release.md).

### Release gate

- [ ] Prior supported surfaces still pass; **V18–V25** passes for the published Connect matrix, including proxy certification for the planned proxy route.
- [ ] The unmodified JVM-free client computes `spark.range(10).filter("id > 5").count() == 4` through the native engine, alongside the complete declared conformance suite.
- [ ] Reconnection after delivered batches does not rerun the plan; release/expiry/capacity-pressure tests demonstrate bounded retention.
- [ ] Multiple real client processes produce independent correct results while health traffic progresses.
- [ ] **V26** records reproducible concurrent-client benchmarks and limitations, without unmeasured speedup claims.
- [ ] All packages/artifacts are published and verified at **4.18.0**.

## 4.19.0 — Optional isolated Python workers

Target **Phase 3** as an explicitly enabled capability. Republish the complete existing package set and any new first-party worker distribution at **4.19.0**.

Primary plan: [SS-12](12-optional-python-workers.md).

- [ ] Resolve the worker security, packaging, API, and resource contract before enabling execution.
- [ ] Support only the declared Jobs/notebook subset with separate run/session identity, bounded output, truthful errors, and deterministic process-tree cleanup.
- [ ] Pass **V27**, including isolation, secret handling, quota/failure cases, and disabled-feature tests.
- [ ] Preserve Python/JVM-free core SQL/Connect images and rerun their regression gates.
- [ ] Publish and verify all participating packages at **4.19.0**; installation of optional worker support remains opt-in.

## 4.20.0 — Optional native Delta Lake / Unity Catalog

Target **Phase 4** using native storage/catalog integration. Publish the complete package set, including any new first-party integration distributions, at **4.20.0**.

Primary plan: [SS-13](13-optional-delta-and-unity-catalog.md).

- [ ] Pin native storage/catalog dependencies and document supported operations, credentials, conflict handling, and recovery semantics.
- [ ] Pass **V28** using an independent reader for schema, decimal precision, data files, and `_delta_log` transactions.
- [ ] Verify concurrent writers, cancellation/failure near commit, and explicit unsupported DDL/DML errors.
- [ ] Preserve previously shipped capabilities and keep Delta/UC optional; no Python worker dependency is introduced.
- [ ] Publish and verify all participating packages at **4.20.0**.

## Publication checklist for every release

1. Freeze the release inventory and implemented scope; record passing validation evidence and immutable source/client/emulator revisions.
2. Set all first-party package versions to the selected `4.N.0` (or coordinated patch version), update internal dependency requirements and lockfiles, and align runtime/binary metadata.
3. Build and test packaged artifacts in clean environments. Validate the combined release candidate, not only individual change branches.
4. Publish Rust packages in dependency order: parser/core before Polars, then facade, then server. Build the native Python extension from the same candidate and ship it in the matching `sparkless` distribution.
5. Publish compatible server-binary Python wheels before advertising/installing the matching `sparkless[server]` extra. Publish all declared binaries and container tags from the same candidate, with checksums/digests.
6. Verify fresh Cargo/Python installs and native binary/container execution from the published artifacts, with exact package versions checked.
7. Mark the release complete only when the whole inventory and gates pass. Registries do not publish atomically: resume a partial release only for missing verified artifacts; never overwrite existing versions. If published contents need correction, use a new coordinated patch release.

Before 4.16.0, extend the existing release version validator and CI workflows to enforce this inventory and dependency order. Record that implementation in [SS-02](02-crate-cli-and-distribution.md), [SS-06](06-python-launcher.md), and [SS-11](11-proxy-benchmarks-and-release.md). Keep platform and compatibility choices in [D05](DECISIONS.md); the shared package-version policy is fixed by this roadmap.
