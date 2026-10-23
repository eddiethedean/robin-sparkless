# sparkless-server implementation plans

**Status: implementation in progress; release gates pending.** Source for the 4.16 HTTP server, bounded sessions/runtime, SQL subset, Python launcher, binary wheels, native image, and release jobs is present. Native acceptance/platform gates remain pending. Emulator/SDK and Connect feasibility are optional certifications and do not block 4.16 publication. Prepared on 2026-10-23.

Build a single-node Rust service in `crates/sparkless-server/` that calls Robin's native engine directly. HTTP statement-agent and Spark Connect transports share execution/session/catalog services. Native SQL/Connect requires neither Python nor a JVM. The optional Python launcher manages an owned executable; optional Python code workers are a later, separate capability.

## Read first

- [Release roadmap](ROADMAP.md): 4.16.0 and later release scope, coordinated package versions, and publication gates.
- [4.16 implementation plan](RELEASE_4_16.md): ordered delivery slices, SQL/type boundary, proposed limits, lifecycle/control design, and package/release checks.
- [Full revised epic](EPIC.md): baseline architecture, contracts, limitations, and phase gates.
- [Decision register](DECISIONS.md): unresolved choices, required milestones, and evidence to record.
- [Validation matrix](VALIDATION.md): concrete release evidence required for each milestone.
- [MiniLake interoperability](14-minilake-interoperability.md): JVM-free Connect Jobs, shared external Delta/catalogs, and minimal optional upstream hooks.

The epic is the baseline requirements document. The release roadmap assigns those phases to versions and fixes the coordinated package-version policy for 4.16+. Workstream plans refine delivery without relaxing its gates. If a decision changes a requirement, update the epic, affected plans, and validation rows in the same change; synchronize the GitHub issue deliberately rather than allowing two competing specifications.

## Workstreams

SS identifiers are local plan IDs, not GitHub issue numbers. Source work has begun; table checkboxes and validation gates remain open until evidence is recorded.

| ID | Plan | Delivery |
| --- | --- | --- |
| SS-01 | [Compatibility contracts and emulator results](01-compatibility-and-emulator.md) | Optional 0A integration certification; no 4.16 publication dependency |
| SS-02 | [Rust crate, CLI, and native distribution](02-crate-cli-and-distribution.md) | Minimal scaffolding in 0A; supported CLI/image in 0B |
| SS-03 | [Early Spark Connect feasibility](03-connect-feasibility.md) | Optional 0A experiment; no 4.16 publication dependency |
| SS-04 | [Admission, operation lifecycle, and shutdown](04-runtime-and-operations.md) | Minimum production controls in 0B; replay/mutation extensions in 1–2 |
| SS-05 | [Read-only HTTP SQL and typed results](05-http-sql-and-results.md) | 0B |
| SS-06 | [Python launcher and binary packaging](06-python-launcher.md) | 0B; supported Spark Connect URL in 2 |
| SS-07 | [Session ownership, shared catalogs, and mutations](07-sessions-catalogs-and-mutations.md) | Bounded read-only sessions in 0B; catalogs/writes in 1 |
| SS-08 | [Logical types and lazy native execution](08-native-types-and-lazy-execution.md) | Scalar/schema fixes in 0B; expanded types/lazy paths in 1–2 |
| SS-09 | [Spark Connect plan translation and analysis](09-connect-plans-and-analysis.md) | 2 |
| SS-10 | [Arrow streaming, replay, and Connect conformance](10-arrow-replay-and-connect-conformance.md) | 2, informed by 0A feasibility |
| SS-11 | [Emulator routes, MiniLake certification, benchmarks, and release](11-proxy-benchmarks-and-release.md) | 0B release foundations; separate Phase 2 proxy/Jobs and concurrent-client gates |
| SS-12 | [Optional isolated Python Jobs and notebooks](12-optional-python-workers.md) | 3; optional and separately packaged |
| SS-13 | [Optional native Delta Lake and Unity Catalog](13-optional-delta-and-unity-catalog.md) | 4; optional |
| SS-14 | [MiniLake interoperability without Java or a JVM](14-minilake-interoperability.md) | 2 Connect Jobs; 4 optional shared Delta/catalogs; no 4.16 gate |

## Milestone dependency order

| Milestone | Required work | Exit evidence |
| --- | --- | --- |
| 0A, optional certification | SS-01 external contracts and emulator fix tracking; SS-03 native Connect feasibility | Exact test version manifest; SDK decoder fixtures; real default-client initialization/compute/empty/reconnect/release trace |
| 0B | SS-02 native package; SS-04 runtime; SS-05 HTTP; SS-06 launcher; SS-07 read-only session slice; SS-08 minimum scalar/schema repairs; SS-11 release checks | Direct native HTTP values/schema; lifecycle, admission, isolation, negative, shutdown, and package checks |
| 1 | SS-07 shared catalogs/mutation coordination; SS-08 richer schema and lazy paths; SS-04 accounting extensions | Visibility/restart/commit/concurrency/type/laziness evidence for each newly enabled capability |
| 2 | SS-09 plans/config/analysis and SS-10 Arrow/replay jointly; SS-06 gRPC endpoint exposure; SS-11 separate proxy gate | Standalone unmodified JVM-free client conformance first, then proxy-route certification |
| 3, optional | SS-12 and its runtime/catalog/release prerequisites | Isolated Python worker tests; core remains Python-free |
| 4, optional | SS-13 and native catalog/type/release prerequisites | Independent Delta/UC readers validate transactions; no dependency on Python workers |

SS-03 uses a bounded prototype of execution lifecycle concepts; it does not wait for the complete Phase 2 implementation. Its findings guide SS-09/10. Those workstreams share an interface contract and must pass together; neither claims compatibility alone.

MiniLake is a separate integration target from `calvinchengx/databricks-emulator`: its Jobs runner connects directly to Sparkless, while its SQL warehouse stays on DuckDB. SS-14 targets zero upstream changes for initial Jobs/catalog reads and forbids Java/JVM throughout the integration. Its optional route certification is separate from emulator typed-result/proxy certification and SS-12 Python workers. None of these upstream routes blocks the 4.16 native HTTP release.

SS-07 and SS-08 have explicit early slices: session ownership and scalar schema repairs cannot be deferred past the HTTP gate. Mutations remain rejected in 0B. General nested types, richer catalog semantics, and public Spark Connect remain gated later.

## First implementation sequence

1. Freeze the native SQL/result/control contracts and package inventory.
2. Deliver the native crate, bounded runtime, read-only sessions, scalar schema, HTTP endpoint, and owned-process launcher.
3. Pass native V05–V14 and V29 before publishing the HTTP release.
4. Evaluate pinned client/SDK/emulator contracts and Connect feasibility independently as optional certification.
5. Expand shared catalogs/types and then production Connect lifecycle/plan coverage.

## Tracking conventions

For each workstream, add its owner, implementation issue/PR, exact tested commit, and evidence links when assigned. Keep unchecked criteria until actual tests demonstrate them. An external dependency can block an integration gate without preventing independent local work.

Record known failures and unsupported cases explicitly. A successful status, accepted request, generated protobuf service, or smoke query is not sufficient evidence for a phase.

Server source paths now exist under `crates/sparkless-server/` and `python/sparkless/sparkless/server.py`. Their existence does not establish the release acceptance gates. The references to engine/Python files remain implementation touchpoints.
