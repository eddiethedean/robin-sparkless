# sparkless-server implementation plans

**Status: planned.** These documents describe proposed work for [issue #1726](https://github.com/eddiethedean/robin-sparkless/issues/1726). They do not announce a working server, launcher, or protocol compatibility. Prepared on 2026-10-23.

Build a single-node Rust service in `crates/sparkless-server/` that calls Robin's native engine directly. HTTP statement-agent and Spark Connect transports share execution/session/catalog services. Native SQL/Connect requires neither Python nor a JVM. The optional Python launcher manages an owned executable; optional Python code workers are a later, separate capability.

## Read first

- [Release roadmap](ROADMAP.md): 4.16.0 and later release scope, coordinated package versions, and publication gates.
- [4.16 implementation plan](RELEASE_4_16.md): ordered delivery slices, SQL/type boundary, proposed limits, lifecycle/control design, and package/release checks.
- [4.17 implementation plan](RELEASE_4_17.md): shared catalog/snapshot ownership, controlled mutation identities and outcomes, logical schemas, lazy execution, and coordinated release checks.
- [4.18 implementation plan](RELEASE_4_18.md): public Connect subset, physical Arrow encoding, bounded replay/lifecycle, standalone then proxy conformance, and candidate/publication gates.
- [Spark Connect public-contract research](CONNECT_PROTOCOL_RESEARCH.md): Apache Spark 4.0.5 source review, 4.0.0 source comparison, pinned client fixtures, retry/config/IPC requirements, and product scope versus public protocol behavior.
- [DuckDB data source](14-duckdb-data-source.md) and [DuckDB Connect backend](15-duckdb-connect-backend.md): separate Phases 3/4 after supported Connect, targeted at 4.19/4.20.
- [Full revised epic](EPIC.md): baseline architecture, contracts, limitations, and phase gates.
- [Decision register](DECISIONS.md): unresolved choices, required milestones, and evidence to record.
- [Validation matrix](VALIDATION.md): concrete release evidence required for each milestone.

The epic is the baseline requirements document. The release roadmap assigns those phases to versions and fixes the coordinated package-version policy for 4.16+. Workstream plans refine delivery without relaxing its gates. If a decision changes a requirement, update the epic, affected plans, and validation rows in the same change; synchronize the GitHub issue deliberately rather than allowing two competing specifications.

## Workstreams

All workstreams are **planned**, with owners unassigned and implementation issue/PR links not yet allocated. SS identifiers are local plan IDs, not GitHub issue numbers.

| ID | Plan | Delivery |
| --- | --- | --- |
| SS-01 | [Compatibility contracts and typed results](01-compatibility-and-emulator.md) | 0A, with direct HTTP gate in 0B |
| SS-02 | [Rust crate, CLI, and native distribution](02-crate-cli-and-distribution.md) | Minimal scaffolding in 0A; supported CLI/image in 0B |
| SS-03 | [Early Spark Connect feasibility](03-connect-feasibility.md) | 0A only; experimental endpoint |
| SS-04 | [Admission, operation lifecycle, and shutdown](04-runtime-and-operations.md) | Minimum production controls in 0B; replay/mutation extensions in 1–2 |
| SS-05 | [Read-only HTTP SQL and typed results](05-http-sql-and-results.md) | 0B |
| SS-06 | [Python launcher and binary packaging](06-python-launcher.md) | 0B; supported Spark Connect URL in 2 |
| SS-07 | [Session ownership, shared catalogs, and mutations](07-sessions-catalogs-and-mutations.md) | Bounded read-only sessions in 0B; catalogs/writes in 1 |
| SS-08 | [Logical types and lazy native execution](08-native-types-and-lazy-execution.md) | Scalar/schema fixes in 0B; expanded types/lazy paths in 1–2 |
| SS-09 | [Spark Connect plan translation and analysis](09-connect-plans-and-analysis.md) | 2 |
| SS-10 | [Arrow streaming, replay, and Connect conformance](10-arrow-replay-and-connect-conformance.md) | 2, informed by 0A feasibility |
| SS-11 | [Standalone conformance, benchmarks, and release](11-proxy-benchmarks-and-release.md) | 0B release foundations; Phase 2 Connect and concurrent-client release |
| SS-12 | [Optional isolated Python Jobs and notebooks](12-optional-python-workers.md) | 5; optional and separately packaged |
| SS-13 | [Optional native Delta Lake and Unity Catalog](13-optional-delta-and-unity-catalog.md) | 6; optional |
| SS-14 | [DuckDB data source for Polars execution](14-duckdb-data-source.md) | 3; optional, after supported Connect |
| SS-15 | [DuckDB execution backend for Connect](15-duckdb-connect-backend.md) | 4; optional, after the DuckDB source phase |

## Milestone dependency order

| Milestone | Required work | Exit evidence |
| --- | --- | --- |
| 0A | SS-01 compatibility contracts; minimal SS-02 scaffold; SS-03 native Connect feasibility | Exact test version manifest; typed HTTP decoder fixtures; real default-client initialization/compute/empty/reconnect/release trace |
| 0B | SS-02 native package; SS-04 runtime; SS-05 HTTP; SS-06 launcher; SS-07 read-only session slice; SS-08 minimum scalar/schema repairs; SS-11 release checks | Direct HTTP values/schema and packaged launcher checks; lifecycle, admission, isolation, negative and shutdown checks |
| 1 | SS-07 shared catalogs/mutation coordination; SS-08 richer schema and lazy paths; SS-04 accounting extensions | Visibility/restart/commit/concurrency/type/laziness evidence for each newly enabled capability |
| 2 | SS-09 plans/config/analysis and SS-10 Arrow/replay jointly; SS-06 gRPC endpoint exposure; SS-11 standalone release checks | Standalone unmodified JVM-free client conformance; optional proxy certification is supplemental |
| 3, optional | SS-14 after supported Connect; D14 source/ownership contract | V35/V36 typed DuckDB-to-Polars scans, snapshots, bounded resources, and independent writer/reader sharing |
| 4, optional | SS-15 after SS-14; D15 execution/backend contract | V37/V38 DuckDB computation, transaction outcomes, Connect lifecycle, and shared managed-table writes |
| 5, optional | SS-12 and its runtime/catalog/release prerequisites | V27 isolated Python workers; core remains Python-free |
| 6, optional | SS-13 and native catalog/type/release prerequisites | V28 independent Delta/UC readers validate transactions; no dependency on Python workers |

SS-03 uses a bounded prototype of execution lifecycle concepts; it does not wait for the complete Phase 2 implementation. Its findings guide SS-09/10. Those workstreams share an interface contract and must pass together; neither claims compatibility alone.

SS-07 and SS-08 have explicit early slices: session ownership and scalar schema repairs cannot be deferred past the HTTP gate. Mutations remain rejected in 0B. General nested types, richer catalog semantics, and public Spark Connect remain gated later.

## First implementation sequence

1. Assign owners and resolve the 0A compatibility decisions using self-contained fixtures.
2. Establish only enough native crate/protobuf scaffolding for the Connect feasibility test.
3. Capture actual HTTP/Connect client contracts and record missing engine work.
4. Deliver the minimum runtime, read-only sessions, scalar schema fixes, HTTP endpoint, and owned-process launcher.
5. Pass the full 0B matrix, including direct HTTP typed results and packaged consumers, before calling the HTTP MVP complete.
6. Expand shared catalogs/types and then production Connect lifecycle/plan coverage.
7. After supported Connect, deliver the DuckDB source phase, then the separate DuckDB execution backend phase. Python workers and native Delta/UC follow under the revised release map. Workstream IDs SS-12–SS-15 are identifiers, not phase order.

## Tracking conventions

For each workstream, add its owner, implementation issue/PR, exact tested commit, and evidence links when assigned. Keep unchecked criteria until actual tests demonstrate them. Optional external interoperability is supplemental and cannot block standalone development or release gates.

Record known failures and unsupported cases explicitly. A successful status, accepted request, generated protobuf service, or smoke query is not sufficient evidence for a phase.

All server source paths in these plans are proposed unless they already exist in the repository. The references to current native engine/Python files identify implementation touchpoints, not completed server work.
