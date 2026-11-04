---
search:
  exclude: true
---

# SS-23: Local, base-server, and DuckDB-server product separation

**Status: product, package, runtime, and transport source implementation is present on the unpublished 4.19 candidate; both products’ HTTP-only, Connect-only, and combined feature builds pass locally, as do the shared server regressions and base product integration tests. Exact split-product build, consumer, and inventory checks passed for commit `f3e69dd785783a4318272996b8dc62fa9ead4ca3` in [release run 41771863666](https://github.com/eddiethedean/robin-sparkless/actions/runs/41771863666); remaining release-level source/performance/support and public checks are open.** The 2026-11-02 audit found that both 4.19 server products use Polars for execution, with DuckDB limited to the read-only source adapter. The [adopted 4.19 SOLID refactor plan](../solid-refactor-4.19.md) supersedes the earlier timing decision: use an execution port implemented by the real Polars adapter and route actual requests through the backend-neutral runtime. Phase 5 extracts runtime accounting, HTTP transport, Connect protocol/replay lifecycle, and protobuf build into separate crates. Product-specific HTTP and Connect application behavior remains in the Polars composition crate. This adds no pretend DuckDB execution backend, and DuckDB query execution and writes remain future work.

**Phase 6 package decision:** the 4.19 release uses direct imports from `sparkless_connect`, `sparkless_server`, and `sparkless_server_duckdb`; it does not ship `sparkless.server` or `sparkless-server-bin` forwarding APIs. Test helpers live in the independent `sparkless-testing` distribution. This approved breaking-change decision supersedes the compatibility-forwarding proposal below; the earlier proposal remains as a historical record of the initial product design.

[4.19 delivery plan](RELEASE_4_19.md) · [Decision D22](DECISIONS.md#d22-product-and-package-separation-2026-11-02) · [Validation V59](VALIDATION.md#v59-product-separation-and-migration) · [4.20 execution scope](15-duckdb-connect-backend.md)

## Product boundaries and release scope

| Product | Rust entry point | Python distribution / import | 4.19 execution and capabilities |
| --- | --- | --- | --- |
| Local | Existing `robin-sparkless` | Existing `sparkless` / `sparkless`, backed by `sparkless-native` | Existing embedded Polars API and optional local features |
| Base server | Existing `sparkless-server` library/executable | New `sparkless-server` / `sparkless_server` | Polars; existing HTTP and Connect contracts and 4.19 performance improvements; no DuckDB library or extension |
| DuckDB server | New `sparkless-server-duckdb` library/executable | New `sparkless-server-duckdb` / `sparkless_server_duckdb` | Polars plus the existing read-only standalone/Quack DuckDB source; preserves the base server surface and adds source capabilities |

These are product profiles, not a permanent ranking of feature support. Local-to-server expansion remains required by [SS-22](22-local-sql-feature-parity.md). Advertise capabilities by product, execution engine, source, transport, types and tested combinations. Quack identifies the shared-owner connection mode, not a third query engine or an upstream Spark compatibility version.

The DuckDB product does **not** claim DuckDB query execution in 4.19. Phase 4 / 4.20 adds that execution backend to this product after D15 and V37/V38; it must explicitly select and report the execution mode. In 4.19 both server products execute transformations in Polars. Separating packages neither implements writes nor expands the Connect SQL/type matrix. Keep optional local drivers, workers and future compatibility profiles independently scoped.

## Rust dependency and ownership boundaries

Keep existing published names where their purpose remains valid. Give each server product a thin composition crate and executable. In the 4.19 target they compose around one backend-neutral runtime, separate transports and the Polars execution adapter.

| Crate | Ownership | Allowed internal dependencies |
| --- | --- | --- |
| `robin-sparkless-core` (existing) | Engine-neutral types, expressions/plans, errors, cancellation and external catalog/Arrow IPC contracts | No concrete execution engine or server runtime |
| `spark-sql-parser` (existing) | Shared SQL parsing | No server runtime or concrete execution engine |
| `robin-sparkless-polars` (existing) | Polars execution, local API implementation, catalog and schema/IPC behavior | Core and parser; optional source compatibility feature described below |
| `robin-sparkless` (existing) | Source-compatible public local facade | Core and Polars; existing opt-in features |
| `robin-sparkless-duckdb` (new) | DuckDB source configuration, connections/transactions, Quack ownership/cancellation, type mapping and bounded IPC production | Core and native DuckDB/Arrow dependencies; no Polars or server dependency |
| `sparkless-server-core` (new) | Engine-neutral server configuration and SQL/Connect execution-port contracts | Neutral engine contracts and JSON value types; no runtime, transport, local facade, Polars, DuckDB or PyO3 dependency |
| `sparkless-server-runtime` (new) | Execution-port dispatch, connection/admission limits, retained operation registry and generic session registry | Server core and neutral core contracts only; no transport, local facade or concrete engine |
| `sparkless-server-http` (new) | HTTP/1 listener, route dispatch, JSON header validation and bounded body reads | Runtime and JSON/HTTP contracts; no concrete engine or Connect wire types |
| `sparkless-server-connect` (new) | Protobuf build, bounded wire decoding, generated gRPC service dispatch/listener, replay/session history, sender accounting and drain state | Runtime, neutral core, tonic/prost and protocol dependencies; no concrete execution engine or HTTP implementation |
| `sparkless-server-polars` (existing) | Product-specific HTTP and Connect handlers, Polars execution adapters, catalog/schema/IPC integration and composition | Runtime, server core, parser and local engine/facade; no DuckDB by default; optional DuckDB-source feature is enabled only by the DuckDB product |
| `sparkless-server` (existing) | Base CLI/library composition and compatibility exports | Server core, runtime, HTTP/Connect transports and Polars adapter; no DuckDB feature on the default product |
| `sparkless-server-duckdb` (new) | DuckDB product CLI/library composition, trusted source wiring and product identity | Server core, runtime, HTTP/Connect transports, Polars adapter and DuckDB source crate in 4.19 |

Move the current `robin-sparkless-polars/src/duckdb_source.rs`, its source tests and native owner fixture into `robin-sparkless-duckdb`. Its production imports already use core contracts rather than Polars. Keep Polars responsible for consuming typed source batches. Preserve the existing root/Polars `duckdb-source` feature and public source reexports as optional compatibility forwarding to the new crate, with no defaults enabling them. The base server's published graph must not enable this forwarding feature.

The source audit confirmed that the existing Connect plan translator directly uses `SparkSession`, `SharedCatalog`, Polars schema/export helpers and engine hooks. Phase 1 separates state and responsibilities in place. Phase 5 now places admission, operation accounting and the generic session registry in `sparkless-server-runtime`; HTTP listener/routing/body handling in `sparkless-server-http`; and protobuf generation, bounded wire handling, generated gRPC service dispatch, listener lifecycle, replay storage, Connect session history and transport budgets in `sparkless-server-connect`. Product-specific handlers and physical execution/schema conversion stay behind the Polars composition adapter. Both products use this adapter in 4.19, so the extracted boundaries are exercised by real requests. The DuckDB source implementation lives in `robin-sparkless-duckdb`; its resolver and bounded Arrow producer use core contracts and have no Polars/server dependency. The DuckDB product enables that source through the existing optional compatibility forwarding edge.

The 4.19 runtime extraction introduces a used server-facing execution interface now, implemented by Polars and consumed by both transports. Before Phase 4 adds actual DuckDB query execution, implement that interface for DuckDB and split common parsing/validation from physical evaluation and engine schema conversion. Preserve typed bounded IPC batches, logical metadata, zero-row schemas, statement snapshots, byte accounting and producer lifetime, and reuse the single scheduler/replay/catalog ownership model. Do not bridge engines by materializing rows.

Preserve existing public `sparkless_server` Rust exports through reexports/facades where possible; inventory signatures, CLI flags, error types and examples before moving them. An intentional source break needs a named migration and version-policy decision before acceptance. Both executables share CLI/control implementations but have distinct executable names. `cargo install sparkless-server` and `cargo install sparkless-server-duckdb` must coexist. HTTP-only, Connect-only and combined builds remain independently supported for each advertised product. Shared core defaults must not pull an unused transport into embedding consumers.

Map the candidate's `cargo run -p sparkless-server --features connect,duckdb-source` invocation to the new DuckDB product explicitly; do not leave a base-product feature that silently changes its identity. A base server given `SPARKLESS_DUCKDB_SOURCES` must reject that unsupported configuration before advertising readiness and direct the operator to the DuckDB product. Existing local facade/source feature forwarding is a separate compatibility path.

Cargo features can unify when multiple workspace members are built together. Prove dependency isolation using separate consumer projects and separate product build invocations/target directories, then inspect both actual packaged binaries. A combined workspace test graph alone cannot establish that the base artifact excludes DuckDB. Record `cargo tree` feature graphs and native dependencies for the exact builds used for packaging.

## Python distribution and import boundaries

Use one lightweight shared launcher and one payload-owning distribution per server product. Avoid a second layer of permanent binary-only projects for each new product.

| Distribution | Import / contents | Direct dependency policy |
| --- | --- | --- |
| `sparkless` (existing) | Local API and documented server extras; no `sparkless.server` module | Exact `sparkless-native`; server dependencies remain extras |
| `sparkless-native` (existing) | Existing PyO3 engine | Local engine only; no server dependency |
| `sparkless-connect` (new) | `sparkless_connect`; canonical sync/async launcher and owned `SparklessConnect` helper | Standard-library launcher; optional `[client]` pins `pyspark-client==4.0.5`; no local engine or bundled server |
| `sparkless-server` (new) | `sparkless_server`; base executable, accessor and thin product-bound launcher exports | Exact `sparkless-connect`; optional `[connect]` adds the pinned client |
| `sparkless-server-duckdb` (new) | `sparkless_server_duckdb`; DuckDB-enabled executable, exact matching Quack extension/license notices, accessors and thin product-bound launcher exports | Exact `sparkless-connect`; optional `[connect]` adds the pinned client; no base server or local extension dependency |
| `sparkless-testing` (new) | `sparkless_testing`; backend-independent pytest plugin, fixtures, and comparison helpers | `pytest>=7`; no Sparkless, native engine, or PySpark dependency |

The package names are the 4.19 candidate inventory; their first PyPI uploads remain a release action. The release workflow uses the configured `PYPI_API_TOKEN` secret for all four new projects: `sparkless-connect`, `sparkless-server`, `sparkless-server-duckdb`, and `sparkless-testing`. Existing PyPI projects retain Trusted Publishing. Confirm the names are available and the token can create and upload each project before publication. If a name is unavailable, resolve D22 and update all manifests/docs together; do not silently publish under another name.

Retain Python >=3.11, the local native wheel matrix, and the existing two server wheel targets unless separately expanded with execution evidence. Server product wheels remain platform-specific `py3-none-*` wheels, not PyO3 extensions. `sparkless-connect` and `sparkless-testing` are universal wheels; publish source archives only for distributions with a tested source-build/install path. Each server product uses a distinct import namespace, executable basename, package resource directory and console command so both can coexist. Only the DuckDB product owns the Quack extension and its notices. Opening a source still requires trusted configuration; no runtime extension download is introduced.

Move the launcher implementation out of `sparkless.server` into `sparkless_connect`; product modules bind it to their own executable accessor. Keep imports lazy and side-effect free. `sparkless_connect` alone can manage an explicit executable; `SparklessConnect` imports the public client only on entry and reports the precise missing extra. A user connecting to an existing endpoint can install only `sparkless-connect[client]` (or use the pinned public client directly), without either server binary or the local extension. Owning an external server remains separate from the existing owned-helper API.

Preserve the constructor and cleanup behavior already implemented for `SparklessServer`, `AsyncSparklessServer`, `SparklessConnect`, and their exported exceptions. The approved 4.19 breaking change removes `sparkless.server` and its top-level lazy exports instead of keeping forwarding facades. Canonical server/client imports must not import `sparkless`, `sparkless_native` or PySpark during import. Re-export shared exception objects from their owning packages so callers' `except` clauses remain valid.

Default selection must be deterministic: the base product is the default, and installing the DuckDB product alongside it never changes that default. A DuckDB product-bound helper explicitly selects its own binary. Missing selected products and capability mismatches fail with an actionable install error; never search PATH, build/download a binary, or silently substitute another product. Preserve the existing explicit `executable_path` route and its version/control checks. Add product/source checks for callers requiring DuckDB capabilities.

## Compatibility, extras and capability identity

Preserve these convenience extras on the local API while making independent server installation the documented server path:

| Install request | Resolution at 4.19 |
| --- | --- |
| `sparkless` | Local API plus local native engine only |
| `sparkless[server]` | Local API plus base `sparkless-server` |
| `sparkless[connect]` | Local API plus `sparkless-server[connect]` |
| `sparkless[server-duckdb]` | Local API plus `sparkless-server-duckdb` |
| `sparkless[connect-duckdb]` | Local API plus `sparkless-server-duckdb[connect]` |
| `sparkless-server[connect]` | Base executable, common launcher and public client; no local PyO3 engine |
| `sparkless-server-duckdb[connect]` | Source-enabled executable, extension, common launcher and public client; no local PyO3 engine |
| `sparkless-connect[client]` | Remote client/common launcher only; no packaged server or local engine |

The pre-separation 4.19 `sparkless-server-bin` candidate included DuckDB and Quack. The final 4.19 graph moves that payload to `sparkless-server-duckdb` and removes `sparkless-server-bin` from the candidate. The published 4.18.0 base server did not promise a DuckDB source. Current product accessors live in `sparkless_server` and `sparkless_server_duckdb`; `quack_extension_path()` is specific to the DuckDB package. Old candidate hashes remain historical because payload ownership changed.

Examples using `sparkless[connect-duckdb]` import the product-bound helper from `sparkless_server_duckdb`, or explicitly select that product in the common launcher. The removed `sparkless.server` helper is not a supported import path; package selection makes the server product explicit rather than guessing from the environment.

Retain control protocol v2 and capabilities v1 if the new identity fields are additive. Record product ID (`base` / `duckdb`), actual execution backend (`polars` in both 4.19 products), source capabilities and build/version identity in capabilities and readiness. Bind each operation to the instance and selected semantic context as before. Test old optional-field parsing, strict new-product checks, mismatched executables and combined installs; bump the applicable protocol if implementation requires an incompatible change. Never infer query execution engine from a distribution name or the presence of Quack.

Keep product identity independent of the versioned upstream compatibility packages in [SS-21](21-versioned-connect-packages.md): engine/product choice and Spark protocol profile are separate axes. This migration does not add `connect35/40/41/42` packages early.

## Migration of the implemented candidate

Use the current branch and [retained evidence](RELEASE_4_19_EVIDENCE.md), not the original pre-implementation baseline. Preserve the implemented source contracts, helper lifecycle, exporter/count fast paths, scheduling changes, numeric budgets and raw reports. Existing evidence remains evidence for its exact source/artifact; it cannot certify a newly composed binary or dependency graph.

| Slice | Work and ownership | Dependencies / completion evidence |
| --- | --- | --- |
| **419-12 — implemented; first upload pending** | Freeze D22 inventory, public API/CLI/import compatibility map, dependency graphs and capability identity | Product map, manifests, graph commands and publisher split are recorded. Confirm names are available and the configured token has upload access |
| **419-13 — source and feature matrix; exact-product candidate checks passed** | Move DuckDB source into its crate and compose two products with the Polars execution adapter | Source/API forwarding, separate product crates/targets, runtime/transport extraction, isolated feature graphs, and both products’ HTTP/Connect builds passed in [release run 41771863666](https://github.com/eddiethedean/robin-sparkless/actions/runs/41771863666). Phase 4 adds DuckDB execution, not the initial runtime boundary |
| **419-14 — implementation and exact-product candidate checks passed** | Extract shared Python launcher, package both products, and publish the breaking migration | Common launcher, separate wheel payloads, extras, deterministic selection, retired-path checks, and client/local/base/DuckDB/coinstall installs passed in the candidate matrix; first uploads and public verification remain |
| **419-15 — exact candidate run passed; public stage pending** | Update CI/release/build/package verifiers, both images, product docs and exact inventories | Shared dependency-graph checks, source-revision-bound inventory, source/differential/resource checks, both product images, and migration docs passed in run 41771863666. Public verification follows publication |

The implementation and exact-product candidate evidence are recorded for commit `f3e69dd785783a4318272996b8dc62fa9ead4ca3` in the [release evidence](RELEASE_4_19_EVIDENCE.md). Preserve pre-separation hashes and reports as historical evidence; they do not certify rebuilt products. Candidate acceptance passed for the product migration, while remaining source/performance/support gates, registry-name and first-upload checks remain release prerequisites. The fresh-public V59 stage follows publication.

## Build, release and verification inventory

Extend the current tooling instead of adding a parallel release framework. Update workspace manifests/lockfiles/package include lists, source archive vendoring, Python build metadata, version/pin checks, wheel payload/extension verifiers, install helpers, release distribution/public-inventory checks, storage accounting and tests. Both native Rust consumers and default Python engine builds must remain independent of DuckDB. Publish Rust crates in topological order and retain archive/downstream-consumer validation; a workspace-only build cannot prove crates.io packages are complete.

Build each product's Linux executable separately, package it once, and copy those exact checked wheel bytes into that product's image. Keep the existing base image reference and add a separately named DuckDB image; never replace one product's image with the other. Preserve non-root execution, both transports, graceful shutdown and the Python/JVM-free runtime checks. Re-measure startup, binary/wheel/image sizes and the applicable CPU/RSS/latency guardrails for the final products; do not compare a new base binary to an old source-enabled binary without recording the configuration difference.

All changed/new Rust crates and binary products use engine version 4.19.0 for this first split; new launcher/product/testing Python distributions use 4.19.0 and exact compatible pins. Retain the existing API-versus-engine version policy for future Python-only patches. Never mutate published 4.18 artifacts. Confirm that 4.19 remains unpublished before publication; a partial publication requires the existing patch/recovery policy.

The acceptance workflow enumerates every new distribution and both server/image products in one candidate inventory. The inventory records the exact source revision, file names, sizes, and hashes; every publisher checks its downloaded bytes against it before upload. Publish checked immutable bytes in dependency order: shared launcher and product payloads before their API dependents, alongside the existing native-engine/Rust order. Confirm new project names/token access and check per-project storage budgets before any publisher runs. Required missing/skipped product jobs block publication. Fresh public checks must verify all exact file inventories/hashes, crates, both image digests, imports, selection and HTTP/Connect/source behavior after publication. A partially published split leaves release completion open.

V59 requires clean installs of client-only, local-only, base-only, DuckDB-only, both products, and all documented extras on supported targets. Assert absent distributions as well as successful imports: server-only installs contain no `sparkless-native`; base artifacts contain no DuckDB/Quack; client-only installs contain neither engine nor server; removed `sparkless.server` and `sparkless-server-bin` paths are absent. Exercise both products in one environment and as concurrent separate processes, including shared port-collision errors and independent identities/cleanup. Repeat source differential/snapshot/cancellation/replay checks through the DuckDB product, and earlier supported HTTP/Connect checks through both products. Rerun relevant helper, performance and resource acceptance on final artifacts and record limitations by product/platform.

## Release hold and completion

The exact split-product checks for isolated graphs, clean client/local/base/DuckDB/combined installs, both product wheels and images, breaking migration, capability selection, source checks, and candidate inventory passed in run 41771863666. The candidate is **not ready to cut** until the remaining original feature/source/resource gates and first-upload setup are closed. Configure and confirm registry project ownership before publication. Public artifact checks close only after publication. Keep pre-separation evidence immutable and add exact-product evidence rather than overwriting it. Product separation does not claim DuckDB execution, writes, new platform support or full local/server parity.
