---
search:
  exclude: true
---

# Repository-wide SOLID refactor for 4.19.0

Status: adopted for 4.19.0. Phase 1 source decomposition is implemented; its targeted-test
gate and the P0 inventory closeout remain pending.

Baseline: working tree inspected on 2026-11-02, based on commit `1292e3dd`, including
existing uncommitted product/source changes. File sizes below describe that snapshot,
not a clean release commit. Rebaseline before implementation.

The release may introduce breaking changes. New Rust crates and Python packages are
allowed. DuckDB writes are a future requirement; the 4.19 source remains read-only.
The objective is cohesive responsibilities, enforceable dependency boundaries, and
explicit behavioral contracts across the entire repository.

## 1. Decisions and scope

1. Use the 4.19 breaking-change window to remove obsolete adapters and duplicate
   entry points. Migrate first-party consumers in the same change; publish a migration
   guide for external consumers. Do not maintain parallel implementations merely to
   preserve old signatures.
2. Preserve supported query semantics, lazy evaluation, type fidelity, resource
   limits, and lifecycle guarantees. An API break is not permission to change results,
   silently drop a capability, or weaken cancellation/commit guarantees. Any intended
   semantic change gets its own specification and regression evidence.
3. Keep the local Rust facade explicitly Polars-backed. Engine-neutral types and
   contracts belong below it. Remove the box/downcast round trip used to return a
   concrete Polars frame from supposedly generic facade methods.
4. Extract a server runtime and transport crates now, using a small execution port
   implemented by the existing Polars adapter and consumed by real HTTP/Connect
   requests. Do not implement a pretend DuckDB execution backend.
5. Split catalog resolution, snapshot reads, and writes now. Existing writable
   providers exercise the write contract; DuckDB can acquire that capability later.
   Reading a DuckDB source, writing to DuckDB, and executing queries in DuckDB are
   three separate capabilities.
6. Prefer private modules for cohesion and crates for dependency/feature boundaries.
   Prefer Python subpackages for organization and distributions for independently
   installable products. No interface or distribution exists solely to satisfy SOLID.

This plan deliberately revises the runtime-extraction timing and compatibility policy
in [SS-23](sparkless-server/23-product-and-package-separation.md). Its earlier decision
to wait for a second execution backend is superseded **when this plan is adopted**;
update D22 and SS-23 in the first implementation change. Keep their product separation,
installation isolation, and truthful capabilities. Future DuckDB execution/writes
remain tracked by [SS-15](sparkless-server/15-duckdb-connect-backend.md).
Historical plans, including `SPARKLESS_REFACTOR_PLAN.md`, are not implementation
specifications for the current codebase.

In scope: all first-party Rust crates, Python bindings/API/testing helpers, server
products, SQL and expression translation, storage adapters, tests, benchmarks,
build/release automation, examples, and architecture/migration documentation.
Generated protobuf output, vendored dependencies, built wheels, `python/build`,
debug symbols, and benchmark output are excluded from source refactoring. Retain
vendor patches and historical release evidence unless separately changed and verified.

## 2. Findings that drive the work

Sizes include inline tests; size is an investigation signal, not proof of a SOLID
violation. Separate production and test counts in the implementation baseline.

| Current location | Observed issue | Refactor direction |
| --- | --- | --- |
| `python/native/src/lib.rs` — 15,508 lines | Binding registration, Python conversion, session/catalog, actions/cancellation, readers/writers, frames, columns, functions and UDF behavior share one module | Separate binding classes, conversion, execution context and registration; move engine policy down |
| `robin-sparkless-polars/src/dataframe/mod.rs` — 10,469 lines | Frame state, actions, Arrow producers, range tracking and writers coexist | Separate plan state, execution/export and writes; preserve existing joins/aggregations modules |
| `robin-sparkless-polars/src/session/mod.rs` — 9,277 lines | Session state, shared catalog, construction, Arrow input validation, SQL orchestration and tests coexist | Session composition over catalog, construction, input and execution components |
| `sparkless-server-polars/src/lib.rs` — 6,929 lines; `connect.rs` — 4,924 | Transport handling, operation ownership, admission, sessions, replay and concrete execution are coupled | Runtime state machines, transport crates and a Polars adapter |
| `functions/rest.rs` — 5,284 lines; `udfs/rest.rs` — 7,269 | Function families have catch-all ownership | Category modules with explicit registration/dispatch and shared semantic helpers |
| SQL translator — 5,348 lines; JSON expression translator — 3,306; Connect plan translator — 3,650 | Different frontends mix validation, name/type rules and concrete execution | Typed normalized plans and a shared supported semantic layer, with separate frontend policies |
| `src/dataframe.rs`, core `engine/*`, `engine_backend.rs` | Concrete facade wraps Polars; trait returns are boxed then downcast; same-engine operations rely on `as_any`; a session trait includes one fixed tuple constructor | Concrete local facade; narrow typed query contracts; runtime type erasure only at runtime boundaries |
| Core `ExternalCatalogProvider` | Resolution, read-only namespace policy, scans, snapshots, writes and optional batch writes share one trait | Explicit ownership resolution plus separate reader/writer capabilities |
| `plan/mod.rs` | JSON decoding and evaluation interleave; `PolarsPlanExecutor` maps all plan failures to `EngineError::Internal` | Decode/validate before evaluation; preserve typed failure categories |
| `python/connect/sparkless_connect/server.py` — 1,028 lines | Product lookup, protocol validation, sync/async process ownership and client wrapper share a file | Shared protocol/product policy plus distinct sync/async lifecycle implementations |
| Release workflow — 1,245 lines; CI — 572 | Large build/verification/publication orchestration; product graph isolation needs explicit checking | Reusable build/verify jobs with narrow publication authority and one artifact inventory |

Existing assets to retain: neutral schemas/expressions/cancellation in core; external
catalog snapshot and Arrow batch contracts; the SQL parser crate; the new standalone
DuckDB source crate; product identity/config in server core; PySpark oracle fixtures;
Connect replay/resource tests; clean-install and published-artifact verification scripts.
This is an incremental restructuring of working components, not a replacement engine.

## 3. Target ownership and dependencies

Arrows below mean “depends on.” Names of new crates are proposed local workspace names,
not claims of registry availability. Decide publication names before release work.

```text
sparkless Python API -> sparkless-native -> robin-sparkless facade
                                                |
                                                v
                                      robin-sparkless-polars
                                        |       |       |
                                        v       v       v
                              core contracts   plan    sql -> spark-sql-parser
                                                |       |
                                                v       v
                                               core <- plan

server product composition -> http / connect -> server-runtime -> server-core
            |                      |                                  |
            v                      +-> plan / sql                     v
    server-polars adapter -> polars                              core / plan
            |
            +-> server-core execution contracts

product/local composition -> delta / jdbc / duckdb adapters -> core contracts
```

The facade and engine never depend on server crates. Server runtime and transports
never depend on the root facade, Polars, DuckDB, or PyO3. Storage drivers do not depend
on transport or server lifetime state. Runtime execution uses injected contracts;
product constructors select concrete implementations.

| Crate | Owns | Dependency rule |
| --- | --- | --- |
| `robin-sparkless-core` | Logical types, errors, expressions, cancellation and provider contracts | No execution engine, transport, Python or database driver dependencies |
| **New** `robin-sparkless-plan` | Typed relational operations, JSON plan decoding and structural validation | Core + serialization; no Polars, protobuf or SQL-parser AST in public contracts |
| **New** `robin-sparkless-sql` | SQL normalization, supported AST-to-plan lowering, identifiers and semantic analysis shared by local/server consumers | Parser + plan + core; physical execution remains in engine adapters |
| `spark-sql-parser` | Spark SQL syntax and AST production | No engine, catalog, IO or execution policy |
| `robin-sparkless-polars` | Plan lowering, expression kernels, execution, native conversion and Polars session/frame state | Core/plan/SQL; optional driver integration modules use provider contracts |
| `robin-sparkless-duckdb` | DuckDB source ownership, snapshots, bounded IPC and future write adapter | Core + DuckDB; future execution lowering may depend on plan; no server dependency |
| **New** `robin-sparkless-delta`, `robin-sparkless-jdbc` | Provider/driver connection, metadata, storage IO and transaction behavior | Core + respective drivers; Polars conversion remains in Polars integration modules |
| `robin-sparkless` | Local public composition and ergonomic Rust API | Explicitly Polars-backed; retain only useful wrappers rather than duplicate every method |
| `sparkless-server-core` | Product/config types, request/result/error contracts, execution/session ports and capability descriptors | Core/plan; no Hyper, Tonic, Polars or database drivers |
| **New** `sparkless-server-runtime` | Session and operation ownership, admission, mutation ordering, budgets, cancellation, replay retention, readiness/drain | Server core + async primitives; no wire DTOs or concrete engines |
| **New** `sparkless-server-http` | HTTP routes, request decoding, response/status rendering and listener | Runtime/core; Hyper and HTTP-specific dependencies |
| **New** `sparkless-server-connect` | Protobuf build, gRPC service, Connect validation/translation, analysis/config responses | Runtime/core/plan/SQL; Prost/Tonic isolated here |
| `sparkless-server-polars` | Concrete execution/session adapter and result/schema conversion | Server core + Polars engine; transports compose it from above |
| `sparkless-server`, `sparkless-server-duckdb` | Product composition, binaries and product-specific capabilities | Runtime, selected transports, Polars adapter; only DuckDB product enables its source |
| `sparkless-native` | Python ABI, argument/result conversion, exception mapping, callback/GIL ownership | Local facade and engine conversion boundary; no server dependency |

Do not move a module into core just because two consumers use it. Shared physical
Polars logic remains in the Polars crate. Add an Arrow support crate only if extraction
reveals genuinely shared IPC validation/accounting code with independent consumers;
do not force incompatible driver/Polars Arrow types into one public native type.

Python ownership:

- Keep `sparkless`, `sparkless-native`, `sparkless-connect`, `sparkless-server` and
  `sparkless-server-duckdb` as distinct installable products. The new package split
  already present is the starting point, not work to repeat.
- Divide native Rust bindings into `conversion/`, `session/`, `dataframe/`,
  `column/`, `functions/`, `io/`, `catalog/`, `udf/`, `actions/`, `errors.rs`,
  and `registration.rs`. Keep PyO3 class names/registration explicit. Verify how the
  pinned PyO3 configuration supports split implementations before moving methods;
  use ordinary helper functions where multiple `#[pymethods]` blocks are unsuitable.
- Divide Python SQL function implementations into private category modules while
  retaining the familiar `sparkless.sql.functions` import. Keep argument adaptation
  in Python; place engine semantics in Rust and Python callback semantics at the ABI.
- Divide `sparkless_connect` into product resolution, control protocol, sync process,
  async process, and client helper modules. Share parsing and validation without
  forcing sync/async resource ownership into one inheritance hierarchy.
- Extract **new** `sparkless-testing` if its API can run with either backend without
  importing the native extension; use canonical `sparkless_testing` imports and a
  `sparkless[testing]` extra. Migrate the pytest plugin and backend selection together.
  This is the only proposed additional Python distribution; ordinary module splits
  do not justify additional releases.

## 4. SOLID contracts

### Single responsibility

Give each stateful component one owner: session registry owns session lifetime;
operation registry owns operation identity/outcome; replay store owns retained result
bytes; admission owns permits; catalog adapter owns storage transactions; execution
adapter owns physical plans. Transports map requests into these services.

Split server files without changing lock order first. Document every lock and its
protected invariant, which guards survive worker handoff, and which component owns
each resource counter. Never hold registry locks across engine work or client IO.
Keep shared HTTP/Connect admission, mutation ordering and session ownership intact.

### Open/closed

Frontends decode into typed operations and call shared semantic helpers. New expression
families add a category implementation and explicit dispatch entries. Exhaustive Rust
enums remain appropriate; SOLID does not require a dynamic plugin registry for built-ins.
Use provider interfaces for actual extension points: catalogs, sources and execution.

Start with currently supported common operations: input, projection, filter, join,
aggregation, sort and limit. Preserve frontend-specific validation and supported
subsets. Extend the typed representation to cover the current supported corpus before
removing old lowering paths. Do not pretend Connect, local SQL and JSON plans have
identical feature support. Keep Polars optimization plans distinct from portable plans.
Represent supported DDL/DML as explicit commands with effect classification, separate
from relational plans. Preserve local SQL mutation support while keeping unsupported
remote commands rejected; unifying representations must not expand transport authority.

### Liskov substitution

Contract suites must establish that an implementation meets its advertised capability:

- Analysis resolves metadata without scanning rows, invoking UDFs or causing writes.
- Lazy transformations do not execute prematurely; actions preserve column order,
  duplicate names, logical types, nullability, decimal scale, timestamps and empty schemas.
- `None` means a missing table; provider failures propagate. Owned namespaces never
  fall through to the local catalog after a missing table or provider error.
- A statement uses one source snapshot; resources remain live through consumption
  and are released on early drop, failure and cancellation.
- Cancellation has explicit safe points and commit boundaries. No interface promises
  immediate interruption of a non-interruptible engine call.
- A read-only provider is substitutable for a reader, not for a writer. Capability
  absence is reported before effects; advertised support cannot silently degrade.

Avoid `as_any` as the normal route for operations. Typed local frame operations use
`Self`/associated frame types and same-backend arguments. Where runtime erasure is
required, session/plan handles carry explicit ownership and reject mismatched handles
before effects. This refactor does not add cross-engine joins.

### Interface segregation

Split the current wide backend/provider interfaces by real consumer needs. Proposed
contract names are design labels to settle in the first contract PR:

| Contract | Responsibility and consumer |
| --- | --- |
| `FrameTransform`, `FrameActions`, `FrameSchema` | Typed local operations; query construction does not require collection or catalog mutation methods |
| `CatalogResolver` | Namespace ownership and table metadata; used by planning and mutation routing |
| `CatalogReadSnapshot` | Statement-consistent metadata and bounded batch scans; used by execution |
| `CatalogWriter` | Explicit writable-target capability and bounded writes; used by existing external writes and future DuckDB writes |
| `ExecutionSession` | Analyze/prepare supported plans and execute through an owned handle; server-facing, no Polars or wire types |
| `ExecutionFactory` | Create isolated execution sessions and report capabilities; used by runtime composition |
| `ResultStream` | Schema plus bounded typed IPC batches and cancellation/drop behavior; no requirement to collect all rows |

An execution handle retains its owning session and source snapshot until execution or
release. Preparing it performs no mutations; the runtime admits execution before
starting work. The adapter runs blocking engine work on the designated worker path,
while the runtime owns cancellation and consumption budgets. Replay stores opaque
payload bytes plus neutral sequence/outcome metadata; Connect protobuf objects stay
in the Connect crate. Specify these lifetimes before extracting the runtime.

Move the fixed `Vec<(i64, i64, String)>` constructor out of the session backend
contract into the ergonomic facade. Replace boolean-plus-default-error capabilities
with explicit optional reader/writer objects. Keep interfaces with one real production
consumer small; remove unused traits after the call-site audit.

### Dependency inversion

Inject execution and catalog capabilities at product/session construction. Pass an
explicit execution context containing configuration snapshot, cancellation and source
snapshot into planning/actions. Scope unavoidable Polars/thread-local integration
behind restoration guards; test nested calls and worker reuse so one session cannot
leak timezone, ANSI, decimal or UDF settings into another.

Define structured internal errors once, then map them at Python/HTTP/Connect boundaries.
Do not classify all plan failures as internal errors or infer control flow from error
strings. Preserve useful causes and documented public categories. A newly structured
public error API is an intentional 4.19 break with migration examples.

## 5. Storage boundary for future DuckDB writes

The new contracts must support the following now, using existing writable providers
and contract fixtures, without advertising DuckDB writes in 4.19:

1. Resolve **namespace ownership before table existence**. A missing DuckDB table is
   still owned by its configured source. Replace `is_read_only_target` as the sole
   routing signal with explicit owner/capabilities; unknown namespaces may use the
   local catalog, owned namespaces cannot silently do so.
2. Separate metadata/snapshot reads from writer acquisition. Capability policy can
   differ by configured owner and operation; “DuckDB” is not a permanent synonym for
   “read-only.” Select a writer only when the product, provider and requested operation
   all permit it.
3. Use bounded typed IPC input plus an explicit schema, including zero-row writes.
   Validate save mode, schema, options and target ownership before acceptance. Do not
   bridge engines by converting the entire write to Python/JSON rows.
4. Specify write phases: prepared, executing, commit accepted, terminal outcome.
   Require cancellation to be observed before irreversible commit acceptance; after
   acceptance, report the actual outcome rather than fabricating cancellation/rollback.
   A default implementation that checks cancellation after committing is insufficient.
5. Return a receipt tied to provider/target/operation identity. Distinguish committed,
   definitely not committed, and outcome unknown. Preserve exact Delta-style committed
   versions where meaningful; allow provider-specific commit identity instead of
   inventing a universal monotonic integer for DuckDB.
6. Keep transaction ownership in the storage adapter and request identity/retry policy
   in the server runtime. Repeated operation identity with changed input is rejected.
   Unknown commit outcomes are not blindly retried. Do not promise durable exactly-once
   execution from in-memory replay records.

Future DuckDB work implements the writer on this boundary and certifies local-owner
versus Quack/shared-owner transaction behavior. DDL/DML and full DuckDB query execution
still require their own operation/capability matrix; a table writer does not imply
arbitrary SQL mutation support. Read-only rejection, snapshots and cancellation remain
covered throughout 4.19. No new write capability is exposed by this refactor alone.

## 6. Execution sequence

Each row is a workstream, usually several reviewable PRs. Every PR builds and updates
its first-party call sites. Separate mechanical movement, contract changes and semantic
fixes. Transitional adapters may exist during a workstream, but must have an explicit
removal step before final acceptance.

| Phase | Work and concrete deliverables | Dependencies | Exit gate |
| --- | --- | --- | --- |
| **P0 — Baseline and decisions** | Snapshot dirty-tree diff/HEAD without resetting work; inventory public APIs/features/imports/CLI/protocols; record test failures/skips and benchmark baselines; create break ledger; revise D22/SS-23 and architecture ownership | None | Reproducible baseline and explicit intended changes; every current product/test lane has an owner |
| **P1 — Mechanical decomposition** | Extract Polars frame/session components and category modules; split PyO3 registration/conversion/classes; split server state modules within the current crate; preserve guard/drop/lock behavior | P0 | Existing targeted tests pass with no intended behavior differences; no broad visibility increases |
| **P2 — Core contracts and errors** | Introduce segregated frame/catalog contracts, owner-aware routing, explicit context and structured errors; migrate current Polars and Python provider implementations; remove ceremonial facade boxing/downcasts and fixed tuple trait constructor | P1 | Contract suites cover capability absence, namespace ownership, snapshots, write outcomes and cancellation; all first-party call sites migrated |
| **P3 — Plan and SQL crates** | Extract typed plan decoding/validation, common name/type semantics and SQL lowering; migrate JSON, local SQL and Connect incrementally; retain engine-specific optimization in Polars | P2 | Supported frontend corpus gives equivalent values/schema/errors; malformed plans rejected before source scans; old duplicate lowering removed |
| **P4 — Storage crates** | Extract Delta/JDBC driver responsibilities; split DuckDB config/resolution/snapshot/scan/connection lifecycle; wire providers through composition; migrate existing writes to bounded writer contract | P2; P3 where plan types are used | Driver feature matrix, external catalog tests, missing-table ownership, rollback/receipt cases, read-only DuckDB and source budgets pass |
| **P5 — Server runtime and transports** | Introduce server-core execution ports and Polars implementation; move runtime ownership then HTTP then Connect into new crates; move proto build with Connect; compose both products; remove original monolithic implementation | P2–P3; P4 for final source integration | Same session/admission/replay/mutation guarantees; runtime has no concrete-engine imports; HTTP-only, Connect-only and combined products pass |
| **P6 — Python API and packages** | Finish private SQL function subpackages and binding cleanup; split launcher modules; extract backend-independent testing package; remove approved legacy imports/packages; consolidate stubs/export checks | P2; P4–P5 for final product interfaces | Native rebuild + Python parity/types/import checks; clean local/client/base/DuckDB/combined installs; sync/async cleanup behavior preserved |
| **P7 — Automation, docs and release acceptance** | Enforce dependency rules; factor build/verification jobs and artifact inventory; migrate examples/docs; publish migration guide; remove transitional code; run final exact-artifact validation | P3–P6 | All required correctness, resource, packaging and migration gates pass on the final candidate |

Suggested PR boundaries within the largest phase, P5: execution port + Polars adapter;
session/admission/operation runtime; result/replay lifecycle; HTTP crate; Connect crate;
product composition and removal of legacy server implementation. Keep one ownership
model active throughout. Do not maintain two independent operation registries during
transport migration.

### Phase 1 implementation record — 2026-11-02

Phase 1 is implemented as behavior-preserving module extraction within the existing
crates. DataFrame range and IPC batching and file writing now live in `dataframe/batches.rs`
and `dataframe/writer.rs`; session input conversion and catalog state live in
`session/input.rs` and `session/catalog.rs`. The Polars function and UDF catch-all files
are divided into semantic modules while public functions remain re-exported from their
existing namespaces. The native binding separates conversion, DataFrame/Column APIs,
reader/writer classes, session APIs and module registration. Server state records and
RAII guards live in `sparkless-server-polars/src/state.rs`; the runtime remains in that
crate until P5.

The Polars crate passes `cargo check --locked -p robin-sparkless-polars --all-features`,
the native crate passes `cargo check --locked -p sparkless-native`, the server crate
passes `cargo check --locked -p sparkless-server-polars --all-features`, and
`cargo fmt --all --check` passes. Tests were not run in this implementation turn. The
P1 targeted-test exit gate and P0 inventory closeout were still pending when P2 began
at the user's direction. They remain open; Phase 2 implementation does not represent
P1 behavioral acceptance.

### Phase 2 implementation record — 2026-11-02

Phase 2 source work is implemented. Core frame, reader, and session contracts are
narrow and typed; the Polars facade no longer boxes frames for a downcast, and the
fixed tuple session constructor is gone. Catalog resolution now separates ownership
from metadata lookup and passes the resolved owner through the lookup/write path, so a
missing external table cannot fall through to a local table and ownership callbacks
are not repeated to fetch metadata. Reader, materialized writer, and batch writer
capabilities are independent. Writes use prepared commits with committed,
not-committed, and unknown outcomes; DuckDB remains read-only.

`ExecutionContext` captures operation ID, settings, cancellation, and an optional source
snapshot. Rust errors now carry serializable categories, codes, retryability, operation,
metadata, and cause; Python exceptions and HTTP error responses retain structured
details, while Connect maps categories to gRPC status and includes the stable error
code. First-party Rust and Python catalog providers now declare ownership. The
4.19-specific API replacements and examples are in [the migration guide](../MIGRATION_4_19.md).

`cargo fmt --all` and `cargo check --locked --workspace --all-features --all-targets`
pass. All targets were compiled; test binaries were not executed. The Phase 2
behavioral contract gate therefore remains open, as do the P1 targeted-test and P0
inventory gates. Do not treat these source changes as release acceptance until those
gates are recorded.

P4 and early Python module organization can progress independently after their
prerequisites. This describes dependency order, not a request to start parallel agents.
Estimate work only after P0 inventories API use and baseline failures; line counts alone
do not justify a credible calendar estimate. Do not cut 4.19 midway through an agreed
contract migration merely to meet a date.

## 7. Breaking-change ledger

Record exact removed symbols/signatures and replacement examples during P0/P2. The
following are the default decisions for the refactor, subject to evidence from that
inventory rather than an automatic requirement to preserve compatibility:

| Surface | Planned change | Migration |
| --- | --- | --- |
| Rust facade | Remove `EngineDataFrame` and generic-to-concrete adapters; reorganize backend-specific exports | Use concrete local frame methods; use core contracts for engine-neutral consumers |
| Backend implementation traits | Remove `DataFrameBackend`, boxed frame returns, `as_any` frame matching, and tuple-only `SparkSessionBackend::create_dataframe`; add associated typed frames and `create_dataframe_from_rows` | Implement `FrameTransform`, `FrameActions`, `FrameSchema`, associated reader/frame types, and the JSON row constructor |
| Rust session catalog setter | Remove `SparkSessionBackend::set_external_catalog_provider`; add `set_external_catalog(Option<Arc<ExternalCatalog>>)` | Compose the resolver and optional reader/writer ports, then attach the catalog |
| External catalogs | Remove `ExternalCatalogProvider`, `supports_batch_writes`, and the combined read/write trait; `ExternalCatalog::resolve_table` now takes the resolved `&CatalogOwnership` | Resolve ownership first, use separate reader/writer capabilities, and implement prepared-write outcomes |
| Python external catalogs | Require `resolve_ownership(identifier)`; add `ExternalCatalogOwnership`; add `operation_id` and provider commit identity to write request/receipt types | Return `None` for local ownership or an owner with capabilities; use `request.operation_id` to reconcile ambiguous commits |
| Plan API | Replace unvalidated JSON throughout execution with a typed plan boundary | JSON callers use decoder then typed executor; unsupported/legacy aliases get explicit migration errors |
| Errors | Replace enum-shaped `EngineError::{User,Internal,Io,Sql,NotFound,Cancelled,Other}` with the structured `EngineError` type and lower-case constructors | Use `invalid_input`, `internal`, `io`, `sql`, `not_found`, `cancelled`, `other`, then branch on category/code and inspect `detail()` |
| Server Rust library | Move runtime, transport and adapter symbols to owning crates | Product constructors remain entry points; embedders import the required layers explicitly |
| Python native module | Make `sparkless_native._native` the canonical internal extension location; remove obsolete aliasing if no retained public contract needs it | Public users import `sparkless`; direct internal consumers migrate to the documented extension path |
| Python server helpers | Retire `sparkless.server` and `sparkless-server-bin` compatibility forwarding from the new release graph | Import `sparkless_connect`, `sparkless_server` or `sparkless_server_duckdb` explicitly |
| Python testing | Move canonical helpers/plugin to `sparkless_testing` if independent-package gate passes | Install `sparkless-testing` / `sparkless[testing]`; update imports and `pytest_plugins` |
| Rust source features | Remove obsolete source forwarding edges where explicit provider construction replaces them | Enable/install the provider crate and register its capability at composition |

Preserve familiar PySpark-facing operation names unless a specific break materially
improves the design. Removing a duplicate implementation does not require renaming
`select`, `filter` or `sparkless.sql.functions`.

Changing the standard Spark Connect wire protocol is not an architectural cleanup.
Keep the pinned unmodified client working. If first-party HTTP/control payloads must
change, version them and fail mismatched clients clearly; never silently reinterpret
an existing protocol version. Coordinate Python launcher/binary versions.

Release notes must prominently state that 4.19 contains breaking changes despite its
minor-version number. Update existing version-support claims accordingly. Already
published historical packages remain immutable; removing a compatibility distribution
means omitting it from the new graph, not deleting old uploads. Verify publication
state at release time and follow the existing partial-release recovery policy.

## 8. Verification and architectural gates

Baseline failures are recorded, not silently repaired by weakening assertions or adding
skips. Differential runs compare supported capabilities only, using pinned PySpark
profiles and the existing Connect oracle environments. Rebuild native wheels after
Rust changes before trusting Python results.

| Area | Required evidence |
| --- | --- |
| Core/provider contracts | Ownership, missing versus error, absent capabilities, snapshot consistency, empty schema, cancellation safe points, bounded consumption, exact/unknown write outcomes |
| SQL/plan/expressions | Existing parity fixtures; decimal/ANSI/overflow, null/NaN, timezone, duplicate columns, aliases, joins, aggregation/window and ordering behavior; frontend-specific unsupported errors |
| Native/API | Existing Python suites, external catalog callback tests, GIL/callback lifetime, action cancellation, Arrow/pandas conversion, class/module identity, signatures and stubs |
| Session/context | Clones versus new sessions, temp/global views, catalog sharing, persistence, nested execution, UDF configuration and thread reuse |
| Server | Existing `phase1_http`, `control_protocol`, inline lifecycle tests and Connect smoke/fault-proxy/slow-reader tests; duplicate identities, replay/release, disconnect, deadline, shutdown and mutation races |
| Storage | Delta/JDBC feature combinations, driver error/transaction behavior, DuckDB snapshots/owner loss/connection queues, unsupported writes and missing-table namespace ownership |
| Resources/performance | Existing filter/select/groupby, window and 4.19 count/export/source/xdist benchmarks; validate results plus wall time, allocations/RSS, queue/replay bytes and cancellation latency |
| Distribution | Isolated dependency graphs, wheel contents/native libraries, clean installs and missing optional dependencies, both products coexisting, examples using installed artifacts |

Set numeric latency/RSS thresholds from P0 runs on a fixed host and retain existing
hard resource budgets. Use repeated runs and report variance. Require investigation
for a repeatable >5% median latency or peak-memory regression where measurement noise
is lower than that threshold; larger variance requires a better measurement, not an
automatic pass. No new eager materialization in formerly lazy/bounded paths.

Existing commands to use selectively during implementation, and as applicable at the
final gate (these were not run as part of writing this plan):

```bash
cargo fmt --check
cargo clippy --locked --workspace --all-features --all-targets -- -D warnings
cargo test --locked -p robin-sparkless-core
cargo test --locked -p robin-sparkless-polars --features sql
cargo test --locked --release -p sparkless-server-polars --no-default-features --features http,connect
cargo test --locked --release -p sparkless-server --no-default-features --features http,connect --test control_protocol --test phase1_http
uv run --no-project --python .venv/bin/python python -m pytest tests/parity/ -q
uv run --no-project --python .venv/bin/python python -m pytest tests/unit/test_external_catalog_provider.py -q
make lint-python
python3 scripts/check_documentation.py
```

Update package/feature/test locations as extraction moves them. Add equivalent test
commands for every new crate; do not leave CI testing only legacy facades. Retain the
full feature, Delta, database integration, Python and Connect lanes required by CI and
release. `make check-full` currently cleans the target directory before rebuilding;
use targeted commands for iteration and retain full acceptance at milestone boundaries.

Automate dependency assertions using Cargo metadata/feature graphs and Python installed
distribution/import checks. Verify product graphs in isolated consumer projects/builds:
workspace feature unification can otherwise hide an unwanted DuckDB dependency. Assert
absence as well as presence: core contains no engine/transport; runtime contains no
Polars/DuckDB/Python; base artifacts contain no DuckDB/Quack; client-only installs contain
no local engine or server payload; server-only installs contain no native local extension.

Refactor release automation only after product manifests settle. Share build/verify
jobs and inventory parsing, while keeping signing/publication permissions narrowly
scoped. Preserve immutable artifact hashes, dependency publication order, required-job
acceptance and recovery semantics. Do not rewrite historical recovery workflows as a
side effect of current-release cleanup. Keep vendor/security exceptions independently
reviewable and run the existing audit/deny checks.

Update workspace manifests, crate includes, Python build metadata, lockfiles, Docker
build contexts and both release Dockerfiles for the new graph. Check base/DuckDB image
contents and runtime dependencies separately, including Quack notices. Keep the test
compatibility-matrix container template and `docker-compose.yml` aligned with the
driver/service lanes. Preserve script entry points where external automation uses
them, or list their replacement commands in the breaking-change ledger. Keep examples
and benchmark harnesses calling public APIs rather than reaching into new internals.

## 9. Completion criteria and risk controls

- Every first-party area has an explicit owner and dependency direction. New crates
  enforce boundaries instead of merely distributing a monolith across files.
- Root and binding registration modules primarily compose/export; frame/session/server
  modules no longer own unrelated IO, conversion and lifecycle policy. Use a review
  trigger around 800 production lines for a new module, not an arbitrary hard cap or
  a reason to scatter cohesive code.
- Normal facade operations have no box/downcast round trip. Production read-only
  providers do not implement fake writer methods. Structured failures survive all
  adapter layers.
- Supported behavior passes the baseline/regression corpus. Planned breaks are listed
  with runnable migrations; removed duplicate paths are actually deleted.
- Future DuckDB writes fit the owner/writer/outcome boundary without revising read
  contracts or transport lifecycle. DuckDB remains truthfully read-only in this release.
- Every supported feature/product combination builds and installs independently.
  Docs, examples, stubs, package metadata and capability reports agree with the code.
- Final candidate artifacts pass existing release gates plus the new architecture and
  migration gates. Planning or source tests alone do not establish release readiness.

The highest risks are lifecycle races during server extraction, semantic drift during
translator consolidation, changed Python class/exception identity, hidden feature
unification, and unbounded materialization while moving storage code. Address these
with existing behavioral tests plus targeted contract/race tests, not tests that merely
assert the new file layout. Keep mechanical moves independently revertible. If a
workstream fails its gates, revert that workstream before releasing; do not ship two
permanent implementations or claim the whole refactor is complete.
