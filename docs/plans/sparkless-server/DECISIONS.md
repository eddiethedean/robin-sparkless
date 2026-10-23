# Decision register

[Plan index](README.md) · [Epic](EPIC.md) · [Validation](VALIDATION.md)

**Status:** decisions below are open except for D05's coordinated package-version policy, resolved by the [4.16+ release roadmap](ROADMAP.md). All first-party packages publish together at 4.16.0, then 4.17.0, and so forth, including `spark-sql-parser`. D05's platform, packaging, and publication implementation choices remain open. Owners, evidence, and implementation issue/PR links are unassigned. The Spark 4.0 family is an evaluation target, not a declared tested patch version.

Do not replace an unresolved value with a guessed version or silently treat a recommendation as implemented. Record resolution date, owner, exact immutable revisions, rationale, impacted plans, and validation evidence here.

## 4.16 implementation baseline

The [4.16 implementation plan](RELEASE_4_16.md) proposes concrete choices for D02/D04 (restricted SQL, explicit server semantics, exact scalar results), D06 (owned JSON-lines control), D07/D08 (operation accounting and bounded defaults), D09 (read-only ephemeral sessions), and the HTTP scalar slice of D11. These are design baselines to verify in slices 416-01 through 416-08, not completed compatibility decisions or test evidence. Record deviations and their impact before freezing the contracts. All unrelated platform/version choices remain open; the shared 4.16.0 package-version policy is already fixed.

## Decisions required by milestones

| ID | Decision | Required by | Responsible workstream | Resolution requirements |
| --- | --- | --- | --- | --- |
| D01 | Client/proto/oracle compatibility manifest | 0A | SS-01, SS-03 | Exact pyspark-client release, matching proto tag/commit, Python range, gRPC/protobuf/PyArrow locks, generator version, reference Spark/Sail versions and settings |
| D02 | SQL/configuration semantics | 0A | SS-01, SS-08 | Explicit engine profile, ANSI/case sensitivity/timezone/decimal behavior; mutable versus read-only configuration; unsupported keys fail |
| D03 | Emulator prerequisite and trust scope | 0A tracking; 0B integration | SS-01, SS-07 | External owner and issue/PR, typed-result fix, exact revision/image digest and SDK version; shared sql-warehouse identity limitations |
| D04 | Supported SQL/type/JSON contract | 0A specification; 0B verification | SS-01, SS-05, SS-08 | Allowlisted read-only nodes/functions, logical schema JSON, exact integer/decimal/date/time/null encodings, duplicate labels, non-finite float policy |
| D05 | Binary/platform/release matrix | 0B | SS-02, SS-06, SS-11 | Supported OS/arch/libc targets, runtime dependencies, artifact verification, package publishing order and binary-extra compatibility |
| D06 | Launcher control and compatibility | 0B | SS-02, SS-06 | Message framing/size, per-launch identity, endpoint readiness, version negotiation, Windows/POSIX graceful shutdown, cleanup and fork restrictions |
| D07 | Operation/transport state and retention | Prototype in 0A; production base in 0B; replay in 2 | SS-03, SS-04, SS-10 | State transitions, actual completion ownership, deadlines, stable identities, reattach/release, expiry and restart behavior |
| D08 | Resource limits and enforcement | 0B base; replay extension before 2 | SS-04, SS-10, SS-11 | Concrete defaults for sessions/active work/queues/input/decoded data/output; Polars thread sizing; enforced versus cooperative budgets; replay byte/TTL limits |
| D09 | Catalog lifetime and persistence | Session slice in 0B; shared catalog in 1 | SS-07, SS-13 | Reserved session lifetime, catalog scope, temp-view isolation, storage/metadata ownership, visibility, restart durability and recovery |
| D10 | Mutation outcomes and retries | Before enabling writes in 1 | SS-07 | Operation/idempotency identity, duplicate handling, catalog locking/snapshots, commit receipts, unknown outcome and restart limits; external adapter extension |
| D11 | Logical schema and Arrow boundary | Minimal scalar path in 0B; full enabled matrix before 2 | SS-08, SS-09, SS-10 | Metadata preservation, physical/logical mapping, IPC implementation/version, empty batches, byte limits, local relation decode expansion/artifact threshold |
| D12 | Python worker security and packaging | Before Phase 3 implementation | SS-12 | Explicit opt-in scope, worker API, code trust, OS isolation, quotas, credentials, environment, results, and process-tree ownership |
| D13 | Native Delta/UC compatibility | Before Phase 4 implementation | SS-13 | Library/storage/catalog versions, credentials policy, supported operations, conflict/transaction semantics and independent reader |

## Required compatibility record

Populate the following together at the 0A gate; file locations may be chosen during implementation.

| Component | Exact version/revision | Lockfile/artifact/evidence |
| --- | --- | --- |
| Native engine and server source | Unselected | Pending |
| Rust toolchain/MSRV | Follow root rust-toolchain.toml and Cargo.toml; capture exact values at build | Pending build evidence |
| PySpark client and Python | Unselected | Pending |
| Spark Connect protocol sources/compiler | Unselected | Pending |
| gRPC/protobuf/PyArrow dependencies | Unselected | Pending |
| Polars/Arrow serialization boundary | Unselected | Pending |
| Databricks SDK | Unselected | Pending |
| Corrected emulator and image digest | Unselected | Pending external dependency |
| Reference Spark/Sail and settings | Unselected | Pending |
| Launcher control and HTTP envelope versions | Unselected | Pending |

## Fixed constraints

These are requirements, not open design choices:

- Direct native Rust execution; no Python/JVM in default SQL/Connect service paths.
- Loopback default; explicit deployment trust boundary for remote/container access.
- Read-only initial HTTP surface; unsupported operations return named failures.
- Exact typed SDK results, including BIGINT values beyond binary64's exact integer range.
- Unmodified Connect client defaults, including reattachment, in conformance tests.
- Native work retains admission/accounting until it actually finishes.
- No hard in-process per-query CPU/RSS or durable exactly-once promise without the required implementation.
- Minimum session and scalar/schema work ships with 0B.
- Standalone Connect conformance precedes proxy certification.

## Resolution record template

Copy this block when a decision is resolved:

- Decision ID and date:
- Owner and implementation issue/PR:
- Selected behavior and exact versions:
- Alternatives considered and reason:
- Affected contract/plan documents:
- Tests, immutable source revision, and evidence:
- Remaining limitations:

A decision can have staged resolutions (for example, D07's feasibility and production stages). Record each stage; do not mark later stages complete because the prototype passed.
