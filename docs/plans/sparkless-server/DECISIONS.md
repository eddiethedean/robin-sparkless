# Decision register

[Plan index](README.md) · [Epic](EPIC.md) · [Validation](VALIDATION.md)

**Status:** D01 now has a pinned Spark 4.0.4 / `pyspark-client` 4.0.4 candidate and a CPython 3.11 dependency lock in [COMPATIBILITY_4_16.json](COMPATIBILITY_4_16.json) and [requirements-connect-4.0.4.lock](../../../scripts/server-tests/requirements-connect-4.0.4.lock); it remains incomplete until Connect feasibility is exercised. D03 remains unresolved for optional certification on a corrected, immutable external emulator build and owner. D01/D03 and experimental Connect do not block 4.16 publication; the required native gates are V05–V14 and V29. Other decisions remain open except for D05's coordinated package-version policy, resolved by the [4.16+ release roadmap](ROADMAP.md). All first-party packages publish together at 4.16.0, then 4.17.0, and so forth, including `spark-sql-parser`. D05's platform, packaging, and publication implementation choices remain open. Owners and evidence links remain unassigned where not recorded below.

Do not replace an unresolved value with a guessed version or silently treat a recommendation as implemented. Record resolution date, owner, exact immutable revisions, rationale, impacted plans, and validation evidence here.

## 4.16 implementation baseline

The [4.16 implementation plan](RELEASE_4_16.md) proposes concrete choices for D02/D04 (restricted SQL, explicit server semantics, exact scalar results), D06 (owned JSON-lines control), D07/D08 (operation accounting and bounded defaults), D09 (read-only ephemeral sessions), and the HTTP scalar slice of D11. These are design baselines to verify in slices 416-01 through 416-08, not completed compatibility decisions or test evidence. Record deviations and their impact before freezing the contracts. All unrelated platform/version choices remain open; the shared 4.16.0 package-version policy is already fixed.

## Decisions required by milestones

| ID | Decision | Required by | Responsible workstream | Resolution requirements |
| --- | --- | --- | --- | --- |
| D01 | Client/proto/oracle compatibility manifest | Optional integration/Connect certification | SS-01, SS-03 | Candidate pinned: `pyspark-client==4.0.4`, Spark protocol/reference tag `v4.0.4` at `c7d67e3f5d4c9d88a480367b44fc54d26adf99ab`, CPython 3.11 test lock; remaining: generator provenance and executed client/reference evidence |
| D02 | SQL/configuration semantics | 0A | SS-01, SS-08 | Explicit engine profile, ANSI/case sensitivity/timezone/decimal behavior; mutable versus read-only configuration; unsupported keys fail |
| D03 | Optional emulator integration and trust scope | Before advertising a certified emulator route | SS-01, SS-07 | Blocked: inspected upstream commit `05044929caf78a7f2155d8de218d3e3e4c53a173` still maps typed agent JSON to `result.text`; needs external owner/issue/PR, typed-result fix, immutable corrected revision/image digest, SDK integration, and documented shared `sql-warehouse` identity limitations |
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
| D14 | MiniLake runner/catalog/delegation interoperability | Before advertising Phase 2 Jobs; storage/DML extensions before their Phase 4 claims | SS-01, SS-11, SS-13, SS-14 | Pin MiniLake wheel/source and runner image/client; validate zero-upstream-change seams, separate route identity, JVM-free builds/runtime, lifecycle limits, native catalog/snapshot/file policy, and any optional upstream hook |

## Required compatibility record

Record native build/transport versions for the HTTP release. Client/protocol/SDK/emulator/reference records below are for optional integration certification and do not block native publication.

| Component | Exact version/revision | Lockfile/artifact/evidence |
| --- | --- | --- |
| Native engine and server source | Unselected | Pending |
| Rust toolchain/MSRV | Follow root rust-toolchain.toml and Cargo.toml; capture exact values at build | Pending build evidence |
| PySpark client and Python | Candidate pinned: `pyspark-client==4.0.4`; protocol test runtime CPython 3.11; client package supports Python >=3.9 | [Compatibility manifest](COMPATIBILITY_4_16.json), [test lock](../../../scripts/server-tests/requirements-connect-4.0.4.lock); run evidence pending |
| Spark Connect protocol sources/compiler | Apache Spark `v4.0.4`, commit `c7d67e3f5d4c9d88a480367b44fc54d26adf99ab`; compiler/provenance not yet recorded | [Compatibility manifest](COMPATIBILITY_4_16.json); generation pending |
| gRPC/protobuf/PyArrow dependencies | Candidate test lock records exact resolved versions for CPython 3.11 | [test lock](../../../scripts/server-tests/requirements-connect-4.0.4.lock); cross-platform resolution and run evidence pending |
| Polars/Arrow serialization boundary | Unselected | Pending |
| Databricks SDK | Unselected | Pending |
| Corrected emulator and image digest | Not available; inspected upstream commit `05044929caf78a7f2155d8de218d3e3e4c53a173` is known not to satisfy typed results | [Compatibility manifest](COMPATIBILITY_4_16.json); external fix and immutable image digest blocked |
| Reference Spark/Sail and settings | Spark 4.0.4 / `v4.0.4` commit `c7d67e3f5d4c9d88a480367b44fc54d26adf99ab`; ANSI on, case-insensitive, UTC | [Compatibility manifest](COMPATIBILITY_4_16.json); oracle run pending |
| MiniLake wheel/source and integration image | Unselected; SS-14 records a source-inspection baseline only | V31–V36 pending; separate from the corrected HTTP emulator |
| MiniLake Python-only task runner and client locks | Unselected | Pending D14 implementation/build/runtime evidence |
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
- Standalone Connect conformance precedes proxy certification and separate MiniLake Jobs certification.
- MiniLake integration targets no source changes for initial Jobs/external-table reads, with no Java/JVM, Apache Spark runtime, or classic fallback in its deployment/build recipe. Its SQL warehouse remains DuckDB; its external Python runner does not require SS-12. These scope constraints are fixed; D14 implementation choices and test pins remain open.

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
