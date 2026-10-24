# 4.16.0 implementation and release plan

**Status: the 4.16.0 artifact set is partially published, but the release is incomplete. V29 and V30 have not passed.** The annotated tag exists and its original release run passed its recorded pre-publication checks, but initially failed to publish `sparkless-server-bin` to PyPI. The guarded [recovery workflow](../../../.github/workflows/recover-4.16.0.yml) later published the PyPI files, crates.io crates, and GHCR image. The final [recovery run](https://github.com/eddiethedean/robin-sparkless/actions/runs/40143218104) failed its fresh public `sparkless[server]==4.16.0` install check: the immutable `sparkless-server-bin` wheel does not export the `executable_path` API required by the launcher. No GitHub Release record was created.

The public PyPI inventory currently contains nine `sparkless==4.16.0` files and four `sparkless-server-bin==4.16.0` wheels; all five first-party crates and `ghcr.io/eddiethedean/sparkless-server:4.16.0` are also published. Public inventory checks pass, but the server-extra install fails on the missing launcher API. PyPI files and registry versions are immutable, so correcting that package requires a new coordinated patch version under the [release policy](ROADMAP.md#coordinated-version-and-publication-policy), such as 4.16.1. Until a corrected 4.16-line release passes its candidate and public-install gates, the 4.16 prerequisite for 4.17 is unmet. The current workspace manifests still report 4.15.3.

[Roadmap](ROADMAP.md#4160-http-service-launcher-and-unified-distribution) · [Epic](EPIC.md) · [Workstreams](README.md#workstreams) · [Decisions](DECISIONS.md) · [Validation](VALIDATION.md)

## Release outcome

A fresh `sparkless[server]==4.16.0` installation on an advertised platform can launch an owned native Rust child, execute the declared read-only SQL subset through HTTP, and clean up with synchronous or asynchronous lifecycle methods. An unmodified Databricks SDK receives exact values and schema through the corrected pinned emulator. Non-Python consumers can install the Rust CLI or use its native image.

The public server has no supported Spark Connect endpoint in 4.16.0. The required Phase 0A Connect experiment remains test-only and feature-gated. Catalog mutations, file/network readers, Python execution, nested types, and production gRPC remain assigned to their later gates. Existing embedded Rust/Python APIs retain their supported behavior; the restricted surface applies to remote server execution.

## Implementation baseline

The choices below are the proposed 4.16 baseline for implementation. Verify them in the relevant slice before marking a decision resolved. Exact third-party pins, binary platform tags, and tested resource defaults still need recorded evidence; they are not inferred from this document.

| Area | 4.16 baseline |
| --- | --- |
| Native service | `crates/sparkless-server`, direct `robin-sparkless` dependency with SQL; HTTP enabled by default |
| Experimental Connect | Separate feature and test harness; disabled in normal binaries, images, readiness, and launcher URLs |
| Bind | CLI `127.0.0.1:8099`; launcher `127.0.0.1:0`; explicit override for trusted container networking |
| Configuration | Defaults, then documented environment settings, then explicit CLI options; invalid/zero/contradictory limits fail startup |
| SQL semantics | Explicit server-local 4.0 compatibility profile, ANSI enabled, case-insensitive resolution, fixed UTC; no remote `SET` or process environment mutation |
| Sessions | Opaque ID, fixed configuration, bounded registry; one active operation per session; no durable catalog |
| Results | Bounded complete HTTP JSON envelope with positional cells and Spark-compatible logical schema |
| Cancellation | Deadline/disconnect requests cooperative cancellation; operation owns its permit until native execution and its producer end |
| Python discovery | Exact package-resource binary or explicit executable path; no download, build, or arbitrary PATH fallback |
| Package coupling | `sparkless[server]` pins `sparkless-server-bin==4.16.0`; launcher validates package/binary version and independently versioned control protocol |
| Candidate server binary targets | `macosx_14_0_arm64` and `manylinux_2_28_x86_64`; only advertise after native wheel install/start/health/shutdown gates pass |
| Candidate native image | Single `linux/amd64` image tagged `v4.16.0`; only publish after a non-root HTTP SQL and graceful-stop smoke passes |

Do not use `set_var` inside the multithreaded Rust service to reconfigure Polars. Thread-pool settings must be supplied before pool initialization and measured under concurrent load. Configuring a server session must not change the embedded engine's default semantic profile.

## Ordered delivery slices

Slice IDs are implementation planning IDs, not GitHub issues or claims of merged code. Give each slice an owner, reviewable change, and evidence link when work starts. A slice may span more than one PR; avoid combining protocol discovery, engine repairs, and publication into one change.

| Slice | Deliverable | Dependencies | Exit evidence |
| --- | --- | --- | --- |
| **416-01** | Freeze compatibility, SQL/result contract, package inventory, and external emulator work | None | D01–D04 baseline; SDK decoding fixtures; upstream owner/issue/PR recorded |
| **416-02** | Minimal server crate, CLI/config types, protocol-generation test boundary | 416-01 | Direct Rust linkage, feature separation, clean-package build investigation |
| **416-03** | Native Connect feasibility with the unmodified client defaults | 416-01, 416-02 | V03/V04: computed witness, typed empty result, reconnect, release trace |
| **416-04** | Bounded executor, operation ownership, read-only sessions, drain/control state | 416-02; incorporate 416-03 findings before freezing interfaces | V09–V11 core tests; a timed-out native task retains its permit |
| **416-05** | Scalar logical schema/positional export and SQL allowlist | 416-01, 416-02 | Engine fixture corpus passes, including duplicate labels and exact decimals/BIGINT |
| **416-06** | HTTP agent wired to the real engine/runtime | 416-04, 416-05; 416-03 feasibility complete | V05–V11 direct-agent tests; readiness follows a real native witness |
| **416-07** | Sync/async launcher and binary distribution | 416-02 control contract, 416-04 shutdown, 416-06 readiness | V12–V14 across advertised launcher/binary platforms |
| **416-08** | Integrated SDK/emulator, locked release inventory, candidate artifacts/CI | Corrected emulator; 416-01 through 416-07 | V01–V14 plus V29; all 4.16.0 candidate artifacts install and run |
| **416-09** | Coordinated publication and public artifact verification | 416-08 and every pre-publication gate | V30: all inventoried artifacts published and verified at 4.16.0 |

After 416-02, runtime work, native schema work, and the external emulator fix can proceed independently. The launcher can develop against the agreed control contract before HTTP completion; its end-to-end gate waits for 416-06. The Connect feasibility test must not expand into the full Phase 2 translator or replay implementation.

## 416-01: compatibility and external dependency

Record one exact client/proto tag for the feasibility test, one exact SDK version, the emulator source revision/image digest, and reference-engine settings in [D01–D04](DECISIONS.md). Distinguish the server version from its semantic and transport compatibility identifiers.

The external emulator change must preserve the agent's schema and exact cells, then construct public `manifest.schema` and `result.data_array`. Use real SDK model decoding in fixtures, including BIGINT `9007199254740993`, a 38-digit decimal, null, zero rows, and duplicate column names. Neither `result.text` decoding nor a fixture that bypasses the emulator adapter satisfies the gate.

Track the external implementation and its immutable test revision as a release dependency. Direct-agent work can continue while it is pending, but **4.16.0 is not ready to publish until the integrated SDK gate passes**.

## 416-04: runtime, sessions, and bounded defaults

Use a fixed-capacity admission queue and a bounded blocking executor. A validated request reserves admission before engine planning; bounded transport reading/decoding and cheap structural validation must also be accounted for. Keep an operation record alive until producers actually exit, including after a response deadline or disconnected client.

### Initial defaults to validate

These are proposed defaults for trusted local usage, not performance claims or hard native-memory limits. Verify saturation and boundary cases before freezing them in D08.

| Setting | Proposed default | Enforcement |
| --- | --- | --- |
| Active native operations | 2 per process, 1 per session | Admission ownership through actual completion; no unbounded threads waiting on session locks |
| Pending operations | 16 process-wide, 4 per session | Queue capacity includes requests waiting for session access; excess rejected immediately |
| Queue wait | 5 seconds | Remove expired work before it starts |
| Response deadline | 30 seconds from acceptance, including queue wait | Ends waiting and requests cancellation; does not kill running native code |
| Sessions | 64, including the reserved warehouse session | Bounded creation; no eviction of active references |
| Idle TTL | 15 minutes for ordinary sessions; `SPARKLESS_SESSION_IDLE_TTL_MS` overrides it | Reaper runs at most every 30 seconds; expire only when queued/running work no longer holds a reference; preserve the reserved `sql-warehouse` session |
| Live statement HTTP requests | 32 | Nonblocking reservation before body reading; keep through response disposal; health/control have separate bounded capacity |
| Request body / SQL text | 1 MiB / 256 KiB | Bound before parsing; reject compressed request bodies initially |
| Body read / result send wait | 5 / 5 seconds | Bound slow clients; body timeout is a transport error, not an engine cancellation result |
| SQL structure | Depth 64, 10,000 AST nodes, 256 output columns | Bounded parse/validation path before engine work; include string/identifier limits |
| Output | 10,000 rows, 8 MiB complete encoded envelope, 1 MiB encoded single cell | Incremental row/byte accounting, including schema/JSON escaping; oversized results fail before success headers |
| Retained result envelopes | 2 process-wide | Reserve before production, retain until sent/dropped; bounds encoded success buffers to 16 MiB independently of compute permits |
| Startup / drain / kill wait | 30 / 10 / 5 seconds | Lifecycle deadlines and reaping; report delayed native completion honestly |
| Launcher diagnostics | 64 KiB retained tail per child | Continuous draining and bounded framing; no SQL or secret-bearing environment dumps |

Bytes mean encoded bytes unless explicitly stated otherwise. A single large internal allocation can precede an encoded-size check; these limits do not constitute a hard intermediate-memory guarantee. Limit validation and error responses must not allocate proportional to rejected input beyond the declared transport budget.

Queue scheduling must allow other eligible sessions to progress while one session has active work. A request that waits for a session slot consumes its pending budget, not an active compute worker. Reserve health/control servicing independently from statement saturation.

Perform SQL parsing, AST validation, planning, and result encoding within bounded compute admission; cheap HTTP envelope validation alone may precede it. Set a parser recursion budget before creating an AST, then enforce node/column counts after parsing. An HTTP request permit, a native compute permit, and a retained-result reservation have different lifetimes: disposing of a response cannot release a running native task's lease, and native completion cannot free a buffer still being sent.

Create `sql-warehouse` lazily and keep it exempt from idle expiry while the service runs; explicit close is still allowed. Close marks a session closing, stops new admission to that generation, and cancels queued/active work cooperatively. Repeated close succeeds; recreating the same opaque ID starts a new generation only after old work releases its references. Return an explicit closing-session error while draining. Session close never implies that running native work was forcibly terminated.

Use fixed UTC/profile configuration and no server-owned catalog data in the production 4.16 surface. Session-isolation tests can seed independent native fixtures through Rust test setup; do not add a public fixture or table-registration endpoint to make those tests pass.

## 416-05: explicit read-only SQL and result contract

### Required SQL subset

Accept exactly one parsed query. A trailing semicolon is allowed; multiple statements are rejected in full. Permit `SELECT` with literal/typed-literal projections, aliases, safe column references from a derived constant query, `CAST` to supported scalar types, basic arithmetic/comparison/boolean predicates, `IS NULL`, `WHERE`, column-based `ORDER BY`, and nonnegative literal `LIMIT`. Permit bounded `UNION ALL` of such queries for multirow fixtures. Validate every nested query/expression; an outer `SELECT` does not make arbitrary descendants safe.

Support constant/derived relations without attaching file readers or external catalog resolvers. Reject named tables, table-valued functions, general function calls/UDFs, scalar subqueries, joins, CTEs, grouping/windows, `SELECT INTO`, locks, offsets/fetch extensions, and unknown clauses until separately enabled. Reject all DDL/DML, `SET`/`USE`, arbitrary Python, and client-controlled file/network access. Do not rely on the engine parser's broad accepted-statement set as the remote allowlist.

Native gaps within this subset are 4.16 prerequisites. Unknown or partially implemented syntax fails before execution; no clause may be ignored while returning success. Keep validation and the executed AST consistent rather than validating one interpretation and executing a different one. Avoid adding a second general SQL engine in the server adapter.

### Required scalar results

Return BOOLEAN, SMALLINT, INT, BIGINT, finite DOUBLE, STRING, DATE, fixed-UTC/microsecond TIMESTAMP, and DECIMAL(1–38, scale 0–precision), plus null values declared with one of these types. Bare untyped null, non-finite floats, FLOAT32/BYTE/NTZ, binary/nested values, unsupported decimal scales, and unknown physical types fail explicitly in 4.16.

Construct the logical Spark schema with ordered fields, declared type/precision/scale, nullable flags, and metadata. Preserve query-derived nullability even for zero rows; scanning result values cannot recover it. Where native metadata is insufficient, add the minimum logical-schema support in the native API. Do not label all fields nullable as a shortcut to the exact-schema gate.

Export values by column index using bounded native batches. Do not call the name-keyed JSON-row APIs for positional results or eager whole-query JSON collection for bounded output. Preserve duplicate public labels using unique internal names and a positional logical-schema mapping; verify ordering and references through aliases before enabling that path. Internal aliases must not escape into public schemas.

The agent JSON contains exact integer numbers, booleans, strings, null, decimal strings, ISO dates, and UTC timestamps with a documented six-digit fractional precision and `Z` suffix. The emulator translates non-null cells into its SDK JSON_ARRAY string encoding without routing numbers through floating point. Preserve decimal scale, including trailing zeros.

### Minimum golden corpus

| Query / scenario | Required assertion |
| --- | --- |
| `SELECT 1 AS result` | One INT cell and matching non-null logical field |
| `SELECT CAST(9007199254740993 AS BIGINT) AS n` | Exact BIGINT through both HTTP and SDK |
| `SELECT CAST('1234567890123456789012345678901234.5678' AS DECIMAL(38,4)) AS d` | Exact decimal text, precision 38 and scale 4 |
| `SELECT CAST(NULL AS BIGINT) AS n` | Null cell and nullable BIGINT schema |
| `SELECT CAST(NULL AS BIGINT) AS n WHERE 1 = 0` | Empty row array with the same logical schema |
| `SELECT 1 AS x, 2 AS x` | Two positional cells and two logical fields named `x` |
| Typed date and timestamp casts | Exact date/microseconds, UTC representation, declared logical types |
| Derived constant query, UNION ALL, ORDER BY, LIMIT | Correct rows/ordering and native computation |
| Cast failure / division by zero / overflow | Declared ANSI behavior; no fabricated null/success |
| Read query containing a rejected descendant or unsupported clause | Named error before engine execution |

Run each applicable golden case through direct HTTP and the real SDK/emulator route. Compare logical schemas against the pinned reference settings; normalize only documented transport representation differences. Record discovered native fixes rather than weakening the expected schema.

## 416-06: HTTP behavior

| Route | Contract |
| --- | --- |
| `GET /health` | 200 only after listener availability and a native startup query verifies schema/value; 503 while starting/draining/unavailable; no SQL on each probe |
| `GET /capabilities` | Exact enabled SQL/type/agent versions, configured limits, cooperative cancellation limitations, no supported gRPC advertisement |
| `POST /statements` | Required bounded `session`, `kind`, and `code`; valid execution requests return the agent envelope |
| `POST /close` | Bounded session identifier; idempotent cleanup with the session-generation rules above |

Keep a cheap liveness path distinct from statement readiness. Valid execution failures, unsupported kinds/Python/SQL/types, and output limits return HTTP 200 with `status:error`, `ename`, and bounded `evalue`. JSON/schema errors use 400; unsupported request content type/encoding 415; body overflow 413; admission/session-capacity rejection 429; unavailable/draining service 503. Give all these failures a bounded named error body. A response deadline uses `DeadlineExceeded` in the execution envelope while separately tracking any still-running native operation.

Body-read timeouts use HTTP 408 with a bounded transport error; send timeouts drop the response and release its retained envelope. Error names distinguish invalid input, unsupported behavior, query failure, result limit, deadline, cancellation, and session closing. Freeze their exact names/HTTP mapping with adapter fixtures in 416-01. Never emit a successful status before the complete bounded HTTP result has been validated and serialized.

## 416-07: launcher and control protocol

Use a versioned, bounded JSON-lines control mode over pipes owned by the launcher: stdin carries graceful shutdown and stdout carries readiness/control records; ordinary diagnostics go to stderr. Framing/records are capped at 16 KiB. A readiness record includes the per-launch identity, binary version, control/agent versions, actual HTTP address, and enabled capabilities. Only emit it after listener/native readiness; verify it belongs to this launch rather than probing an arbitrary port. Control mode must not mix SQL output with readiness frames.

`SparklessServer` is an owned-child lifecycle object: start/stop are idempotent, restart creates a fresh identity, distinct instances can nest, and same-object nested entry is rejected. Endpoint access requires readiness and a live child. Validate supported binary package version, protocol version, and capabilities before returning from startup. During 4.16, `spark_connect_url` always raises a descriptive unsupported error while running.

Both sync and async implementations share ownership and compatibility rules. Async cancellation during spawn/readiness/shutdown must trigger bounded cleanup that cannot be abandoned by the same cancellation; reap before propagating it. Support Python 3.8 without assuming newer asyncio helpers are available. Document graceful pipe shutdown, forced termination, fork ownership checks, and POSIX/Windows differences.

The baseline binary wheel package is `sparkless-server-bin`, containing package resources and the precompiled executable, with no runtime download/build fallback. Its version matches `sparkless` exactly in the server extra. The release workflow now builds, installs, and starts wheels tagged `macosx_14_0_arm64` and `manylinux_2_28_x86_64` on matching native runners, and checks readiness, HTTP health, and graceful shutdown. These are the only candidate `sparkless[server]` targets; all other targets must fail clearly until a wheel and lifecycle gate are added. The manylinux job also runs `auditwheel show`. Do not publish a generic empty wheel that pretends to supply every platform.

The initial server target matrix is selected for 416-01, with advertisement still conditional on passing native lifecycle evidence. Existing Python engine wheel support remains intact; a platform lacking a server binary may still install ordinary `sparkless`. Unsupported `sparkless[server]` installations must fail clearly rather than silently omit the extra. The native image is built from the executable without Python/JVM, runs an HTTP SQL plus graceful-stop smoke before publication, and uses the coordinated version tag.

## 416-08: metadata, candidate packages, and CI

The release checker now includes `sparkless-server` in the Rust package/dependency/lock graph, requires `spark-sql-parser` version and lock alignment for any 4.16+ candidate branch even without a tag, and verifies the `sparkless-server-bin` version/extra coupling. Tests cover a coordinated 4.17 candidate branch without relying on a release tag. The release workflow packages every publishable Rust crate archive, builds and freshly installs the server wheels, installs the candidate `sparkless[server]` wheel pair offline, runs the pinned MiniLake SDK integration, and builds/smokes the native image before publication. Publication now waits for all checks, then releases Rust crates, server binary wheels/container, and the `sparkless` extension in sequence. Moving `spark-sql-parser` from 0.5.0 to 4.16.0 still requires updating every first-party parser dependency; changing only its manifest leaves an invalid published graph.

Maintain one explicit inventory of package names, manifests, dependency edges, artifact targets, and publication destinations. Check every version and first-party dependency requirement, Cargo.lock entry, Python runtime value, binary report, wheel metadata, container tag, and `v4.16.0` tag. `sparkless-native` ships inside `sparkless` wheels; it does not add a new crates.io publication requirement.

Build package archives and install them from an isolated consumer outside the checkout with the candidate dependency set. Root `[patch.crates-io]` overrides are not a downstream installation contract: verify that the published graph resolves and builds without inheriting this checkout's Git patches or local paths. If it does not, fix distributable dependencies or document the release blocker before publication. Validate the packaged binary-extra resources and release dependency order as well as source builds.

Run the following independent CI lanes on the integrated candidate:

1. Native crate/feature/dependency isolation and existing embedded-engine regression checks.
2. Direct HTTP, logical schema, admission/session/deadline/shutdown checks.
3. Experimental Connect feasibility with pinned unmodified client defaults; no experimental gRPC in normal artifacts.
4. Unmodified SDK through the corrected immutable emulator build, with actual value/schema assertions.
5. Sync/async launcher and package-resource installation on each advertised binary platform, plus existing Python wheel regression coverage.
6. Native image and fresh candidate-package consumer tests, complete metadata inventory, checksums, and platform wheel tags.

Keep exact client/emulator versions and D01–D09/D11 evidence in the decision/validation records. HTTP/binary/launcher failures cannot be masked by a passing embedded-engine suite. Third-party pins remain open until tested; all first-party candidate package versions are 4.16.0.

## Release readiness versus completion

**Ready to tag/publish:** 416-01 through 416-08 are complete; V01–V14 and V29 pass against one integrated candidate and the corrected emulator; all platforms/artifacts in the inventory are built and verified. The release workflow changes are implementation only until a non-publishing candidate run passes, including the selected wheel matrix and container smoke. No publication is needed to prove these pre-publication gates.

**Released:** publish in dependency order, verify package registries plus binaries/container, and pass V30 using fresh public installs. Only then mark 4.16.0 complete. A partial registry publication is an incomplete release. Resume only missing verified artifacts; if existing published contents require correction, issue a coordinated new patch version instead of overwriting artifacts.

- [ ] 416-01–416-03: contracts and feasibility complete (Phase 0A).
- [ ] 416-04–416-07: native HTTP, bounded runtime, schema, and owned launcher complete.
- [ ] 416-08: integrated SDK and all candidate distribution gates pass (Phase 0B ready).
- [ ] 416-09: complete 4.16.0 inventory published and verified.

This refinement creates implementation instructions only. Native changes, version bumps, external repository changes, and publication happen in the corresponding delivery slices, with evidence recorded before advancing their gates.
