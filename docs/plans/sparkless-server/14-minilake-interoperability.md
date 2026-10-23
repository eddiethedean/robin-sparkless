# SS-14: MiniLake interoperability without Java or a JVM

> **Status: planned; no integration evidence recorded.** Phase: 2 for Connect Jobs; Phase 4 for optional Delta/catalog interoperability. Contract preparation may start earlier; neither adds a 4.16 release gate.
> [Index](README.md) · [Epic integration contract](EPIC.md#minilake-integration-contract) · [Decisions](DECISIONS.md) · [Validation](VALIDATION.md)

## Outcome and ownership

Support `dmux/minilake` with as few upstream changes as possible. Sparkless owns the native server, a Python-only Connect task-runner image, the integration recipe/tests, and a Rust MiniLake catalog adapter. MiniLake retains workspace storage, Jobs/DAG orchestration, catalog registrations, and its DuckDB SQL warehouse. No Java/JRE/JVM, Apache Spark runtime, Py4J execution, or classic-Spark fallback is permitted anywhere in this integration deployment or its image-build recipe. Independent reference Spark tests remain outside that deployment.

The Python client executes script control flow and sends supported plans to the native Rust service. The service does not execute arbitrary Python. MiniLake-hosted Python runners are not the optional Sparkless-hosted workers in SS-12 and do not depend on Phase 3.

## Separate integration routes

| Route | Execution and transport | Scope |
| --- | --- | --- |
| Existing Databricks emulator HTTP agent | SDK → `calvinchengx/databricks-emulator` → native HTTP agent | Optional typed-result integration certification; independent of native Phase 0B publication |
| Existing emulator Connect proxy | Client → emulator h2c proxy → native Connect endpoint | Existing V25; standalone Connect conformance precedes proxy certification |
| MiniLake Connect Jobs | SDK/CLI → MiniLake Jobs → Python-only runner → `sc://sparkless-server:15002` | Separate V31–V33; MiniLake is an orchestrator, not a Connect proxy |
| MiniLake shared Delta/catalog | Native Sparkless ↔ shared local Delta files; Sparkless → MiniLake UC HTTP API; MiniLake SQL → DuckDB `delta_scan()` | Optional V34–V35 after native storage capability gates |
| MiniLake SQL external-Delta DML | Future structured write delegation → native commit service | V36 only if a separately agreed upstream hook and native mutation contract exist |

`DATABRICKS_SPARK_CONNECT_URL` and `DATABRICKS_SPARK_CONNECT_GRPC_URL` belong to the existing emulator, not MiniLake. No MiniLake HTTP-agent forwarding, warehouse replacement, or gRPC proxy is assumed. Its ordinary statements and `sql_task` execution continue using DuckDB. A successful MiniLake SQL query alone does not prove Sparkless executed anything and cannot replace V02/V05 or V25.

## Dependencies and delivery slices

- Contract/source inventory: SS-01 and SS-11; no server feature prerequisite.
- Phase 2 Jobs: pinned client/protos, SS-04 session/operation ownership, and standalone SS-09/10 conformance. Prototype witnesses may precede release but remain experimental.
- Phase 4 shared storage/catalog: SS-07/08/13 and explicitly enabled native file reads/writes. Existing embedded Delta/provider APIs do not establish remote server support.
- Optional MiniLake SQL DML: native mutation/commit gates plus an explicit integration contract; neither raw Python compatibility nor the standard Connect endpoint implies Delta Connect extension support.

MiniLake route certification is separate from standalone Connect and the existing emulator proxy. Require its gates before advertising it; do not turn optional MiniLake work into a prerequisite for 4.16 or an unadvertised route into a hidden core-release dependency.

## JVM-free deployment and runner

1. Build the MiniLake service image from Python and a pinned MiniLake wheel, with any selected extras. Do not use its upstream Spark/JAR-warming Docker build stages or the optional classic PySpark notebook image. Verify the installed wheel's UI if the recipe advertises it.
2. Build a separate Python-only task image with the exact selected `pyspark-client` and locked Python/gRPC/protobuf/PyArrow dependencies. Keep the Rust service image Python/JVM-free. If introducing a first-party runner artifact, include it in D05's coordinated release inventory and version policy.
3. Use existing `MINILAKE_SPARK_IMAGE` to select that task image. Supply an executable at the currently fixed `/opt/spark/bin/spark-submit` path as a documented argument adapter; this image contains no Spark distribution. Preserve script path/argv/stdout/stderr/exit code, validate submission options, and reject Maven packages and unsupported execution modes explicitly. The native server must reject unsupported JVM/static-extension configuration supplied by scripts. Never silently discard `--packages` or fall back to classic Spark.
4. Set `SPARK_REMOTE` through the image's deployment default or each task's existing `new_cluster.spark_env_vars`. MiniLake forwards those task variables, including its secret-resolution behavior. Docker tasks do not inherit arbitrary environment variables from the MiniLake service container. The runner must require a valid remote endpoint and fail if it is unavailable; it must never start local compute.
5. Use MiniLake's existing shared-volume/network controls. Join MiniLake, the runner, and the Rust service to the same explicitly configured Docker network; mount the same named volume at `/data` for the initial recipe. Disable image prewarming with `MINILAKE_SPARK_PREWARM=0` and prebuild/load the pinned custom image. Verify no default Apache Spark image is pulled or used.
6. Treat the runner's lifecycle separately from Connect operation lifecycle. Create a fresh session per task, close/release it on normal exits, and implement bounded interruption/cleanup on runner shutdown. Forced runner death can detach remote work; test server retention/expiry and eventual resource reclamation without claiming that process exit proves cancellation.

Existing configuration example (the runner image is a proposed artifact, not a published image):

```yaml
MINILAKE_SPARK_IMAGE: our-built-connect-runner
MINILAKE_DOCKER_NETWORK: sparkless-interop
MINILAKE_DOCKER_VOLUME: minilake_data
MINILAKE_SPARK_PREWARM: "0"
```

MiniLake task shape using the current environment-forwarding seam:

```json
{
  "task_key": "main",
  "spark_python_task": {"python_file": "/Shared/example.py"},
  "new_cluster": {
    "spark_env_vars": {"SPARK_REMOTE": "sc://sparkless-server:15002"}
  }
}
```

The first script uses an unmodified client and `SparkSession.builder.getOrCreate()` with `SPARK_REMOTE`, asserts `spark.range(10).filter("id > 5").count() == 4`, prints the result, and stops its session in `finally`. Validate ordinary SDK/CLI workspace import and Jobs submission/poll/output paths, rather than only invoking the runner by hand. Source notebooks are Python files in this path, not full Databricks notebook/dbutils emulation.

A development-only alternative installs the same client into MiniLake's `python3` environment and uses existing `MINILAKE_JOB_EXECUTOR=subprocess`. Its REST Jobs path can provide a proof without a launcher shim, but it has a different process-isolation contract and the MCP `run_python_script` helper rejects subprocess mode. Do not claim that helper works without an independently verified small upstream change. In Docker mode, test MCP with `delta=False` and no packages before advertising it; Delta/Maven helpers and shipped classic-Spark notebooks are outside the initial recipe.

## Catalog and storage contract

MiniLake owns registrations; the Delta transaction log owns committed table data. Start on one Docker host with a shared local volume and identical absolute paths. A different machine needs an explicitly shared filesystem with equivalent semantics. Neither a common path string nor existing cloud SDKs prove shared access or S3/cloud-credential support.

Implement a Rust `MinilakeCatalogProvider` using the existing `ExternalCatalogProvider` contract. Install it explicitly on each enabled server session; do not assume engine `new_session()` inherits a provider. Keep HTTP lookup off async I/O threads and within bounded execution/deadline accounting because the current provider callback is synchronous.

For `spark.table("main.analytics.orders")`, translate the normal Connect named-table relation and query `GET /api/2.1/unity-catalog/tables/main.analytics.orders`. Accept only `EXTERNAL` + `DELTA` in the first adapter. Map `table_id`, canonical allowed `storage_location`, ordered `columns` and exact types into `ExternalTableDescriptor`. Use native Delta reads; do not embed the Java Unity Catalog connector or read MiniLake's DuckDB database files directly. Listing/configuration requires its own supported catalog API mappings; the table-resolution trait alone does not supply them.

Handle quoted identifiers, namespace defaults, case rules, decimals/time/nested types within the enabled type matrix, missing tables versus HTTP errors, schema mismatch, and table drop/recreate IDs explicitly. MiniLake does not supply a pinned Delta snapshot version in this API. Resolve and verify a consistent physical snapshot; do not combine indefinitely cached catalog metadata with an unrelated latest schema or claim cross-engine snapshot transactions. Unknown names in the configured MiniLake namespace must not silently create Sparkless-only tables. Reject managed DuckDB tables and unsupported formats clearly.

First shared-data workflow, after the applicable native capabilities pass:

1. Sparkless writes Delta by explicit allowed path and waits for the native commit.
2. The caller registers that location as an external Delta table through MiniLake's existing SDK Tables API.
3. MiniLake queries the committed rows through DuckDB `delta_scan()`.
4. Sparkless reads the same table by path, then through the catalog adapter.

Registration and the file commit are separate operations, not one atomic transaction. Define recovery for committed-but-unregistered files and registration failures; never blindly replay a possibly committed write. Pre-registration for named writes and their commit receipts require SS-13 validation. Existing provider hooks support some writes to resolved tables; they do not automatically implement remote `WriteOperation`, table creation/registration, `UPDATE`/`DELETE`, or mutation coordination.

## Existing MiniLake external-Delta DML boundary

MiniLake's `_execute_delta_write` generates classic Spark Python for SQL `INSERT`, `UPDATE`, and `DELETE`, requests Maven Delta libraries, and sets JVM catalog/extension configuration. Updates/deletes call `delta.tables.DeltaTable`. The pure Connect runner must reject this unsupported path before execution; no Java fallback is allowed. Generic Connect and native Delta file support do not imply compatibility with the separate Delta Connect extension protocol.

Initially support Sparkless-originated enabled native writes and MiniLake-originated reads. If MiniLake SQL Delta DML is required, propose a narrow structured delegation hook before Python generation: location plus prepared input for inserts, or parsed conditions/assignments for updates/deletes, with an operation identity, deadline, and truthful committed/failed/unknown outcome. Input files remain valid until native work actually releases them, including after an HTTP timeout. Reuse shared native mutation services, not an unbounded ad hoc HTTP execution path. Specify schema/row-count/commit receipts and retry behavior before agreeing the hook. No new MiniLake API, hook name, or server endpoint is assumed to exist.

## Minimal upstream requests

| Request | Requirement |
| --- | --- |
| Configurable task-runner executable/argv instead of the fixed Spark path | Optional convenience; retain the current default and allow our image to use a plainly named launcher |
| Structured external-Delta write delegation | Optional later feature; needed only to advertise MiniLake SQL Delta DML without implementing its classic script/Delta-extension surface |
| Active-task cancellation plumbing | Optional later feature if immediate MiniLake Jobs cancellation is promised; current cancellation checks between tasks |

The first Jobs and external-table read integrations require no MiniLake source changes. Keep runner, catalog adapter, deployment recipe, fixtures, and native capabilities in Sparkless. Prepare any upstream proposal separately; this plan does not authorize posting issues, PRs, or messages.

## Acceptance criteria and evidence

- [ ] V31–V33: real MiniLake Jobs compute native results with pinned unmodified clients, correct task behavior, and truthful lifecycle outcomes in an entirely JVM-free deployment.
- [ ] V34: native Delta commits are independently read through MiniLake/DuckDB on the shared volume, with schema/precision and failure/restart evidence.
- [ ] V35: named-table resolution uses the existing MiniLake API and our native provider; unsupported managed tables/formats, stale schemas, and catalog failures cannot fall back silently.
- [ ] V36 only if enabled: agreed structured DML delegation has truthful commit/retry/cleanup behavior and independently verified native writes.
- [ ] Record exact revisions, wheel/image digests, dependency locks, enabled capabilities, commands, and limits in the validation records. A Jobs `SUCCESS`, catalog registration, TCP connection, or DuckDB-only query is insufficient proof.

## Source baseline and implementation touchpoints

Reviewed MiniLake revision: [`b2736501ed13c6b069204a202a9e6142a5b7e54c`](https://github.com/dmux/minilake/tree/b2736501ed13c6b069204a202a9e6142a5b7e54c). This is a source-inspection baseline, not a tested compatibility pin. Select the actual wheel/version/image digest during implementation and retest changes.

- [MiniLake task executor](https://github.com/dmux/minilake/blob/b2736501ed13c6b069204a202a9e6142a5b7e54c/src/minilake/docker_executor.py), [Jobs](https://github.com/dmux/minilake/blob/b2736501ed13c6b069204a202a9e6142a5b7e54c/src/minilake/services/jobs.py), [MCP helper](https://github.com/dmux/minilake/blob/b2736501ed13c6b069204a202a9e6142a5b7e54c/src/minilake/mcp/tools/jobs.py).
- [MiniLake catalog API](https://github.com/dmux/minilake/blob/b2736501ed13c6b069204a202a9e6142a5b7e54c/src/minilake/services/unity_catalog.py), [SQL write path](https://github.com/dmux/minilake/blob/b2736501ed13c6b069204a202a9e6142a5b7e54c/src/minilake/services/sql_statements.py), [Docker build](https://github.com/dmux/minilake/blob/b2736501ed13c6b069204a202a9e6142a5b7e54c/Dockerfile).
- [External catalog contract](../../EXTERNAL_CATALOGS.md), [SS-09](09-connect-plans-and-analysis.md), [SS-10](10-arrow-replay-and-connect-conformance.md), [SS-13](13-optional-delta-and-unity-catalog.md).
- [Standalone Python Connect client](https://spark.apache.org/docs/4.0.0/api/python/getting_started/install.html#python-spark-connect-client), [Delta Connect extension](https://docs.delta.io/delta-spark-connect/). These references do not select an exact tested client release or establish supported native extensions.
