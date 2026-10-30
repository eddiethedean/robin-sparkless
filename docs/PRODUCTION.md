# Native server deployment and operations

Scope: **Sparkless 4.18.0 in a trusted single-node environment**. The project
primarily targets local development/testing. This runbook does not establish an
enterprise SLA, multi-tenant isolation, distributed recovery, or Spark cluster
replacement. See the [support policy](SUPPORT.md) and [surface contract](SUPPORT_MATRIX.md).

## Deployment boundary

Use the packaged native executable or repository-built image on a supported
platform. Bind both listeners to loopback by default. For remote access, place
an authenticated TLS proxy/network boundary in front of **both HTTP and gRPC**,
allow only intended callers, and test gRPC streaming/timeout behavior through
that exact proxy. Connect user/session IDs are not authentication credentials.
Do not expose either port directly to untrusted networks or execute untrusted
workloads in the same process as sensitive data.

Run as a non-root OS user with minimal filesystem/database permissions. Supply
secrets through your deployment's secret mechanism. Set hardening before startup:
`SPARKLESS_HARDENED=1`, `SPARKLESS_FILES_BASE` to an isolated data root, and
`SPARKLESS_JDBC_ALLOW_ARBITRARY_SQL=false` when database access is enabled locally.
These are defense-in-depth controls, not an OS sandbox.

## Start, admit traffic, observe

1. Pin package/binary/container identity and matching Python launcher version.
2. Allocate process/container memory and CPU limits; select admission and result
   budgets using [configuration](CONFIGURATION.md) and [sizing](PERFORMANCE.md).
3. Start with explicit bind addresses; query `/health` until ready and record
   `/capabilities`, instance ID, limits, executable version and deployment commit.
4. Run a SELECT1 witness and a supported Connect witness if that listener is used.
5. Admit clients only after readiness. A launcher `ready` record is emitted after
   listener binding and a native witness; process existence alone is insufficient.

Health is readiness, not proof that every query/connector works. Readiness can
return 503 while draining. Capture stderr for diagnostics and preserve structured
client records (instance/session/operation IDs, duration, result/error category).
Avoid logging credentials or complete sensitive SQL/input data. There is no
promised Prometheus endpoint or distributed tracing contract; measure process
RSS/CPU, health failures, latency, timeouts and queue saturation externally.

## Capacity and overload

Default active/pending operations are 2/16; ordinary sessions 64. Output/request/
cell/replay budgets are bounded, but Polars intermediate memory and CPU are not
hard per-query isolated. Raising output budgets does not make a query safe.
Test realistic joins, aggregates, skew and cancellation under the same host
limits. Reject or queue excess work in your application; back off on admission
failures instead of creating retry storms. Use separate processes for stronger
workload isolation.

## Shutdown and restart

Stop new traffic, let callers inspect mutation outcomes, then request graceful
shutdown. Owned Python launchers send control protocol shutdown, wait, and
escalate/reap after configured timeouts. With the CLI/container, send its normal
termination signal and allow the configured shutdown grace period before force
termination. Cooperative work can outlive a response timeout; keep admission
accounted until native execution returns.

Saved service tables, session views, identities and retained operation records
are **in memory and lost on restart**. Local library warehouse persistence is a
separate feature and does not turn the HTTP catalog into durable storage.
After restart, rehydrate required state and rerun readiness witnesses. Never
blindly retry a timed-out mutation with a new ID: follow [HTTP recovery](HTTP_API.md).
There is no durable cross-restart exactly-once guarantee or service backup/restore API.

## Incident checklist

- Is the process ready, draining, or restarting? Compare current instance identity.
- Is a caller exceeding effective request/result/session/admission limits?
- Is process RSS/CPU near its OS limit? Reduce concurrency/input first.
- Is the failure a supported SQL/Connect operation and logical type?
- For ambiguous writes, retain the original request/identity and query status.
- Preserve version, commit, limits and bounded diagnostics before restarting.
- Escalate reproducible bugs through [support](SUPPORT.md); follow [security](SECURITY.md)
  for sensitive issues. There is no guaranteed response/on-call service.
