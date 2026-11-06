# Native server deployment and operations

This runbook covers native Sparkless servers in a trusted single-node
environment. The project primarily targets local development and testing.
See the [support policy](SUPPORT.md) and [support matrix](SUPPORT_MATRIX.md)
for compatibility and deployment scope.

## Deployment boundary

Use the packaged native executable or repository-built image on a supported
platform. Choose the base server for standard use or the
[DuckDB server](DUCKDB_SOURCE.md) for configured DuckDB sources and its
optional execution backend.
Bind both listeners to loopback by default. For remote access, place
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
4. Run a `SELECT 1` witness and a supported Connect witness if that listener is used.
5. Admit clients only after readiness. A launcher `ready` record is emitted after
   listener binding and a native witness; process existence alone is insufficient.

Health is readiness, not proof that every query/connector works. Readiness can
return 503 while draining. Capture stderr for diagnostics and preserve structured
client records (instance/session/operation IDs, duration, result/error category).
Avoid logging credentials or complete sensitive SQL/input data. There is no
promised Prometheus endpoint or distributed tracing contract; measure process
RSS/CPU, health failures, latency, timeouts and queue saturation externally.

## Start an installed HTTP server

Use this loopback smoke on a supported server-wheel platform. In one terminal,
with the virtual environment active:

```bash
python -m pip install "sparkless-server==4.20.0"
server_bin="$(python -c 'from sparkless_server import executable_path; print(executable_path())')"
"$server_bin" --version
"$server_bin" serve --host 127.0.0.1 --port 8099 --disable-connect
```

In a second terminal, download
[verify_server_http.py](https://github.com/eddiethedean/robin-sparkless/blob/main/scripts/verify_server_http.py)
into the current directory and run:

```bash
python verify_server_http.py http://127.0.0.1:8099 --mutations
```

The verifier creates temporary test data in the running instance, checks the
mutation receipt, and verifies that replay preserves the original result.
Expect `server health and HTTP SQL smoke passed`. Stop the process with Ctrl+C,
then restart it and repeat the health and SQL smoke. The instance ID changes
after restart. Reload your service data before admitting clients.

Use the [release artifact list](RELEASE_EVIDENCE.md) to record versions and
checksums for a deployment. For applications that own the process in Python,
see the [launcher reference](SERVER_PYTHON_API.md).

## Capacity and overload

Default active/pending operations are 2/16; ordinary sessions 64. Output/request/
cell/replay budgets are bounded, but Polars intermediate memory and CPU are not
hard per-query isolated. Raising output budgets does not make a query safe.
Test realistic joins, aggregates, skew and cancellation under the same host
limits. Reject or queue excess work in your application; back off on admission
failures instead of creating retry storms. Use separate processes for stronger
workload isolation.

In DuckDB execution mode, one serialized connection owns writes and receipts;
four read connections can run concurrently and up to 16 more reads queue.
Connections use one DuckDB thread and a 256 MiB memory limit. The accepted
load envelope is eight Connect clients plus one paced one-million-row reader
at an active-operation limit of 16 and a five-second queue timeout. Under that
workload the release gate requires at least five correct operations/second,
caps server RSS at 512 MiB, a shared Quack owner's RSS at 256 MiB, short-query
p95 at five seconds, and health p95 at one second.
These ceilings describe the tested workload, not arbitrary queries; size the
host for your own inputs and leave room for the operating system.

## Shutdown and restart

Stop new traffic, let callers inspect mutation outcomes, then request graceful
shutdown. Owned Python launchers send control protocol shutdown, wait, and
escalate/reap after configured timeouts. With the CLI/container, send its normal
termination signal and allow the configured shutdown grace period before force
termination. Cooperative work can outlive a response timeout; keep admission
accounted until native execution returns.

In the base server and in DuckDB server Polars mode, managed service tables,
session views, identities and retained operation records are **in memory and
lost on restart**. Local library warehouse persistence is a separate feature.
In DuckDB execution mode, database tables and transactionally written operation
receipts persist in the configured owner; Connect sessions, temporary state,
and retained query results do not. Reusing the same user/session/operation
identity and unchanged request can replay a durable receipt after restart, but
does not restore the prior session. A local database permits one writable
Sparkless process; use one shared Quack owner when independent clients need
concurrent access. After restart, rerun readiness and representative execution
witnesses before admitting clients. Never blindly retry a timed-out mutation
under a new ID; follow [Connect replay guidance](DUCKDB_EXECUTION.md#persistence-retries-and-shutdown).

## Incident checklist

- Is the process ready, draining, or restarting? Compare current instance identity.
- Is a caller exceeding effective request/result/session/admission limits?
- Is process RSS/CPU near its OS limit? Reduce concurrency/input first.
- Is the failure a supported SQL/Connect operation and logical type?
- For ambiguous writes, retain the original request/identity and query status.
- Preserve version, commit, limits and bounded diagnostics before restarting.
- Escalate reproducible bugs through [support](SUPPORT.md); follow [security](SECURITY.md)
  for sensitive issues. There is no guaranteed response/on-call service.
