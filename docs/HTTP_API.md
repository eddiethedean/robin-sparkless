# HTTP statement API — 4.18.0

This is a bounded native SQL service, separate from the narrower Spark Connect
plan API. Obtain the matching [server distribution](INSTALLATION.md) first.
The service has no built-in authentication or TLS: use loopback or an explicitly
authenticated TLS boundary. Session names and instance IDs are identities, not secrets.

## Start and query

```bash
sparkless-server serve --host 127.0.0.1 --port 8099 --disable-connect
curl --fail http://127.0.0.1:8099/health
curl --fail http://127.0.0.1:8099/capabilities
curl --fail http://127.0.0.1:8099/statements   -H 'Content-Type: application/json'   -d '{"session":"tutorial","kind":"sql","code":"SELECT 1 AS value"}'
```

The owned [Python launcher](SPARK_CONNECT.md) selects ephemeral ports and stops/
reaps the process through a context manager. Do not use its stdin control pipe
as a statement transport.

## Routes

| Method/path | Request | Response |
| --- | --- | --- |
| GET `/health` | None | 200 `{"status":"ready"}`; 503 unavailable/draining |
| GET `/capabilities` | None | Effective transports, instance ID, protocol versions, SQL allowlist, limits |
| POST `/statements` | JSON fields below | Result envelope or error; inspect body status |
| POST `/close` | `{"session":"tutorial"}` | `{"status":"ok"}`; closes session and its local views |
| POST `/operations/status` | Instance and operation IDs | Current retained mutation record; 404 unknown, 409 wrong instance |

All POSTs require JSON Content-Type and valid bounded bodies. Identifiers must
be 1–128 safe ASCII bytes. HTTP session IDs cannot start `connect:`; operation
IDs cannot use the reserved Connect prefix. Do not use shared session names for
unrelated tenants. Ordinary sessions expire after 15 minutes idle; the reserved
`sql-warehouse` session is exempt. [Configuration](CONFIGURATION.md) lists defaults.

Statement fields:

| Field | Required | Meaning |
| --- | --- | --- |
| `session` | Yes | Session name; current database and temp views are session scoped |
| `kind` | Yes | Exactly `sql` |
| `code` | Yes | SQL string in the advertised allowlist |
| `server_instance_id` | Mutations | Exact instance from capabilities |
| `operation_id` | Mutations | Unique caller-chosen identity, retained for same-instance retry/status |

Reads support bounded SELECT/COUNT over service managed tables. Mutations cover
CREATE DATABASE/TABLE, INSERT INTO, DROP TABLE/DATABASE, CREATE TEMP VIEW,
DROP VIEW, USE. This is not general Spark SQL, arbitrary file access, JDBC,
Python execution, or durable storage. Unsupported statements fail explicitly.

## Results and errors

Successful query envelope (field types and order come from schema):

```json
{"status":"ok","data":{"application/json":{"schema":{"type":"struct","fields":[{"name":"value","type":"integer","nullable":false}]},"data":[[1]]}}}
```

The example schema illustrates the shape; numeric inference/nullability can vary
by expression. Rows are positional arrays, preserving duplicate field names.
Supported HTTP scalar types include integer widths, floating point, decimal,
boolean, string, date, timestamp and typed nulls. Nested/binary/unrepresented
types fail with `UnsupportedResultType`. Preserve decimal and large integer
precision in your client rather than converting all numbers to JavaScript Number.

Errors use `{"status":"error","ename":"...","evalue":"..."}`. **SQL/execution
errors can return HTTP200**. Check `status == "ok"` as well as HTTP status.
Transport/protocol errors use 400/413/415; wrong instance uses409, unknown route/
operation404; draining/admission exhaustion can use503. Result limits fail rather
than silently truncate. Use `ename` for classification; messages are diagnostic.

## Mutation retry and recovery

Fetch `server_instance_id` from capabilities, persist it with your new operation
ID and exact request, then submit. Inspect the response's `operation` record:
`execution_state`, `effect_outcome`, `receipt`, `error`, and both IDs. A completed
success includes a committed receipt. Reusing the same identity/request on the
same instance returns the retained outcome; changing SQL/session produces an
identity conflict. A running duplicate is not permission to submit a new ID.

After a timeout or disconnect, POST `/operations/status` with the saved IDs:

```json
{"server_instance_id":"INSTANCE_FROM_CAPABILITIES","operation_id":"my-insert-001"}
```

Wait for terminal state and establish the effect before retrying. A response
deadline requests cooperative cancellation; native work can still hold admission.
A server restart loses catalog and operation records. An unknown operation or
new instance is **not proof of rollback** and cannot provide durable exactly-once
execution. Reconcile through your application before creating a fresh operation.

Run the existing [HTTP contract verifier](https://github.com/eddiethedean/robin-sparkless/blob/main/scripts/verify_server_http.py)
for health, typed results, and mutation replay. [Operations](PRODUCTION.md)
explains shutdown, recovery, resource limits, and the trusted deployment scope.
