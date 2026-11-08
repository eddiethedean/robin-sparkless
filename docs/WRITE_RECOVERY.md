# Recover a write after a timeout or disconnect

A timeout means the client stopped observing, not that the engine rolled back.
Persist the request identity **before** submitting work. Do not create a new
operation ID until the previous outcome is established. See [error codes](ERRORS.md).

| Backend / transport | Retained information | Recovery boundary |
| --- | --- | --- |
| HTTP Polars | Bounded mutation records on one live server instance | Status/replay requires original instance and operation IDs; restart loses records and managed tables |
| Connect Polars | Bounded operation/replay history | Reattach only within session/history/output limits; no durable writes |
| Connect DuckDB execution | Transactional write receipts in the configured owner | Exact user/session/operation identity and unchanged request can replay after restart; sessions/results are not restored |
| Local external provider | Provider-owned storage and receipt protocol | Provider must reconcile its operation ID; Sparkless does not automatically retry callbacks |

## HTTP: save, inspect, replay

Install `sparkless-server==4.20.0` on a supported platform. Download
[http_recovery.py](https://github.com/eddiethedean/robin-sparkless/blob/main/examples/python/http_recovery.py)
and run `python http_recovery.py`. For a development executable, add
`--server-bin /absolute/path/to/sparkless-server`.

The status route returns the operation record directly; statement responses
carry that record under `operation`. The complete example saves its INSERT request before submission, deliberately
discards the successful response, fetches `/operations/status`, replays the
identical request, and verifies that exactly one row exists. It then restarts
the base server and verifies rejection of the old instance identity. It uses
only public HTTP routes and the public process launcher. Expect
`HTTP same-instance replay and restart boundary passed`.

In an application, persist the request in your durable journal, not this example's
temporary file. Inspect both HTTP status and JSON `status`. If status is still
running, poll the **same** IDs with bounded backoff. On committed outcome, consume
the receipt; on a known failed/no-effect outcome, correct the cause before a new
submission. If the record expired, the instance changed, or the outcome remains
unknown, reconcile through application state. An unknown record is not rollback
evidence. Stop retrying when your observation deadline expires, retaining the
journal for later reconciliation.

## Connect: public SQL versus protocol replay

Calling `spark.sql("INSERT ...")` again is a new submission. The public 4.0.5
SQL interface does not expose a documented way to choose and durably reuse the
original operation ID. Do not describe ordinary SQL resubmission as exactly-once
recovery. Automatic live client reattachment also does not promise cross-restart
session restoration.

For applications requiring durable replay, use an explicitly version-pinned
protocol integration that retains the original serialized ExecutePlan request,
user ID, UUID session ID and UUID operation ID. This is **lower-level gRPC
tooling**, not the high-level public SQL helper. The checked fault exercise is
[smoke_phase420_duckdb_write_reattach.py](https://github.com/eddiethedean/robin-sparkless/blob/main/tests/connect/smoke_phase420_duckdb_write_reattach.py).
From a checkout, with the [Connect dependencies](https://github.com/eddiethedean/robin-sparkless/blob/main/tests/connect/README.md)
installed:

```bash
cargo build --locked --release -p sparkless-server-duckdb --features connect
python tests/connect/smoke_phase420_duckdb_write_reattach.py \
  --server-bin target/release/sparkless-server-duckdb \
  --report /tmp/sparkless-write-recovery.json
```

This exercise injects a real UNAVAILABLE after a response, reattaches to the live
operation, then commits another write, forcibly stops the server, checks the
stored receipt through an independent DuckDB client, and replays the unchanged
request after restart. It asserts that each INSERT takes effect once. It uses
internal receipt-table inspection and a private force-stop helper **for fault
testing only**; neither is a stable application API.

An application-level protocol adapter must save serialized bytes before first
send, preserve content when reconnecting, check terminal completion, and reject
changed-content reuse. Never swap the owner database or clear receipts while
replaying. Keep local-owner clients sequential; a separate writable DuckDB
process cannot inspect the file while Sparkless owns it. See
[DuckDB ownership](DUCKDB_EXECUTION.md#query-execution-connections).

## Failure decisions

| Event | Required response |
| --- | --- |
| Request times out before any response | Retain identity; inspect status or replay unchanged through the appropriate protocol |
| Stream disconnects after some responses | Reattach within retention limits; keep acknowledged response position |
| DuckDB write response is lost, server restarts | Reuse saved protocol request against the same owner; do not invent a new session/operation identity |
| Same ID with changed content | Treat conflict as a caller/journal error; retrieve the original request |
| Owner unavailable or commit outcome unknown | Restore observation and reconcile; no fresh-ID blind retry |
| Receipt capacity exhausted | Stop new writes and investigate owner lifecycle; do not delete receipts as a generic retry fix |

The release fault exercise is narrower than a complete application recovery
system. Test your actual proxy, owner failure, caller journal durability and
shutdown path before relying on it. See [operations](PRODUCTION.md).
