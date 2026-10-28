# sparkless-server

Standalone native statement service for Robin Sparkless.

**4.17.0 security notice:** plain `cargo install` retains a temporary zstd
dependency exception. Prebuilt server distributions use the fixed implementation;
Rust users can apply the [pinned Cargo patch and security guidance](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/security/zstd-4.17-exception.md).

Run from the workspace with `cargo run -p sparkless-server -- serve`. It listens on
`127.0.0.1:8099` by default; use `--host`/`--port` or `SPARKLESS_SERVER_HOST` /
`SPARKLESS_SERVER_PORT` to configure the listener. Ordinary sessions expire after
15 minutes of inactivity by default; set `SPARKLESS_SESSION_IDLE_TTL_MS` to a positive
millisecond value to change it. The reserved `sql-warehouse` session does not expire.
The HTTP routes are `/health`, `/capabilities`, `/statements`, `/close`, and
`/operations/status`.

Owned launchers use `sparkless-server control --host 127.0.0.1 --port 0`. In this
mode stdout contains only bounded JSON-lines records: a `ready` record after a
native `SELECT 1` witness and a `stopped` record after shutdown. Send
`{"control_protocol_version":1,"command":"shutdown"}` on stdin for graceful
shutdown; diagnostics remain on stderr. Do not use this pipe protocol as an SQL
transport.

The default `http` feature serves the existing bounded SQL allowlist, in-memory service
catalog, positional JSON results, and same-instance mutation identity records. The optional
`connect` feature generates the Apache Spark 4.0.5 public gRPC service and provides a bounded
adapter on the same `Server` instance (`connect_service` / `serve_connect_until`). It currently
establishes protobuf, session-identity, and shared admission boundaries; plan translation,
analysis, configuration, result streaming, and operation replay remain in later 4.18 slices.
Connect methods outside that implemented boundary return explicit gRPC errors. The Connect
listener is not advertised by the HTTP launcher yet.

Build the features independently:

```sh
cargo check -p sparkless-server --no-default-features --features http
cargo check -p sparkless-server --no-default-features --features connect
```

The Connect proto sources are vendored under `proto/spark/connect` from Apache Spark tag
`v4.0.5` and generated during a Connect-feature build; see `proto/README.md` and the
[4.18 protocol research](../../docs/plans/sparkless-server/CONNECT_PROTOCOL_RESEARCH.md).
Connect user IDs partition session identity and are not authentication credentials. The
HTTP reserves session IDs beginning with `connect:` and operation IDs beginning with
`connect-operation:` to keep its registry namespace separate from Connect. The in-memory catalog
and operation records are lost on restart. Native CPU and intermediate-memory limits remain
cooperative rather than hard per-query isolation.

Build the JVM-free native image from the workspace root with:

```bash
docker build -f crates/sparkless-server/Dockerfile -t sparkless-server:local .
```

The final image contains only the Rust executable and its native runtime libraries.
