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
`{"control_protocol_version":2,"command":"shutdown"}` on stdin for graceful
shutdown; diagnostics remain on stderr. Do not use this pipe protocol as an SQL
transport.

The default `http` feature serves the bounded SQL allowlist, in-memory service catalog, positional
JSON results, and same-instance mutation identity records. The optional `connect` feature compiles
the Apache Spark 4.0.5 public gRPC service and serves the Sparkless range/filter/project/limit/count
subset through the same native `Server` instance. Supported Connect plans include bounded scalar
and schema-bearing local relations, checked analysis/configuration calls, and independently
decodable bounded Arrow result batches. Operation reattachment, release, interruption, stale
identity rejection, and bounded result retention follow the documented lifecycle limits.

The 4.18 packaged server binary enables both `http` and `connect`. The `serve` and `control`
commands bind the listeners independently; Connect defaults to `127.0.0.1:15002`, while port `0`
selects an ephemeral port. `control` readiness reports the actual endpoints, server identity,
control protocol version 2, and capabilities protocol version 1. `--disable-connect` runs the same
binary in HTTP-only mode; `--disable-http` selects Connect-only mode. Readiness is published only
after each requested socket binds.

Spark Connect SQL commands, writes, artifacts, UDFs, Structured Streaming, unlisted plan types,
and types outside the published support matrix remain unsupported and return errors. The 4.18
server does not advertise or certify the optional MiniLake proxy route. Connect user IDs identify
sessions; they are not authentication credentials. Bind remote listeners only behind an explicit
deployment trust boundary. Saved catalog objects and operation records are in memory and are lost on
restart. CPU and intermediate memory limits remain cooperative rather than hard per-query
isolation. See the full [Spark Connect support matrix](../../docs/SPARK_CONNECT.md).

Build the features independently:

```sh
cargo check -p sparkless-server --no-default-features --features http
cargo check -p sparkless-server --no-default-features --features connect
cargo check -p sparkless-server --no-default-features --features http,connect
```

Build the release profile with both listeners using
`cargo run -p sparkless-server --no-default-features --features http,connect -- serve`.
The Connect-only feature build starts only gRPC:
`cargo run -p sparkless-server --no-default-features --features connect -- serve`.

The Connect proto sources are vendored under `proto/spark/connect` from Apache Spark tag
`v4.0.5` and generated during a Connect-feature build; see `proto/README.md` and the
[4.18 protocol research](../../docs/plans/sparkless-server/CONNECT_PROTOCOL_RESEARCH.md). The HTTP
service reserves session IDs beginning with `connect:` and operation IDs beginning with
`connect-operation:` to keep its registry namespace separate from Connect.

Build the JVM-free native image from the workspace root with:

```bash
docker build -f crates/sparkless-server/Dockerfile -t sparkless-server:local .
```

The final image contains only the Rust executable and its native runtime libraries.
