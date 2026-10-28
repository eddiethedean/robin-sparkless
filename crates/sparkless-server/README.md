# sparkless-server

Standalone native HTTP statement service for Robin Sparkless.

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

The server executes through the Rust engine directly. It exposes a bounded SQL allowlist,
an in-memory service catalog, positional JSON results, and same-instance mutation identity
records. It does not expose Spark Connect, file/network readers, arbitrary functions, Python,
or nested and binary HTTP results. The in-memory catalog and operation records are lost on
restart. Native CPU and intermediate-memory limits remain cooperative rather than hard
per-query isolation.

Build the JVM-free native image from the workspace root with:

```bash
docker build -f crates/sparkless-server/Dockerfile -t sparkless-server:local .
```

The final image contains only the Rust executable and its native runtime libraries.
