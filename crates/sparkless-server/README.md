# sparkless-server

Standalone native HTTP statement service for Robin Sparkless.

Run from the workspace with `cargo run -p sparkless-server -- serve`. It listens on
`127.0.0.1:8099` by default; use `--host`/`--port` or `SPARKLESS_SERVER_HOST` /
`SPARKLESS_SERVER_PORT` to configure the listener. The HTTP routes are `/health`,
`/capabilities`, `/statements`, `/close`, and `/operations/status`.

The server executes through the Rust engine directly. It exposes a bounded SQL allowlist,
an in-memory service catalog, positional JSON results, and same-instance mutation identity
records. It does not expose Spark Connect, file/network readers, arbitrary functions, Python,
or nested and binary HTTP results. The in-memory catalog and operation records are lost on
restart. Native CPU and intermediate-memory limits remain cooperative rather than hard
per-query isolation.
