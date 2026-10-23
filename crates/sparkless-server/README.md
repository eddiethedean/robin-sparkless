# sparkless-server

`sparkless-server` is the native, read-only HTTP SQL service for Robin Sparkless
4.16. It executes the accepted SQL directly through `robin-sparkless` and needs
neither Python nor a JVM. Spark Connect, user-defined functions, named tables,
file/network readers, DDL, and DML are not available in this release.

## Run

```sh
cargo run --locked -p sparkless-server -- serve
```

The service binds to `127.0.0.1:8099`. Set `--bind 0.0.0.0` only in a trusted
network deployment. `--port 0` asks the OS for an available port. Bind, port,
and resource-limit options have corresponding `SPARKLESS_SERVER_*` environment
variables; explicit CLI values override environment values. The launcher-only
`--control` and `--launch-id` flags are command-line options. Zero, contradictory,
and above-ceiling resource limits fail startup.

When launched by Python, use `--control --launch-id <id>`. The child writes a
bounded JSON-lines readiness record to stdout after binding and executing a
native `SELECT 1` witness. Shutdown is requested through the matching launch ID
on stdin. Diagnostics go to stderr.

## Container image

Build the JVM-free Linux image from the repository root:

```sh
docker build -f crates/sparkless-server/Dockerfile -t sparkless-server:4.16.0 .
docker run --rm -p 127.0.0.1:8099:8099 sparkless-server:4.16.0
```

The image runs as an unprivileged user and listens on `0.0.0.0:8099` inside the
container. It contains the server executable and its native runtime libraries;
it does not contain Python or a JVM. The release workflow publishes the Linux
x86_64 image as `ghcr.io/eddiethedean/sparkless-server:4.16.0` and updates the
`latest` tag after the release checks pass.

## HTTP contract

- `GET /live` reports process liveness.
- `GET /health` reports ready only after listener binding and native startup.
- `GET /capabilities` reports the HTTP SQL subset and active limits.
- `POST /statements` accepts `{"session":"...","kind":"sql","code":"SELECT ..."}`.
- `POST /close` accepts `{"session":"..."}` and cancels that session's work.

Successful statements return ordered schema fields and positional scalar rows.
Errors from SQL execution use the statement-agent error envelope; malformed
transport requests use HTTP errors. Results are fully bounded before success
headers are sent.

Sessions are isolated, process-local, and read-only. Their configuration is
fixed to Spark 4.0 compatibility, ANSI mode, case-insensitive resolution, and
UTC. The reserved `sql-warehouse` session does not expire while the service is
running. Closing a session cancels cooperatively and does not forcibly stop
native code.

The accepted SQL forms are SELECTs over literals and constant derived queries,
bounded `UNION ALL`, basic scalar arithmetic/comparison/boolean expressions,
`CAST`, `IS NULL`, `WHERE`, output-column `ORDER BY`, and literal `LIMIT`.
Supported results are BOOLEAN, SMALLINT, INT, BIGINT, finite DOUBLE, STRING,
DATE, microsecond UTC TIMESTAMP, and DECIMAL(1..38, scale 0..precision), including
typed nulls. Capabilities and actual configured limits are available from the
server itself. These capabilities are intentionally narrower than the embedded
Sparkless API.

See [the 4.16 release plan](../../docs/plans/sparkless-server/RELEASE_4_16.md)
for native release gates and optional SDK/emulator certification status.
