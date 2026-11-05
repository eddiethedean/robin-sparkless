# Python server and Connect API

Use `sparkless_server` for the base product and `sparkless_server_duckdb` for
configured DuckDB sources. Both export the helpers below. The shared
`sparkless_connect` package exposes the same helpers and accepts an explicit
`product="base"` (default) or `product="duckdb"` on server construction.
Product-bound classes select that option for you. See [installation](INSTALLATION.md#choose-a-package).

## Own a native process

| Option on `SparklessServer` and `AsyncSparklessServer` | Effect |
| --- | --- |
| `host`, `connect_host` | HTTP and gRPC bind IPs. Both default to `127.0.0.1`. |
| `port`, `connect_port` | Default `0` selects an available port. Set `connect_port=None` for HTTP only. |
| `executable_path` | Path to an executable built for the selected product. Omit it to use the installed product binary. |
| `env` | Extra environment values for the child, merged with the caller's environment. |
| `startup_timeout`, `shutdown_timeout`, `kill_timeout` | Positive durations in seconds. Defaults are 30, 10, and 5. |

Use `with SparklessServer(...) as server` or `async with
AsyncSparklessServer(...) as server`. Explicit `start()` and `restart()`
return the launcher once ready, while `stop()` returns `None`. Await these
methods on the async variant. Each launcher belongs to the process that created it.

Read `http_url`, `spark_connect_url`, and `server_instance_id` after startup.
`is_running` reports child liveness and `diagnostics` exposes bounded child
stderr. A new process has a new instance ID and fresh in-memory state.

| Error | Common causes |
| --- | --- |
| `UnsupportedServerPlatformError` | Missing installed product or executable for the platform |
| `UnsupportedSparkConnectError` | Connect disabled, or the selected binary lacks Connect support |
| `SparklessServerError` | Invalid explicit binary path, incompatible readiness metadata, or lifecycle failure |
| `ValueError` | Invalid product, timeout, bind address, or port |

See the [HTTP example](HTTP_API.md#start-and-query) for an HTTP-only process.

## Own a process and client session

`SparklessConnect(*, server_options=None)` accepts a mapping of launcher options.
Entering the context returns a new PySpark Connect `SparkSession`. Exiting
stops the session before stopping the child. Use the product's `connect` extra
to install the client. A missing client raises `ImportError`; passing
`connect_port=None` raises `ValueError`. Entering an active helper again or
using one inherited from another process raises `SparklessServerError`.

For an existing endpoint, build a session with
`SparkSession.builder.remote(url).create()` and stop it yourself. See the
[Connect examples](SPARK_CONNECT.md) and [supported plans](SPARK_CONNECT.md#supported-request-surface).
