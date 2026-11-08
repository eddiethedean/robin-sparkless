# Run and restart a container

This tutorial uses Docker with Compose on a host that can run Linux x86_64
images. The release containers are separate base and DuckDB products. Both
default to Polars; DuckDB execution must be selected at startup. The service
targets trusted local workloads; see the [deployment boundary](PRODUCTION.md#deployment-boundary).

## Start the base service

Download [compose.yaml](https://github.com/eddiethedean/robin-sparkless/blob/main/examples/deployment/compose.yaml)
into an empty directory. Run from that directory:

```bash
docker compose config
docker compose up -d base
docker compose logs base
```

The example publishes ports on host loopback only, limits memory to 512 MiB and
CPU to two cores, and allows 20 seconds for graceful stop. Inside the container
the image binds both listeners to all container interfaces. It has no shell or
curl-based internal healthcheck. Probe readiness externally:

```python
import json
from urllib.request import urlopen

with urlopen("http://127.0.0.1:8099/health", timeout=5) as response:
    assert json.load(response)["status"] == "ready"
with urlopen("http://127.0.0.1:8099/capabilities", timeout=5) as response:
    capabilities = json.load(response)
    print(capabilities)
```

Run the [HTTP mutation verifier](https://github.com/eddiethedean/robin-sparkless/blob/main/scripts/verify_server_http.py)
against `http://127.0.0.1:8099`, then `docker compose restart base` and repeat
readiness. The instance changes and managed tables/operation history are lost.
Do not replay an old-instance HTTP mutation as though it were a fresh write.

## Persist DuckDB execution data

Create a fresh writable bind directory. These Bash commands run the container
under your non-root host UID/GID so its process can write the fixture directory:

```bash
mkdir data
export SPARKLESS_DOC_UID="$(id -u)"
export SPARKLESS_DOC_GID="$(id -g)"
docker compose --profile duckdb up -d duckdb
docker compose logs duckdb
```

Probe `http://127.0.0.1:18099/health` and capabilities before using Connect.
HTTP SQL is disabled in this mode. In a Python 3.11+ client environment, install
`sparkless-connect[client]==4.20.0`. Run this complete client once against the
fresh database:

The DuckDB process also needs its ICU timezone extension. This example gives
`/.duckdb` a writable, executable tmpfs cache owned by the configured UID/GID.
The first startup needs outbound access to DuckDB's extension repository; the
cache is temporary and can be downloaded again after a container restart.
For offline operation, supply a pre-populated cache containing the matching
Linux x86_64 extension version on a writable filesystem that permits loading
shared libraries. The read-only root filesystem alone is insufficient.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://127.0.0.1:25002").create()
try:
    spark.sql("CREATE TABLE events (id BIGINT)").collect()
    spark.sql("INSERT INTO events VALUES (42)").collect()
    assert spark.table("events").collect()[0].id == 42
finally:
    spark.stop()
```

Run `docker compose restart duckdb`, wait for readiness, and create a **new**
Connect session. Verify the stored value without rerunning CREATE or INSERT:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://127.0.0.1:25002").create()
try:
    assert [row.id for row in spark.table("events").collect()] == [42]
finally:
    spark.stop()
```

Tables and durable receipts survive in `./data/events.duckdb`; sessions and
retained results do not. This read-back demonstrates storage persistence, not
operation replay. For lost-response writes, follow [recovery](WRITE_RECOVERY.md).
Use one writable server process for this file. Stop the service before opening
it with a separate writable DuckDB client. Use a shared Quack owner for the
separately documented concurrent-owner mode.

## Operate and stop

Record image digests with `docker image inspect`; the [release record](RELEASE_EVIDENCE.md)
lists reviewed digests. Use those digests in deployment locks rather than an
unbounded image tag. These sample resource limits are a starting point, not a
capacity promise. Measure your inputs, overload and cancellation behavior.

```bash
docker compose --profile duckdb down
```

The bind directory remains after `down`; retain it for another run or remove it
after explicitly deciding to discard the fixture. For remote callers, supply an
authenticated TLS boundary for both HTTP and gRPC and verify streaming through
your actual proxy. This loopback tutorial supplies no proxy/authentication
implementation. See [operations](PRODUCTION.md) for drain, diagnostics and sizing.
