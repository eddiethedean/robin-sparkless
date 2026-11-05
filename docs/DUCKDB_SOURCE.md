# Read DuckDB tables from PySpark

The optional `sparkless-server-duckdb` package exposes configured DuckDB
tables over Spark Connect. Use it when your test data already lives in a
DuckDB file and your test uses the PySpark client.

!!! info "Read-only source access"

    Sparkless opens DuckDB sources read-only. Write support is planned for a
    future release. Polars executes the supported DataFrame operations.

## Query a local DuckDB file

Install the DuckDB server, its Connect client, and the DuckDB Python package
used to create the example database.

```bash
python -m pip install "sparkless-server-duckdb[connect]==4.19.0" "duckdb==1.5.1"
```

Save as `query_orders.py` and run `python query_orders.py`. The example creates
a temporary file and closes its writer before Sparkless opens the database.

```python
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb
from pyspark.sql import functions as F
from sparkless_server_duckdb import SparklessConnect

with TemporaryDirectory() as directory:
    database = Path(directory) / "orders.duckdb"
    with duckdb.connect(str(database)) as db:
        db.execute("""
            CREATE TABLE orders AS
            SELECT * FROM (VALUES (120, 'paid'), (80, 'pending'), (110, 'paid'))
            AS t(amount, status)
        """)

    sources = [{"alias": "warehouse", "database_path": str(database)}]
    options = {"env": {"SPARKLESS_DUCKDB_SOURCES": json.dumps(sources)}}

    with SparklessConnect(server_options=options) as spark:
        orders = spark.table("warehouse.main.orders")
        assert orders.filter("amount > 99").count() == 2
        grouped = orders.groupBy("status").agg(F.count(F.lit(1)).alias("n"))
        counts = {row.status: row.n for row in grouped.collect()}
        assert counts == {"paid": 2, "pending": 1}
        print(sorted(counts.items()))
```

Expect `[('paid', 2), ('pending', 1)]`. To use an existing fixture, remove the
creation step and point `database` at its absolute path.

The server maps the alias to the database file at startup. The PySpark client
uses a name like `warehouse.main.orders` or `warehouse.orders`. Paths and
credentials stay with server configuration.

## What queries can read

| Supported | Scope |
| --- | --- |
| Table reads | Named tables under a configured alias |
| DataFrame operations | Filter, select, limit, row and non-null-column counts, grouped count and sum |
| Joins | Supported inner joins between tables from the same configured owner |
| Types | BOOLEAN, SMALLINT, INTEGER, BIGINT, finite DOUBLE, VARCHAR, DATE, TIMESTAMPTZ, and DECIMAL with precision up to 38 |

Schema analysis resolves metadata without scanning rows. During execution,
the server scans the table in typed Arrow batches under a statement transaction,
then applies DataFrame operations in Polars. Source limits apply to the scan
even when the query returns only a few rows.

| Source budget | Limit |
| --- | ---: |
| Rows per statement | 1,000,000 |
| Arrow IPC input per statement | 32 MiB |
| Decoded source data per statement | 64 MiB |
| Rows / IPC bytes per batch | 16,384 / 1 MiB |
| Active source connections / waiting operations per catalog | 4 / 16 |

Configured aliases reserve their namespaces. A missing source table raises an
error even if a managed table has the same name. Writes, cross-owner joins,
arbitrary client-supplied paths, and unsupported types fail explicitly. The
[Spark Connect result and replay budgets](SPARK_CONNECT.md#limits-and-unsupported-operations)
also apply.

## Read from a shared Quack owner

Use this mode when an existing Quack endpoint owns the DuckDB database. Quack
is an [experimental DuckDB extension](https://github.com/duckdb/duckdb-quack).
Pin the owner to the tested DuckDB 2.0.0 line. The packaged server includes the
matching extension. A source build can specify `extension_path` for a local
matching extension binary.

Pass an entry with `alias`, `quack_uri`, and `token` in the server's
`SPARKLESS_DUCKDB_SOURCES` JSON array. Deliver the token through your deployment
secret mechanism and restrict its permissions on the Quack owner. Sparkless
limits its own source operations to reads. The owner's authorization controls
what the token can do directly.

Each statement reads from one owner transaction. A commit by an independent
writer becomes visible to the next statement. Cancelled scans interrupt the
local client and send cancellation to the Quack owner. See the
[deployment runbook](PRODUCTION.md) for the listener trust boundary and the
[development evidence](plans/sparkless-server/RELEASE_4_19_EVIDENCE.md) for the
standalone, shared-owner, and Spark differential checks.
