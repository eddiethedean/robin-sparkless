# sparkless-server-duckdb

Run the Sparkless server with trusted, read-only DuckDB table sources.
Install the `connect` extra for the pinned `pyspark-client==4.0.5`.

```bash
python -m pip install "sparkless-server-duckdb[connect]==4.20.0"
```

```python
from sparkless_server_duckdb import SparklessConnect

with SparklessConnect() as spark:
    assert spark.range(10).filter("id > 5").count() == 4
```

This example uses the default Polars backend. Configure
`SPARKLESS_DUCKDB_SOURCES` on the server to expose read-only table aliases.
See the [DuckDB source guide](https://robin-sparkless.readthedocs.io/en/latest/DUCKDB_SOURCE/).

## Run Spark Connect plans in DuckDB

Set these variables before starting the server. Use a fresh database for this
example.

```sh
export SPARKLESS_EXECUTION_BACKEND=duckdb
export SPARKLESS_DUCKDB_EXECUTION_OWNER='{"mode":"local","database_path":"./events.duckdb"}'
```

```python
from sparkless_server_duckdb import SparklessConnect

with SparklessConnect() as spark:
    spark.sql("CREATE TABLE events (id BIGINT)").collect()
    spark.sql("INSERT INTO events VALUES (1)").collect()
    assert spark.table("events").collect()[0].id == 1
```

The execution owner holds writable data separately from the read-only source
aliases. In DuckDB mode, use Spark Connect for SQL.

See the [DuckDB execution guide](https://robin-sparkless.readthedocs.io/en/latest/DUCKDB_EXECUTION/)
for a local database quickstart, shared Quack ownership, and supported operations.
