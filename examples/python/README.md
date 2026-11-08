# Python examples

Run these examples from the repository root with Sparkless 4.20.0. Start in a
Python 3.11 or newer virtual environment.

```bash
python -m pip install "sparkless==4.20.0"
```

| Script | Description |
|--------|-------------|
| [api_workflows.py](api_workflows.py) | Nulls, conditionals, joins, aggregation, windows and Arrow (requires PyArrow) |
| [warehouse_roundtrip.py](warehouse_roundtrip.py) | Separate writer/reader processes with disposable warehouse |
| [external_provider.py](external_provider.py) | Delta provider commit, receipt and rejected-prefix validation (requires PyArrow and native Delta) |
| [jdbc_sqlite.py](jdbc_sqlite.py) | Independent SQLite read/write verification (requires a native `sqlite` build) |
| [http_recovery.py](http_recovery.py) | Same-instance HTTP replay and restart boundary (requires base server) |
| [duckdb_execution.py](duckdb_execution.py) | Connect writes, independent DuckDB read and restart (requires DuckDB server/client) |
| [basic_usage.py](basic_usage.py) | Create DataFrame, filter, aggregate, show |
| [sql_and_temp_views.py](sql_and_temp_views.py) | Temp views and `spark.sql()` |
| [connect_load_query.py](connect_load_query.py) | Load CSV rows over Spark Connect, filter/project/count, export JSON |
| [test_dual_mode_example.py](test_dual_mode_example.py) | Actual backend/JVM identity plus aggregation |
| [test_pipeline_example.py](test_pipeline_example.py) | CSV, type normalization, join, aggregate, Parquet round trip |
| [test_udf_example.py](test_udf_example.py) | Typed scalar Python UDF and comparison filter |
| [test_local_contract_example.py](test_local_contract_example.py) | Local RDD and bounded compatibility behavior |

## Run locally

```bash
python examples/python/basic_usage.py
python examples/python/sql_and_temp_views.py
python -m pip install "sparkless[testing]==4.20.0"
python -m pytest examples/python/test_dual_mode_example.py -v
```

## Run over Spark Connect

Install the base server and its matching Connect client.

```bash
python -m pip install "sparkless-server[connect]==4.20.0"
python examples/python/connect_load_query.py --output-dir /tmp/sparkless-connect-output
```

Pass `--server-bin /absolute/path/to/sparkless-server` to use a locally built
binary instead of the packaged server. The example creates `orders.csv`, parses
it in Python, sends explicitly typed rows through Connect, runs filter, projection,
and count queries on the native server, and exports the collected results to
`high_value_orders.json`. It verifies exact results and closes both the session
and server. CSV parsing, sorting the returned rows, and JSON export happen in the
client because the Connect server does not expose file readers or writes.

See [docs/python_getting_started.md](../../docs/python_getting_started.md) and [docs/TESTING_GUIDE.md](../../docs/TESTING_GUIDE.md).

## Run the same pytest examples on PySpark

The adjacent `conftest.py` loads `sparkless_testing.plugin`. Use a separate
virtual environment and a JDK 17 installation for the PySpark 3.5 profile.

```bash
python -m pip install "sparkless[testing,pyspark]==4.20.0"
SPARKLESS_TEST_MODE=pyspark SPARKLESS_PYSPARK_COMPAT=3.5 python -m pytest examples/python -v -s
```

The identity test unwraps the plugin's JDBC session adapter and verifies the
actual PySpark session plus a live JVM witness. See the
[pipeline tutorial](../../docs/PIPELINE_TUTORIAL.md) and
[package install instructions](../../docs/INSTALLATION.md).

Complete example dependencies and continuous-check profiles are recorded in the
[executable inventory](../../docs/EXAMPLES.md). The local wheel does not enable
SQLite; follow [native feature setup](../../docs/INSTALLATION.md#platforms-and-build-features)
for that example. Server product platforms differ from local-engine platforms.
