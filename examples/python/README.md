# Python examples

Runnable examples for the **Sparkless** Python package. Install first:

```bash
pip install "sparkless>=4,<5"
```

| Script | Description |
|--------|-------------|
| [basic_usage.py](basic_usage.py) | Create DataFrame, filter, aggregate, show |
| [sql_and_temp_views.py](sql_and_temp_views.py) | Temp views and `spark.sql()` |
| [connect_load_query.py](connect_load_query.py) | Load CSV rows over Spark Connect, filter/project/count, export JSON |
| [test_dual_mode_example.py](test_dual_mode_example.py) | Actual backend/JVM identity plus aggregation |
| [test_pipeline_example.py](test_pipeline_example.py) | CSV, type normalization, join, aggregate, Parquet round trip |
| [test_udf_example.py](test_udf_example.py) | Typed scalar Python UDF and comparison filter |
| [test_local_contract_example.py](test_local_contract_example.py) | Local RDD and bounded compatibility behavior |

Run scripts:

```bash
python examples/python/basic_usage.py
python examples/python/sql_and_temp_views.py
pytest examples/python/test_dual_mode_example.py -v
```

The Connect example requires the **4.18 candidate** Python package, a matching
Connect-enabled native server, and `pyspark-client==4.0.5`. With those installed:

```bash
python examples/python/connect_load_query.py --output-dir /tmp/sparkless-connect-output
```

Pass `--server-bin /absolute/path/to/sparkless-server` to use a locally built
binary instead of the packaged server. The example creates `orders.csv`, parses
it in Python, sends explicitly typed rows through Connect, runs filter, projection,
and count queries on the native server, and exports the collected results to
`high_value_orders.json`. It verifies exact results and closes both the session
and server. CSV parsing, sorting the returned rows, and JSON export happen in the
client because file readers, SQL, joins, and writes are outside this Connect subset.

See [docs/python_getting_started.md](../../docs/python_getting_started.md) and [docs/TESTING_GUIDE.md](../../docs/TESTING_GUIDE.md).

Use the adjacent `conftest.py` to activate `sparkless.testing.plugin`. To check
all pytest examples on real Spark (Java and the appropriate extra required):

```bash
SPARKLESS_TEST_MODE=pyspark python -m pytest examples/python -v -s
```

The identity test unwraps the plugin's JDBC session adapter and verifies the
actual PySpark session plus a live JVM witness. See the
[pipeline tutorial](../../docs/PIPELINE_TUTORIAL.md) and
[candidate install instructions](../../docs/INSTALLATION.md).
