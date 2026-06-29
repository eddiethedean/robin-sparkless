# Python examples

Runnable examples for the **Sparkless** Python package. Install first:

```bash
pip install "sparkless>=4,<5"
```

| Script | Description |
|--------|-------------|
| [basic_usage.py](basic_usage.py) | Create DataFrame, filter, aggregate, show |
| [sql_and_temp_views.py](sql_and_temp_views.py) | Temp views and `spark.sql()` |
| [test_dual_mode_example.py](test_dual_mode_example.py) | pytest dual-mode (sparkless or PySpark) |

Run scripts:

```bash
python examples/python/basic_usage.py
python examples/python/sql_and_temp_views.py
pytest examples/python/test_dual_mode_example.py -v
```

See [docs/python_getting_started.md](../docs/python_getting_started.md) and [docs/TESTING_GUIDE.md](../docs/TESTING_GUIDE.md).
