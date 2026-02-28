## Category: SQL SHOW / DESCRIBE not supported

**Error:** `SparklessError: SQL: only SELECT, CREATE SCHEMA/DATABASE, DROP TABLE/VIEW/SCHEMA/DATABASE, and DESCRIBE are supported`

SHOW DATABASES, SHOW TABLES, and DESCRIBE EXTENDED (parsed as table name) are not supported.

### Reproduction (sparkless, fails)

```python
from sparkless.sql import SparkSession

spark = SparkSession.builder.app_name("test").get_or_create()
spark.sql("SHOW DATABASES").collect()   # SparklessError
spark.sql("SHOW TABLES").collect()      # SparklessError
spark.sql("SHOW TABLES IN default").collect()
```

### Expected behavior (PySpark)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.sql("SHOW DATABASES").show()
spark.sql("SHOW TABLES").show()
spark.sql("SHOW TABLES IN default").show()
```

### Tests to pass or fix

- tests/upstream_sparkless/tests/parity/sql/test_show_describe.py::TestSQLShowDescribeParity::test_show_databases
- tests/upstream_sparkless/tests/parity/sql/test_show_describe.py::TestSQLShowDescribeParity::test_show_tables
- tests/upstream_sparkless/tests/parity/sql/test_show_describe.py::TestSQLShowDescribeParity::test_show_tables_in_database
- tests/upstream_sparkless/tests/parity/sql/test_show_describe.py::TestSQLShowDescribeParity::test_describe_extended

**Action:** Extend SQL for SHOW/DESCRIBE or skip these tests for robin backend.
