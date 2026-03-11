# Issue #1355: PySpark parity — SQL SHOW DATABASES / SHOW TABLES / DESCRIBE EXTENDED

**Goal:** Support in SQL:
- `SHOW DATABASES` (return list of database names; can be stub e.g. single "default")
- `SHOW TABLES` / `SHOW TABLES IN database` (return list of table names from session catalog)
- `DESCRIBE EXTENDED table_name` (return schema + metadata for temp view or saved table)

**Current state:** Parser/executor returns error: "only SELECT, CREATE SCHEMA/DATABASE, DROP TABLE/VIEW/SCHEMA, and DESCRIBE are supported". Tests in `test_show_describe.py`, `test_show_tables` fail.

**Implementation directions:**
1. Extend SQL parser to recognize `SHOW DATABASES`, `SHOW TABLES`, `SHOW TABLES IN db`, `DESCRIBE [EXTENDED] table`.
2. Add execution path for each: query session catalog (listTables, tableExists, get schema for table) and return result as a DataFrame or equivalent so `spark.sql("SHOW TABLES").collect()` works.
3. DESCRIBE EXTENDED: return columns (e.g. col_name, data_type, comment) plus optional extended metadata.

**Reference:** docs/test_failure_categories.md §2, docs/PYSPARK_DIFFERENCES.md.
