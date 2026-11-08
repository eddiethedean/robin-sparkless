# JDBC and SQLite Testing

> **4.20 native build prerequisite:** official Python wheels do not enable JDBC/SQLite.
> The `jdbc` extra installs test tools, not native features. Follow the
> [native feature build recipe](INSTALLATION.md#platforms-and-build-features)
> before these tests. SQLite needs no external service **after** its native feature
> is compiled; unavailable backends otherwise skip their tests.

This document describes how to run JDBC tests for all supported database backends.

## Supported Backends

| Backend | Feature | Crate Dependency | URL Scheme / Driver Hint |
|---------|---------|------------------|--------------------------|
| PostgreSQL | `jdbc` | `postgres` | `jdbc:postgresql:`, `postgres://`, `postgresql://` |
| SQLite | `sqlite` | `rusqlite` | `jdbc:sqlite:`, `sqlite:` |
| MySQL | `jdbc_mysql` | `mysql` | `jdbc:mysql:` / `com.mysql.cj.jdbc.Driver` |
| MariaDB | `jdbc_mariadb` | `mysql` | `jdbc:mariadb:` / `org.mariadb.jdbc.Driver` |
| SQL Server | `jdbc_mssql` | `tiberius` | `jdbc:sqlserver:` / `com.microsoft.sqlserver.jdbc.SQLServerDriver` |
| Oracle | `jdbc_oracle` | `oracle-rs` | `jdbc:oracle:` / `oracle.jdbc.OracleDriver` |
| DB2 | `jdbc_db2` | `odbc-api` | `jdbc:db2:` / `com.ibm.db2.jcc.DB2Driver` |

Enable one or more features via Cargo (e.g. `--features jdbc,sqlite,jdbc_mysql`).

## PySpark-Compatible JDBC Options

These are native database adapters with PySpark-style URL/options; they do not
load arbitrary Java JDBC classes. `driver` is a routing hint, not a JAR loader.
An accepted option does not imply Spark partitioning or fetch behavior.

### Per-driver option behavior

`Used` means the adapter consumes the option; it does not establish identical
Spark semantics or live integration coverage. PostgreSQL uses `jdbc`, MariaDB
shares the MySQL adapter, and DB2 requires a configured ODBC driver.

| Option | PostgreSQL | SQLite | MySQL / MariaDB | SQL Server | Oracle | DB2 |
| --- | --- | --- | --- | --- | --- | --- |
| `url`, `dbtable` or read `query` | Used | Used | Used | Used | Used | Used |
| `user`, `password` | Used | Not applicable | Used | Used | Used | Used |
| `sessionInitStatement` | Used | Used | Used | Used | Used | Used |
| Read `prepareQuery` | Used | Used | Used | Used | Used | Used |
| Read `queryTimeout` | Server statement timeout | SQLite busy/lock timeout | Server setting | Server setting | Call timeout | Accepted without effect |
| `fetchsize` | Attempts a server setting; unverified as a fetch contract | Accepted without effect | Accepted without effect | Accepted without effect | Accepted without effect | Accepted without effect |
| `partitionColumn`, `lowerBound`, `upperBound`, `numPartitions` | Accepted without partitioned execution | Same | Same | Same | Same | Same |
| Write `batchsize` | Used | Used | Used | Progress interval; no batching guarantee | Used | Used |
| Write `truncate` | Used | DELETE behavior | Used | Used | Used | Used |
| Write `cascadeTruncate` | Used with truncate | No effect | No effect | No effect | Used with truncate | No effect |

Timeouts have backend-specific meanings; a SQLite busy timeout is not a query
execution deadline. Do not size memory or concurrency on the assumption that
`numPartitions` partitions a read or that `fetchsize` bounds materialization.
Adapters collect database results into a local DataFrame. Inspect your exact
backend's integration results before relying on an unverified setting.

`SPARKLESS_HARDENED=1` disables arbitrary query/init/prepare SQL unless explicitly
re-enabled; use `dbtable`. See [configuration](CONFIGURATION.md).

### Save modes and partial writes

Use explicit `mode="append"`, `"overwrite"`, `"error"` or `"ignore"`.
These database adapters generally test existing **rows**, rather than table
existence, for error/ignore behavior. Overwrite can truncate or delete before
inserting. Driver transaction and batch behavior differ; do not assume a failed
multi-batch write rolled back every effect. Reconcile with the database before
retrying a write under a fresh operation identity.

### Runnable SQLite example

First build the native extension with `sqlite` using the
[feature recipe](INSTALLATION.md#platforms-and-build-features). Python extras
alone cannot enable it. Download
[jdbc_sqlite.py](https://github.com/eddiethedean/robin-sparkless/blob/main/examples/python/jdbc_sqlite.py)
and run `python jdbc_sqlite.py`. It creates disposable source/destination tables,
reads two rows, writes them through the native adapter, and verifies storage with
Python's independent SQLite reader. Expect `SQLite native read/write passed`.

For a configured PostgreSQL database, this is a **fragment** using an existing
`spark`, `url`, `props`, and `users` table:

```python
df = spark.read.jdbc(url, "users", properties=props)
df.write.jdbc(url, "users_backup", properties=props, mode="overwrite")
```

Prefer these methods to `format("jdbc").load()` / `.save()`: the current Python
`load(path)` and `save(path)` require a path, and generic file `save` does not
provide a JDBC writer. Direct `jdbc` properties override reader options.

## Quick Reference

| What | Command | Requires |
|------|---------|----------|
| **Rust unit tests** (URL routing, options) | `cargo test -p robin-sparkless-polars jdbc --features "jdbc,sqlite"` | Nothing |
| **Python SQLite tests** (no server) | `pytest tests/sql/test_jdbc_sqlite.py -v` | Nothing (uses temp files) |
| **Python integration** (Postgres) | `pytest tests/sql/test_jdbc_read_write_postgres.py -v` | Postgres + env vars |
| **Python integration** (MySQL) | `pytest tests/sql/test_jdbc_read_write_mysql.py -v` | MySQL + env vars |
| **Python integration** (MariaDB) | `pytest tests/sql/test_jdbc_read_write_mariadb.py -v` | MariaDB + env vars |
| **Python integration** (SQL Server) | `pytest tests/sql/test_jdbc_read_write_mssql.py -v` | SQL Server + env vars |
| **Python integration** (Oracle) | `pytest tests/sql/test_jdbc_read_write_oracle.py -v` | Oracle + env vars |
| **Python integration** (DB2) | `pytest tests/sql/test_jdbc_read_write_db2.py -v` | DB2 + ODBC + env vars |

All Python integration tests (except SQLite) are **skipped** if the corresponding env var is not set.

### SQLite Tests (No External Dependencies)

The `test_jdbc_sqlite.py` test suite uses temporary SQLite database files and requires **no external server or environment variables**. It provides comprehensive coverage of JDBC functionality:

```bash
# Run SQLite JDBC tests (32 tests)
pytest tests/sql/test_jdbc_sqlite.py -v

# Run with parallel workers
pytest tests/sql/test_jdbc_sqlite.py -n 12
```

These tests cover:
- Basic read/write operations with all save modes (append, overwrite, error, ignore)
- PySpark-compatible options: `sessionInitStatement`, `batchsize`, `truncate`, `prepareQuery`
- Data type roundtrips: integers, floats, nulls, special characters, booleans, large strings
- DataFrame operations after JDBC read: filter, select, groupBy, orderBy, count
- Multiple API patterns: `.option()`, `.options()`, properties dict

## Environment Variables

Each backend uses its own `SPARKLESS_TEST_JDBC_<DB>_URL`, `_USER`, `_PASSWORD`:

| Backend | URL Env Var | User Env Var | Password Env Var |
|---------|-------------|--------------|------------------|
| PostgreSQL | `SPARKLESS_TEST_JDBC_URL` | `SPARKLESS_TEST_JDBC_USER` | `SPARKLESS_TEST_JDBC_PASSWORD` |
| MySQL | `SPARKLESS_TEST_JDBC_MYSQL_URL` | `SPARKLESS_TEST_JDBC_MYSQL_USER` | `SPARKLESS_TEST_JDBC_MYSQL_PASSWORD` |
| MariaDB | `SPARKLESS_TEST_JDBC_MARIADB_URL` | `SPARKLESS_TEST_JDBC_MARIADB_USER` | `SPARKLESS_TEST_JDBC_MARIADB_PASSWORD` |
| SQL Server | `SPARKLESS_TEST_JDBC_MSSQL_URL` | `SPARKLESS_TEST_JDBC_MSSQL_USER` | `SPARKLESS_TEST_JDBC_MSSQL_PASSWORD` |
| Oracle | `SPARKLESS_TEST_JDBC_ORACLE_URL` | `SPARKLESS_TEST_JDBC_ORACLE_USER` | `SPARKLESS_TEST_JDBC_ORACLE_PASSWORD` |
| DB2 | `SPARKLESS_TEST_JDBC_DB2_URL` | `SPARKLESS_TEST_JDBC_DB2_USER` | `SPARKLESS_TEST_JDBC_DB2_PASSWORD` |

## Local Setup with Docker

**NOTE:** Tests use `testcontainers` by default, which automatically starts Docker containers. Manual setup is only needed for debugging or when testcontainers isn't available.

All database services are defined in a single `docker-compose.yml`:

```bash
# Start individual services
docker compose up postgres -d
docker compose up mysql mariadb -d

# Start all services
docker compose up -d

# Stop all services
docker compose down
```

### PostgreSQL (port 5432)

```bash
docker compose up postgres -d
export SPARKLESS_TEST_JDBC_URL="postgres://sparkless:sparkless@localhost:5432/sparkless"
export SPARKLESS_TEST_JDBC_USER=sparkless
export SPARKLESS_TEST_JDBC_PASSWORD=sparkless
```

### MySQL (port 3306)

```bash
docker compose up mysql -d
docker exec -i sparkless-mysql mysql -usparkless -psparkless sparkless < tests/sql/ddl/mysql.sql

export SPARKLESS_TEST_JDBC_MYSQL_URL="jdbc:mysql://localhost:3306/sparkless"
export SPARKLESS_TEST_JDBC_MYSQL_USER=sparkless
export SPARKLESS_TEST_JDBC_MYSQL_PASSWORD=sparkless
```

### MariaDB (port 3307)

```bash
docker compose up mariadb -d
docker exec -i sparkless-mariadb mariadb -usparkless -psparkless sparkless < tests/sql/ddl/mariadb.sql

export SPARKLESS_TEST_JDBC_MARIADB_URL="jdbc:mariadb://localhost:3307/sparkless"
export SPARKLESS_TEST_JDBC_MARIADB_USER=sparkless
export SPARKLESS_TEST_JDBC_MARIADB_PASSWORD=sparkless
```

### SQL Server (port 1433)

```bash
docker compose up mssql -d
# Wait for health check, then create tables
docker exec -i sparkless-mssql /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "Sparkless123!" -C -i /dev/stdin < tests/sql/ddl/mssql.sql

export SPARKLESS_TEST_JDBC_MSSQL_URL="jdbc:sqlserver://localhost:1433;databaseName=sparkless;encrypt=false;trustServerCertificate=true"
export SPARKLESS_TEST_JDBC_MSSQL_USER=sa
export SPARKLESS_TEST_JDBC_MSSQL_PASSWORD="Sparkless123!"
```

### Oracle (port 1521)

```bash
docker compose up oracle -d
# Wait ~2-3 minutes for startup, then:
docker exec -i sparkless-oracle sqlplus system/oracle@FREEPDB1 < tests/sql/ddl/oracle.sql

export SPARKLESS_TEST_JDBC_ORACLE_URL="jdbc:oracle:thin:@//localhost:1521/FREEPDB1"
export SPARKLESS_TEST_JDBC_ORACLE_USER=system
export SPARKLESS_TEST_JDBC_ORACLE_PASSWORD=oracle
```

### DB2 (port 50000)

DB2 requires ODBC driver manager (`brew install unixodbc` on macOS, `apt install unixodbc-dev` on Linux).

```bash
docker compose up db2 -d
# Wait several minutes for first startup, then:
docker exec -i sparkless-db2 su - db2inst1 -c "db2 connect to SPARKDB && db2 -tvf /dev/stdin" < tests/sql/ddl/db2.sql

export SPARKLESS_TEST_JDBC_DB2_URL="jdbc:db2://localhost:50000/SPARKDB"
export SPARKLESS_TEST_JDBC_DB2_USER=db2inst1
export SPARKLESS_TEST_JDBC_DB2_PASSWORD=password
```

## CI Strategy

For CI, you can:

1. **Default workflow**: Keep CI lightweight—only run Postgres tests (or skip all JDBC tests).
2. **Nightly / on-demand**: Add optional jobs that spin up each backend via service containers.

Example GitHub Actions job for MySQL:

```yaml
mysql-jdbc-tests:
  runs-on: ubuntu-latest
  services:
    mysql:
      image: mysql:8.0
      env:
        MYSQL_ROOT_PASSWORD: root
        MYSQL_DATABASE: sparkless
        MYSQL_USER: sparkless
        MYSQL_PASSWORD: sparkless
      ports:
        - 3306:3306
      options: >-
        --health-cmd="mysqladmin ping -h 127.0.0.1 -uroot -proot --silent"
        --health-interval=5s
        --health-timeout=5s
        --health-retries=10
  steps:
    - uses: actions/checkout@v4
    - name: Setup DDL
      run: mysql -h 127.0.0.1 -usparkless -psparkless sparkless < tests/sql/ddl/mysql.sql
    - name: Build (with jdbc_mysql)
      run: cargo build --features jdbc_mysql
    - name: Rust tests
      run: cargo test -p robin-sparkless-polars mysql --features jdbc_mysql
      env:
        ROBIN_SPARKLESS_TEST_JDBC_MYSQL_URL: "jdbc:mysql://127.0.0.1:3306/sparkless"
        ROBIN_SPARKLESS_TEST_JDBC_MYSQL_USER: sparkless
        ROBIN_SPARKLESS_TEST_JDBC_MYSQL_PASSWORD: sparkless
    - name: Python tests
      run: pytest tests/sql/test_jdbc_read_write_mysql.py -v
      env:
        SPARKLESS_TEST_JDBC_MYSQL_URL: "jdbc:mysql://127.0.0.1:3306/sparkless"
        SPARKLESS_TEST_JDBC_MYSQL_USER: sparkless
        SPARKLESS_TEST_JDBC_MYSQL_PASSWORD: sparkless
```

Repeat a similar pattern for each backend. Oracle and DB2 tests may be skipped in public CI due to licensing/image size constraints—run them locally or in private infrastructure.
