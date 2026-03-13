# JDBC and SQLite Testing

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

## Quick Reference

| What | Command | Requires |
|------|---------|----------|
| **Rust unit tests** (URL routing, options) | `cargo test -p robin-sparkless-polars jdbc --features "jdbc,sqlite"` | Nothing |
| **Python integration** (Postgres) | `pytest tests/sql/test_jdbc_read_write_postgres.py -v` | Postgres + env vars |
| **Python integration** (MySQL) | `pytest tests/sql/test_jdbc_read_write_mysql.py -v` | MySQL + env vars |
| **Python integration** (MariaDB) | `pytest tests/sql/test_jdbc_read_write_mariadb.py -v` | MariaDB + env vars |
| **Python integration** (SQL Server) | `pytest tests/sql/test_jdbc_read_write_mssql.py -v` | SQL Server + env vars |
| **Python integration** (Oracle) | `pytest tests/sql/test_jdbc_read_write_oracle.py -v` | Oracle + env vars |
| **Python integration** (DB2) | `pytest tests/sql/test_jdbc_read_write_db2.py -v` | DB2 + ODBC + env vars |

All Python integration tests are **skipped** if the corresponding env var is not set.

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

Each backend has its own Docker Compose file under the repo root:

| Backend | Compose File | DDL Script |
|---------|--------------|------------|
| PostgreSQL | `docker-compose.jdbc.yml` | SQL in this doc |
| MySQL | `docker-compose.jdbc.mysql.yml` | `tests/sql/ddl/mysql.sql` |
| MariaDB | `docker-compose.jdbc.mariadb.yml` | `tests/sql/ddl/mariadb.sql` |
| SQL Server | `docker-compose.jdbc.mssql.yml` | `tests/sql/ddl/mssql.sql` |
| Oracle | `docker-compose.jdbc.oracle.yml` | `tests/sql/ddl/oracle.sql` |
| DB2 | `docker-compose.jdbc.db2.yml` | `tests/sql/ddl/db2.sql` |

### PostgreSQL (default)

```bash
# Start
docker compose -f docker-compose.jdbc.yml up -d

# Create tables
PGPASSWORD=sparkless psql -h localhost -U sparkless -d sparkless -c "
CREATE TABLE IF NOT EXISTS sparkless_jdbc_test (id BIGINT PRIMARY KEY, name TEXT);
INSERT INTO sparkless_jdbc_test (id, name) VALUES (1, 'a'), (2, 'b') ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS sparkless_jdbc_writeread_test (id BIGINT, name TEXT);
"

# Env
export SPARKLESS_TEST_JDBC_URL="postgres://sparkless:sparkless@localhost:5432/sparkless"
export SPARKLESS_TEST_JDBC_USER=sparkless
export SPARKLESS_TEST_JDBC_PASSWORD=sparkless
```

### MySQL

```bash
docker compose -f docker-compose.jdbc.mysql.yml up -d
docker exec -i sparkless-mysql mysql -usparkless -psparkless sparkless < tests/sql/ddl/mysql.sql

export SPARKLESS_TEST_JDBC_MYSQL_URL="jdbc:mysql://localhost:3306/sparkless"
export SPARKLESS_TEST_JDBC_MYSQL_USER=sparkless
export SPARKLESS_TEST_JDBC_MYSQL_PASSWORD=sparkless
```

### MariaDB

```bash
docker compose -f docker-compose.jdbc.mariadb.yml up -d
docker exec -i sparkless-mariadb mariadb -usparkless -psparkless sparkless < tests/sql/ddl/mariadb.sql

export SPARKLESS_TEST_JDBC_MARIADB_URL="jdbc:mariadb://localhost:3307/sparkless"
export SPARKLESS_TEST_JDBC_MARIADB_USER=sparkless
export SPARKLESS_TEST_JDBC_MARIADB_PASSWORD=sparkless
```

### SQL Server

```bash
docker compose -f docker-compose.jdbc.mssql.yml up -d
# Wait for health check, then create tables
docker exec -i sparkless-mssql /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "Sparkless123!" -C -i /dev/stdin < tests/sql/ddl/mssql.sql

export SPARKLESS_TEST_JDBC_MSSQL_URL="jdbc:sqlserver://localhost:1433;databaseName=sparkless;encrypt=false;trustServerCertificate=true"
export SPARKLESS_TEST_JDBC_MSSQL_USER=sa
export SPARKLESS_TEST_JDBC_MSSQL_PASSWORD="Sparkless123!"
```

### Oracle

```bash
docker compose -f docker-compose.jdbc.oracle.yml up -d
# Wait ~1-2 minutes for startup, then:
docker exec -i sparkless-oracle sqlplus system/oracle@FREEPDB1 < tests/sql/ddl/oracle.sql

export SPARKLESS_TEST_JDBC_ORACLE_URL="jdbc:oracle:thin:@//localhost:1521/FREEPDB1"
export SPARKLESS_TEST_JDBC_ORACLE_USER=system
export SPARKLESS_TEST_JDBC_ORACLE_PASSWORD=oracle
```

### DB2

DB2 requires ODBC driver manager (`brew install unixodbc` on macOS, `apt install unixodbc-dev` on Linux) and the IBM DB2 ODBC driver.

```bash
docker compose -f docker-compose.jdbc.db2.yml up -d
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
