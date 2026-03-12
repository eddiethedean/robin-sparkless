# JDBC and SQLite Testing

The **JDBC** feature (PostgreSQL read/write) and **SQLite** feature are optional. This doc describes how to run JDBC and SQLite tests and the Rust example locally (Docker for Postgres; no server for SQLite).

## How to test (quick reference)

| What | Command | Requires |
|------|---------|----------|
| **Rust unit tests** (options + Postgres URL parsing) | `cargo test -p robin-sparkless-polars jdbc --features jdbc` | Nothing (no DB) |
| **Rust unit tests** (includes SQLite read/write roundtrip) | `cargo test -p robin-sparkless-polars jdbc --features "jdbc,sqlite"` | Nothing (temp file) |
| **Rust example** (Postgres) | `ROBIN_SPARKLESS_JDBC_URL='postgres://...' cargo run --example jdbc_postgres --features jdbc` | Postgres + tables + env |
| **Python integration tests** | `SPARKLESS_TEST_JDBC_URL='postgres://...' pytest tests/sql/test_jdbc_read_write_postgres.py -v` | Postgres + tables + env; Python built with `jdbc` |

- **SQLite**: Use URL `jdbc:sqlite:/path/to/file.db` (or `sqlite:...`). No server; enable with the `sqlite` feature. Rust tests include an in-process SQLite read/write roundtrip when `sqlite` is enabled.
- Without `SPARKLESS_TEST_JDBC_URL` / `ROBIN_SPARKLESS_JDBC_URL`, the Python tests are **skipped** and the Rust Postgres example exits with usage info.

## Local setup with Docker

1. **Start Postgres**

   From the repo root:

   ```bash
   docker compose -f docker-compose.jdbc.yml up -d
   ```

   Wait until the container is healthy (e.g. `docker compose -f docker-compose.jdbc.yml ps`).

2. **Create tables**

   Connect with any client (e.g. `psql` or GUI) to `postgres://sparkless:sparkless@localhost:5432/sparkless` and run:

   ```sql
   -- For the Rust example (examples/jdbc_postgres.rs)
   CREATE TABLE sparkless_jdbc_example (id BIGINT PRIMARY KEY, name TEXT);
   INSERT INTO sparkless_jdbc_example (id, name) VALUES (1, 'a'), (2, 'b');
   CREATE TABLE sparkless_jdbc_example_written (id BIGINT, name TEXT);

   -- For Python tests (tests/sql/test_jdbc_read_write_postgres.py)
   CREATE TABLE sparkless_jdbc_test (id BIGINT PRIMARY KEY, name TEXT);
   INSERT INTO sparkless_jdbc_test (id, name) VALUES (1, 'a'), (2, 'b');
   CREATE TABLE sparkless_jdbc_writeread_test (id BIGINT, name TEXT);
   ```

   Or in one shot with `psql`:

   ```bash
   PGPASSWORD=sparkless psql -h localhost -U sparkless -d sparkless -c "
   CREATE TABLE sparkless_jdbc_example (id BIGINT PRIMARY KEY, name TEXT);
   INSERT INTO sparkless_jdbc_example (id, name) VALUES (1, 'a'), (2, 'b');
   CREATE TABLE sparkless_jdbc_example_written (id BIGINT, name TEXT);
   CREATE TABLE sparkless_jdbc_test (id BIGINT PRIMARY KEY, name TEXT);
   INSERT INTO sparkless_jdbc_test (id, name) VALUES (1, 'a'), (2, 'b');
   CREATE TABLE sparkless_jdbc_writeread_test (id BIGINT, name TEXT);
   "
   ```

3. **Environment variables**

   ```bash
   export ROBIN_SPARKLESS_JDBC_URL="postgres://sparkless:sparkless@localhost:5432/sparkless"
   export SPARKLESS_TEST_JDBC_URL="$ROBIN_SPARKLESS_JDBC_URL"
   export SPARKLESS_TEST_JDBC_USER=sparkless
   export SPARKLESS_TEST_JDBC_PASSWORD=sparkless
   ```

## Rust example

Build and run the JDBC example (requires `jdbc` feature):

```bash
cargo run --example jdbc_postgres --features jdbc
```

Optional: set `ROBIN_SPARKLESS_JDBC_TABLE` if your table name is not `sparkless_jdbc_example`.

## Python tests

With the same env vars set, run the JDBC integration tests:

```bash
cd python && pip install -e ".[dev]" && cd ..
pytest tests/sql/test_jdbc_read_write_postgres.py -v
```

If `SPARKLESS_TEST_JDBC_URL` is not set, those tests are skipped.

## CI (optional)

To run JDBC tests in CI:

1. Add a job that starts Postgres (e.g. `docker compose -f docker-compose.jdbc.yml up -d` or a GitHub Actions service container).
2. Run the SQL above to create tables (e.g. via a step that runs `psql` or a small script).
3. Set `SPARKLESS_TEST_JDBC_URL`, `SPARKLESS_TEST_JDBC_USER`, `SPARKLESS_TEST_JDBC_PASSWORD` in the job env.
4. Run Rust tests with `cargo test --features jdbc` (if you add JDBC unit tests) and/or run the example; run Python tests as above.

Example GitHub Actions service container (add to a job):

```yaml
services:
  postgres:
    image: postgres:16-alpine
    env:
      POSTGRES_USER: sparkless
      POSTGRES_PASSWORD: sparkless
      POSTGRES_DB: sparkless
    ports:
      - 5432:5432
    options: >-
      --health-cmd "pg_isready -U sparkless -d sparkless"
      --health-interval 5s
      --health-timeout 5s
      --health-retries 5
```

Then set `SPARKLESS_TEST_JDBC_URL=postgres://sparkless:sparkless@localhost:5432/sparkless` (and user/password) and run the Python JDBC tests.
