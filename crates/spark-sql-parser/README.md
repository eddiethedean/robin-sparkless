# spark-sql-parser

Parse SQL into [sqlparser](https://github.com/sqlparser-rs/sqlparser-rs) AST with a Spark-style subset of statements.

Part of [robin-sparkless](https://github.com/eddiethedean/robin-sparkless).

## Usage

```rust
use spark_sql_parser::parse_sql;

let stmt = parse_sql("SELECT 1 FROM t")?;
// Returns sqlparser::ast::Statement on success
```

One statement per call; pass a single SQL statement. Execution and translation are left to the caller.

## Supported statements

- **Queries:** `SELECT`, `WITH ... SELECT`, etc. (any `Query` from sqlparser)
- **DDL:** `CREATE TABLE`, `CREATE VIEW`, `CREATE FUNCTION`, `CREATE SCHEMA` / `CREATE DATABASE`
- **DDL:** `ALTER TABLE`, `ALTER VIEW`, `ALTER SCHEMA`
- **DDL:** `DROP TABLE`, `DROP VIEW`, `DROP SCHEMA`, `DROP FUNCTION`
- **DML:** `INSERT INTO ... SELECT`, `INSERT OVERWRITE DIRECTORY ... SELECT`, `LOAD DATA` (when dialect supports it)
- **Utility:** `USE`, `TRUNCATE`, `DECLARE` (cursor)
- **Utility:** `SHOW TABLES`, `SHOW DATABASES`, `SHOW SCHEMAS`, `SHOW FUNCTIONS`, `SHOW COLUMNS`, `SHOW VIEWS`, `SHOW CREATE`
- **Utility:** `DESCRIBE` / `DESC` (table), `EXPLAIN`, `SET`, `RESET`, `CACHE TABLE`, `UNCACHE TABLE`

Parsing uses sqlparser’s [GenericDialect](https://docs.rs/sqlparser/latest/sqlparser/dialect/struct.GenericDialect.html). Some Spark-specific syntax (e.g. `LOAD DATA` with `LOCAL`) is only parsed by other dialects (e.g. HiveDialect).

## Known gaps

- **Single statement only** — one statement per `parse_sql` call.
- **Query clauses** — Spark-only clauses such as `DISTRIBUTE BY`, `CLUSTER BY`, `SORT BY` may not be recognized; behavior depends on the upstream parser and dialect.
- **REFRESH TABLE** — not exposed as a top-level statement in sqlparser.

## License

MIT. See the [repository](https://github.com/eddiethedean/robin-sparkless) for details.
