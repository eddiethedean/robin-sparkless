-- DB2 schema for Sparkless JDBC tests.
-- Used by: tests/sql/test_jdbc_read_write_db2.py
-- Run via: docker exec -i sparkless-db2 su - db2inst1 -c "db2 connect to SPARKDB && db2 -tvf /dev/stdin" < tests/sql/ddl/db2.sql

DROP TABLE sparkless_jdbc_test;
CREATE TABLE sparkless_jdbc_test (
  id BIGINT NOT NULL PRIMARY KEY,
  name VARCHAR(255)
);
INSERT INTO sparkless_jdbc_test (id, name) VALUES (1, 'a'), (2, 'b');

DROP TABLE sparkless_jdbc_writeread_test;
CREATE TABLE sparkless_jdbc_writeread_test (
  id BIGINT,
  name VARCHAR(255)
);
COMMIT;
