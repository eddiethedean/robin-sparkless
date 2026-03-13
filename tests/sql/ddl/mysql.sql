-- MySQL schema for Sparkless JDBC tests.
-- Used by: tests/sql/test_jdbc_read_write_mysql.py

DROP TABLE IF EXISTS sparkless_jdbc_test;
CREATE TABLE sparkless_jdbc_test (
  id BIGINT PRIMARY KEY,
  name TEXT
);
INSERT INTO sparkless_jdbc_test (id, name) VALUES (1, 'a'), (2, 'b');

DROP TABLE IF EXISTS sparkless_jdbc_writeread_test;
CREATE TABLE sparkless_jdbc_writeread_test (
  id BIGINT,
  name TEXT
);

