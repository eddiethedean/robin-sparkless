-- SQL Server schema for Sparkless JDBC tests.
-- Used by: tests/sql/test_jdbc_read_write_mssql.py
-- Run via sqlcmd with -d sparkless (create DB if needed).

IF DB_ID('sparkless') IS NULL
BEGIN
  CREATE DATABASE sparkless;
END
GO

USE sparkless;
GO

IF OBJECT_ID('sparkless_jdbc_test', 'U') IS NOT NULL
  DROP TABLE sparkless_jdbc_test;
GO

CREATE TABLE sparkless_jdbc_test (
  id BIGINT PRIMARY KEY,
  name NVARCHAR(255)
);
GO

INSERT INTO sparkless_jdbc_test (id, name) VALUES (1, N'a'), (2, N'b');
GO

IF OBJECT_ID('sparkless_jdbc_writeread_test', 'U') IS NOT NULL
  DROP TABLE sparkless_jdbc_writeread_test;
GO

CREATE TABLE sparkless_jdbc_writeread_test (
  id BIGINT,
  name NVARCHAR(255)
);
GO

