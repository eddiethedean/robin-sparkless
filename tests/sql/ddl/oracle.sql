-- Oracle schema for Sparkless JDBC tests.
-- Used by: tests/sql/test_jdbc_read_write_oracle.py
-- Run via sqlplus system/oracle@FREEPDB1

BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE sparkless_jdbc_test';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

CREATE TABLE sparkless_jdbc_test (
  id NUMBER(19) PRIMARY KEY,
  name VARCHAR2(255)
);
INSERT INTO sparkless_jdbc_test (id, name) VALUES (1, 'a');
INSERT INTO sparkless_jdbc_test (id, name) VALUES (2, 'b');
COMMIT;

BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE sparkless_jdbc_writeread_test';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

CREATE TABLE sparkless_jdbc_writeread_test (
  id NUMBER(19),
  name VARCHAR2(255)
);
COMMIT;
