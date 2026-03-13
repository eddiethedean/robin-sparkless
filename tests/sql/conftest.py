"""
JDBC test fixtures using testcontainers.

Provides database containers for PostgreSQL, MySQL, MariaDB, MSSQL, Oracle, and DB2.
Containers are session-scoped for efficiency and automatically cleaned up after tests.
"""

from __future__ import annotations

import os
import time
from typing import Generator, NamedTuple

import pytest


class JdbcConnection(NamedTuple):
    """JDBC connection details."""

    url: str
    user: str
    password: str
    properties: dict[str, str]


def _is_docker_available() -> bool:
    """Check if Docker is available and running."""
    try:
        import docker

        client = docker.from_env()
        client.ping()
        return True
    except Exception:
        return False


def _wait_for_container(container, timeout: int = 60) -> bool:
    """Wait for container to be ready."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            logs = container.get_logs()
            if isinstance(logs, bytes):
                logs = logs.decode("utf-8", errors="replace")
            if container.get_wrapped_container().status == "running":
                return True
        except Exception:
            pass
        time.sleep(1)
    return False


# PostgreSQL fixture
@pytest.fixture(scope="session")
def postgres_container() -> Generator[JdbcConnection, None, None]:
    """Start a PostgreSQL container for testing."""
    if not _is_docker_available():
        pytest.skip("Docker is not available")

    try:
        from testcontainers.postgres import PostgresContainer
    except ImportError:
        pytest.skip("testcontainers[postgres] not installed")

    try:
        import psycopg2
    except ImportError:
        pytest.skip("psycopg2 not installed - run: pip install psycopg2-binary")

    with PostgresContainer("postgres:15-alpine") as postgres:
        # Wait for PostgreSQL to be ready
        host = postgres.get_container_host_ip()
        port = postgres.get_exposed_port(5432)

        # Retry connection a few times
        for attempt in range(10):
            try:
                conn = psycopg2.connect(
                    host=host,
                    port=port,
                    user=postgres.username,
                    password=postgres.password,
                    database=postgres.dbname,
                )
                break
            except Exception:
                if attempt == 9:
                    pytest.skip("PostgreSQL container failed to start")
                time.sleep(2)

        # Create test tables
        cursor = conn.cursor()
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS sparkless_jdbc_test (id BIGINT PRIMARY KEY, name TEXT)"
        )
        cursor.execute(
            "INSERT INTO sparkless_jdbc_test (id, name) VALUES (1, 'a'), (2, 'b') ON CONFLICT DO NOTHING"
        )
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS sparkless_jdbc_writeread_test (id BIGINT, name TEXT)"
        )
        conn.commit()
        cursor.close()
        conn.close()

        # Use postgres:// URL format which the Rust driver expects
        url = f"postgres://{postgres.username}:{postgres.password}@{host}:{port}/{postgres.dbname}"
        yield JdbcConnection(
            url=url,
            user=postgres.username,
            password=postgres.password,
            properties={"user": postgres.username, "password": postgres.password},
        )


@pytest.fixture
def postgres_jdbc(postgres_container: JdbcConnection) -> JdbcConnection:
    """Get PostgreSQL JDBC connection details."""
    return postgres_container


# MySQL fixture
@pytest.fixture(scope="session")
def mysql_container() -> Generator[JdbcConnection, None, None]:
    """Start a MySQL container for testing."""
    if not _is_docker_available():
        pytest.skip("Docker is not available")

    try:
        from testcontainers.mysql import MySqlContainer
    except ImportError:
        pytest.skip("testcontainers[mysql] not installed")

    try:
        import pymysql
    except ImportError:
        pytest.skip("pymysql not installed - run: pip install pymysql")

    with MySqlContainer("mysql:8.0") as mysql:
        host = mysql.get_container_host_ip()
        port = int(mysql.get_exposed_port(3306))

        # Retry connection a few times
        for attempt in range(15):
            try:
                conn = pymysql.connect(
                    host=host,
                    port=port,
                    user=mysql.username,
                    password=mysql.password,
                    database=mysql.dbname,
                )
                break
            except Exception:
                if attempt == 14:
                    pytest.skip("MySQL container failed to start")
                time.sleep(2)

        # Create test tables
        cursor = conn.cursor()
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS sparkless_jdbc_test (id BIGINT PRIMARY KEY, name TEXT)"
        )
        cursor.execute(
            "INSERT IGNORE INTO sparkless_jdbc_test (id, name) VALUES (1, 'a'), (2, 'b')"
        )
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS sparkless_jdbc_writeread_test (id BIGINT, name TEXT)"
        )
        conn.commit()
        cursor.close()
        conn.close()

        # Use mysql:// URL format which the Rust driver expects
        url = f"mysql://{mysql.username}:{mysql.password}@{host}:{port}/{mysql.dbname}"
        yield JdbcConnection(
            url=url,
            user=mysql.username,
            password=mysql.password,
            properties={
                "user": mysql.username,
                "password": mysql.password,
                "driver": "com.mysql.cj.jdbc.Driver",
            },
        )


@pytest.fixture
def mysql_jdbc(mysql_container: JdbcConnection) -> JdbcConnection:
    """Get MySQL JDBC connection details."""
    return mysql_container


# MariaDB fixture - uses MySQL container since MariaDB is MySQL-compatible
@pytest.fixture(scope="session")
def mariadb_container() -> Generator[JdbcConnection, None, None]:
    """Start a MariaDB-compatible container for testing (uses MySQL image for simplicity)."""
    if not _is_docker_available():
        pytest.skip("Docker is not available")

    try:
        from testcontainers.mysql import MySqlContainer
    except ImportError:
        pytest.skip("testcontainers[mysql] not installed")

    try:
        import pymysql
    except ImportError:
        pytest.skip("pymysql not installed - run: pip install pymysql")

    # Use MariaDB image through generic MySQL container
    with MySqlContainer("mariadb:10.11") as mariadb:
        host = mariadb.get_container_host_ip()
        port = int(mariadb.get_exposed_port(3306))

        # Retry connection a few times
        for attempt in range(20):
            try:
                conn = pymysql.connect(
                    host=host,
                    port=port,
                    user=mariadb.username,
                    password=mariadb.password,
                    database=mariadb.dbname,
                )
                break
            except Exception:
                if attempt == 19:
                    pytest.skip("MariaDB container failed to start")
                time.sleep(2)

        # Create test tables
        cursor = conn.cursor()
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS sparkless_jdbc_test (id BIGINT PRIMARY KEY, name TEXT)"
        )
        cursor.execute(
            "INSERT IGNORE INTO sparkless_jdbc_test (id, name) VALUES (1, 'a'), (2, 'b')"
        )
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS sparkless_jdbc_writeread_test (id BIGINT, name TEXT)"
        )
        conn.commit()
        cursor.close()
        conn.close()

        # Use mysql:// URL format (MariaDB is MySQL-compatible)
        url = f"mysql://{mariadb.username}:{mariadb.password}@{host}:{port}/{mariadb.dbname}"
        yield JdbcConnection(
            url=url,
            user=mariadb.username,
            password=mariadb.password,
            properties={
                "user": mariadb.username,
                "password": mariadb.password,
                "driver": "org.mariadb.jdbc.Driver",
            },
        )


@pytest.fixture
def mariadb_jdbc(mariadb_container: JdbcConnection) -> JdbcConnection:
    """Get MariaDB JDBC connection details."""
    return mariadb_container


# SQL Server fixture
@pytest.fixture(scope="session")
def mssql_container() -> Generator[JdbcConnection, None, None]:
    """Start a SQL Server container for testing."""
    if not _is_docker_available():
        pytest.skip("Docker is not available")

    try:
        from testcontainers.mssql import SqlServerContainer
    except ImportError:
        pytest.skip("testcontainers[mssql] not installed")

    try:
        import pymssql
    except ImportError:
        pytest.skip("pymssql not installed - run: pip install pymssql")

    password = "Strong_Password123!"
    with SqlServerContainer(
        "mcr.microsoft.com/mssql/server:2019-latest", password=password
    ) as mssql:
        host = mssql.get_container_host_ip()
        port = int(mssql.get_exposed_port(1433))

        # SQL Server takes a while to start
        for attempt in range(30):
            try:
                conn = pymssql.connect(
                    server=host,
                    port=port,
                    user="sa",
                    password=password,
                    database="master",
                )
                break
            except Exception:
                if attempt == 29:
                    pytest.skip("SQL Server container failed to start")
                time.sleep(3)

        # Create test tables
        cursor = conn.cursor()
        cursor.execute(
            "IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='sparkless_jdbc_test') "
            "CREATE TABLE sparkless_jdbc_test (id BIGINT PRIMARY KEY, name NVARCHAR(255))"
        )
        cursor.execute(
            "IF NOT EXISTS (SELECT * FROM sparkless_jdbc_test WHERE id=1) "
            "INSERT INTO sparkless_jdbc_test (id, name) VALUES (1, 'a')"
        )
        cursor.execute(
            "IF NOT EXISTS (SELECT * FROM sparkless_jdbc_test WHERE id=2) "
            "INSERT INTO sparkless_jdbc_test (id, name) VALUES (2, 'b')"
        )
        cursor.execute(
            "IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='sparkless_jdbc_writeread_test') "
            "CREATE TABLE sparkless_jdbc_writeread_test (id BIGINT, name NVARCHAR(255))"
        )
        conn.commit()
        cursor.close()
        conn.close()

        # Use jdbc:sqlserver:// URL format expected by sparkless
        url = f"jdbc:sqlserver://{host}:{port};databaseName=master;user=sa;password={password}"
        yield JdbcConnection(
            url=url,
            user="sa",
            password=password,
            properties={
                "user": "sa",
                "password": password,
                "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            },
        )


@pytest.fixture
def mssql_jdbc(mssql_container: JdbcConnection) -> JdbcConnection:
    """Get SQL Server JDBC connection details."""
    return mssql_container


# Oracle fixture
@pytest.fixture(scope="session")
def oracle_container() -> Generator[JdbcConnection, None, None]:
    """Start an Oracle container for testing.

    NOTE: Oracle containers take 2-3+ minutes to start, which exceeds normal
    test timeouts. Set SPARKLESS_TEST_ORACLE=1 to enable Oracle tests.
    """
    if not os.environ.get("SPARKLESS_TEST_ORACLE"):
        pytest.skip(
            "Oracle tests disabled by default (slow container startup). "
            "Set SPARKLESS_TEST_ORACLE=1 to enable."
        )

    if not _is_docker_available():
        pytest.skip("Docker is not available")

    try:
        from testcontainers.oracle import OracleDbContainer
    except ImportError:
        pytest.skip("testcontainers[oracle] not installed")

    try:
        import oracledb
    except ImportError:
        pytest.skip("oracledb not installed - run: pip install oracledb")

    # Oracle containers are heavy; use Oracle XE for testing
    with OracleDbContainer("gvenzl/oracle-xe:21-slim") as oracle:
        host = oracle.get_container_host_ip()
        port = oracle.get_exposed_port(1521)

        # Oracle takes a while to start - retry connection (up to 3 minutes)
        for attempt in range(36):
            try:
                conn = oracledb.connect(
                    user="system",
                    password=oracle.password,
                    dsn=f"{host}:{port}/XEPDB1",
                )
                break
            except Exception:
                if attempt == 35:
                    pytest.skip("Oracle container failed to start")
                time.sleep(5)

        # Create test tables
        cursor = conn.cursor()
        try:
            cursor.execute(
                "CREATE TABLE sparkless_jdbc_test (id NUMBER(19) PRIMARY KEY, name VARCHAR2(255))"
            )
        except Exception:
            pass  # Table may already exist
        try:
            cursor.execute("INSERT INTO sparkless_jdbc_test (id, name) VALUES (1, 'a')")
            cursor.execute("INSERT INTO sparkless_jdbc_test (id, name) VALUES (2, 'b')")
        except Exception:
            pass  # Data may already exist
        try:
            cursor.execute(
                "CREATE TABLE sparkless_jdbc_writeread_test (id NUMBER(19), name VARCHAR2(255))"
            )
        except Exception:
            pass
        conn.commit()
        cursor.close()
        conn.close()

        # Use jdbc:oracle:thin:@ URL format expected by sparkless
        url = f"jdbc:oracle:thin:system/{oracle.password}@{host}:{port}/XEPDB1"
        yield JdbcConnection(
            url=url,
            user="system",
            password=oracle.password,
            properties={
                "user": "system",
                "password": oracle.password,
                "driver": "oracle.jdbc.OracleDriver",
            },
        )


@pytest.fixture
def oracle_jdbc(oracle_container: JdbcConnection) -> JdbcConnection:
    """Get Oracle JDBC connection details."""
    return oracle_container


# DB2 fixture
@pytest.fixture(scope="session")
def db2_container() -> Generator[JdbcConnection, None, None]:
    """Start a DB2 container for testing."""
    import platform

    # DB2 Docker images are not available for ARM64 (Apple Silicon)
    if platform.machine() in ("arm64", "aarch64"):
        pytest.skip("DB2 Docker image not available for ARM64 architecture")

    if not _is_docker_available():
        pytest.skip("Docker is not available")

    try:
        from testcontainers.db2 import Db2Container  # noqa: F401
    except ImportError:
        pytest.skip("testcontainers[db2] not installed")

    try:
        # DB2 requires ibm_db driver which is complex to install
        pytest.skip("DB2 tests require ibm_db driver which requires additional setup")
    except Exception:
        pass

    # Note: DB2 container is heavy and may not work on all systems
    pytest.skip("DB2 container tests disabled - requires specialized setup")


@pytest.fixture
def db2_jdbc(db2_container: JdbcConnection) -> JdbcConnection:
    """Get DB2 JDBC connection details."""
    return db2_container
