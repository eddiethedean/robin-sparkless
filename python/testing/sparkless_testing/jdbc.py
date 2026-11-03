"""JDBC compatibility utilities for PySpark testing.

This module provides wrappers that automatically inject JDBC driver class names
based on URL patterns, making tests work seamlessly with both Sparkless and PySpark.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

# Map of JDBC URL prefixes to driver class names
JDBC_DRIVERS: Dict[str, str] = {
    "jdbc:sqlite:": "org.sqlite.JDBC",
    "jdbc:mysql:": "com.mysql.cj.jdbc.Driver",
    "jdbc:mariadb:": "org.mariadb.jdbc.Driver",
    "jdbc:postgresql:": "org.postgresql.Driver",
    "jdbc:sqlserver:": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "jdbc:oracle:": "oracle.jdbc.OracleDriver",
    "jdbc:db2:": "com.ibm.db2.jcc.DB2Driver",
}


def get_driver_for_url(url: str) -> Optional[str]:
    """Get the JDBC driver class name for a given URL.

    Args:
        url: JDBC connection URL (e.g., "jdbc:sqlite:test.db")

    Returns:
        Driver class name if known, None otherwise.
    """
    for prefix, driver in JDBC_DRIVERS.items():
        if url.startswith(prefix):
            return driver
    return None


def inject_driver_property(
    url: str, properties: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Inject the driver property into JDBC properties if needed.

    Args:
        url: JDBC connection URL
        properties: Existing properties dict (or None)

    Returns:
        Properties dict with driver added if needed.
    """
    props = dict(properties) if properties else {}

    # Don't override if driver is already specified
    if "driver" in props:
        return props

    driver = get_driver_for_url(url)
    if driver:
        props["driver"] = driver

    return props


class JdbcDataFrameReaderWrapper:
    """Wrapper for PySpark DataFrameReader that auto-injects JDBC drivers."""

    def __init__(self, reader: Any):
        self._reader = reader
        self._options: Dict[str, str] = {}

    def __getattr__(self, name: str) -> Any:
        # Forward all attribute access to the underlying reader
        return getattr(self._reader, name)

    def option(self, key: str, value: Any) -> "JdbcDataFrameReaderWrapper":
        """Store option and forward to underlying reader."""
        self._options[key] = str(value)
        self._reader = self._reader.option(key, value)
        return self

    def options(self, opts: Dict[str, Any]) -> "JdbcDataFrameReaderWrapper":
        """Store options and forward to underlying reader."""
        for k, v in opts.items():
            self._options[k] = str(v)
        # PySpark's options() takes keyword args, not a dict positional arg
        # Use individual option() calls instead
        for k, v in opts.items():
            self._reader = self._reader.option(k, v)
        return self

    def format(self, source: str) -> "JdbcDataFrameReaderWrapper":
        """Forward format call."""
        self._reader = self._reader.format(source)
        return self

    def load(self, path: Optional[str] = None) -> "JdbcDataFrameWrapper":
        """Load data, injecting driver if this is a JDBC read."""
        # If we have a URL but no driver, inject the driver
        url = self._options.get("url", "")
        if url and "driver" not in self._options:
            driver = get_driver_for_url(url)
            if driver:
                self._reader = self._reader.option("driver", driver)

        # PySpark's load() doesn't require a path for JDBC
        if path is not None:
            df = self._reader.load(path)
        else:
            df = self._reader.load()
        return JdbcDataFrameWrapper(df)

    def jdbc(
        self,
        url: str,
        table: str,
        properties: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> "JdbcDataFrameWrapper":
        """Read from JDBC with auto-injected driver."""
        props = inject_driver_property(url, properties)
        df = self._reader.jdbc(url=url, table=table, properties=props, **kwargs)
        return JdbcDataFrameWrapper(df)


class JdbcDataFrameWriterWrapper:
    """Wrapper for PySpark DataFrameWriter that auto-injects JDBC drivers."""

    def __init__(self, writer: Any):
        self._writer = writer
        self._options: Dict[str, str] = {}

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._writer, name)
        if callable(attr):

            def wrapper(*args: Any, **kwargs: Any) -> Any:
                result = attr(*args, **kwargs)
                # If result is the writer (method chaining), wrap it
                if result is self._writer or type(result).__name__ == "DataFrameWriter":
                    self._writer = result
                    return self
                return result

            return wrapper
        return attr

    def option(self, key: str, value: Any) -> "JdbcDataFrameWriterWrapper":
        """Store option and forward to underlying writer."""
        self._options[key] = str(value)
        self._writer = self._writer.option(key, value)
        return self

    def options(self, options: Dict[str, Any]) -> "JdbcDataFrameWriterWrapper":
        """Store options and forward to underlying writer."""
        for k, v in options.items():
            self._options[k] = str(v)
        self._writer = self._writer.options(options)
        return self

    def mode(self, saveMode: str) -> "JdbcDataFrameWriterWrapper":
        """Set save mode."""
        self._writer = self._writer.mode(saveMode)
        return self

    def format(self, source: str) -> "JdbcDataFrameWriterWrapper":
        """Set format."""
        self._writer = self._writer.format(source)
        return self

    def jdbc(
        self,
        url: str,
        table: str,
        mode: Optional[str] = None,
        properties: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Write to JDBC with auto-injected driver."""
        props = inject_driver_property(url, properties)
        self._writer.jdbc(url=url, table=table, mode=mode, properties=props)

    def save(self, path: Optional[str] = None) -> None:
        """Save data, injecting driver if this is a JDBC write."""
        # If we have a URL but no driver in options, inject the driver
        url = self._options.get("url", "")
        if url and "driver" not in self._options:
            driver = get_driver_for_url(url)
            if driver:
                self._writer = self._writer.option("driver", driver)

        if path is not None:
            self._writer.save(path)
        else:
            self._writer.save()


class CatalogShimWrapper:
    """Shim for PySpark Catalog used in JDBC-parity tests.

    Important: PySpark's public Catalog does **not** expose createDatabase/dropDatabase
    helpers; tests assert that these attributes are absent (AttributeError). To match
    PySpark's API surface, this wrapper forwards all attribute access to the underlying
    Catalog instance without adding new public methods.
    """

    def __init__(self, catalog: Any):
        self._catalog = catalog

    def __getattr__(self, name: str) -> Any:
        return getattr(self._catalog, name)


class JdbcSessionWrapper:
    """Wrapper for PySpark SparkSession that provides JDBC-aware readers/writers."""

    def __init__(self, session: Any):
        self._session = session

    @property
    def catalog(self) -> Any:
        """Return a catalog wrapper with sparkless-compatible helpers."""
        return CatalogShimWrapper(self._session.catalog)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._session, name)

    @property
    def read(self) -> JdbcDataFrameReaderWrapper:
        """Return a JDBC-aware DataFrameReader."""
        return JdbcDataFrameReaderWrapper(self._session.read)

    def createDataFrame(self, *args: Any, **kwargs: Any) -> Any:
        """Create a DataFrame with JDBC-aware writer."""
        df = self._session.createDataFrame(*args, **kwargs)
        return JdbcDataFrameWrapper(df)


class JdbcDataFrameWrapper:
    """Wrapper for PySpark DataFrame that provides JDBC-aware writer."""

    def __init__(self, df: Any):
        self._df = df

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._df, name)
        # Wrap methods that return DataFrames
        if callable(attr):

            def wrapper(*args: Any, **kwargs: Any) -> Any:
                result = attr(*args, **kwargs)
                # If result is a DataFrame, wrap it
                if hasattr(result, "write") and hasattr(result, "collect"):
                    return JdbcDataFrameWrapper(result)
                return result

            return wrapper
        return attr

    def __getitem__(self, item: Any) -> Any:
        """Support column access via df['col'] or df[col]."""
        return self._df[item]

    @property
    def write(self) -> JdbcDataFrameWriterWrapper:
        """Return a JDBC-aware DataFrameWriter."""
        return JdbcDataFrameWriterWrapper(self._df.write)

    def collect(self) -> Any:
        """Collect data."""
        return self._df.collect()

    def count(self) -> int:
        """Count rows."""
        result = self._df.count()
        # Both PySpark DataFrame.count() and sparkless DataFrame.count()
        # return an int, but mypy sees this as Any via the untyped JVM bridge.
        return int(result)

    @property
    def schema(self) -> Any:
        """Get schema."""
        return self._df.schema

    def filter(self, condition: Any) -> "JdbcDataFrameWrapper":
        """Filter with wrapping."""
        return JdbcDataFrameWrapper(self._df.filter(condition))

    def select(self, *cols: Any) -> "JdbcDataFrameWrapper":
        """Select with wrapping."""
        return JdbcDataFrameWrapper(self._df.select(*cols))

    def groupBy(self, *cols: Any) -> Any:
        """GroupBy - returns GroupedData which doesn't need wrapping."""
        return self._df.groupBy(*cols)

    def orderBy(self, *cols: Any) -> "JdbcDataFrameWrapper":
        """OrderBy with wrapping."""
        return JdbcDataFrameWrapper(self._df.orderBy(*cols))


def wrap_session_for_jdbc(session: Any) -> Any:
    """Wrap a PySpark session to auto-inject JDBC drivers.

    This wrapper intercepts read.jdbc() and write.jdbc() calls to
    automatically add the appropriate driver class based on the URL.

    Args:
        session: PySpark SparkSession instance

    Returns:
        Wrapped session with JDBC driver auto-injection
    """
    return JdbcSessionWrapper(session)
