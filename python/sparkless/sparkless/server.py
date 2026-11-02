"""Compatibility facade for the optional shared server launcher package."""

try:
    from sparkless_connect.server import (
        AsyncSparklessServer,
        SparklessConnect,
        SparklessServer,
        SparklessServerError,
        UnsupportedServerPlatformError,
        UnsupportedSparkConnectError,
    )
except ModuleNotFoundError as error:
    if error.name != "sparkless_connect":
        raise
    raise ImportError(
        "server helpers require the optional server package; install "
        "sparkless[server], sparkless[server-duckdb], or sparkless-connect"
    ) from error

__all__ = [
    "AsyncSparklessServer",
    "SparklessConnect",
    "SparklessServer",
    "SparklessServerError",
    "UnsupportedServerPlatformError",
    "UnsupportedSparkConnectError",
]
