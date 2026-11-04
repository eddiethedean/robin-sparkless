"""Lightweight client and owned-server lifecycle helpers for Sparkless."""

from .server import (
    AsyncSparklessServer,
    SparklessConnect,
    SparklessServer,
    SparklessServerError,
    UnsupportedServerPlatformError,
    UnsupportedSparkConnectError,
)

__all__ = [
    "AsyncSparklessServer",
    "SparklessConnect",
    "SparklessServer",
    "SparklessServerError",
    "UnsupportedServerPlatformError",
    "UnsupportedSparkConnectError",
]
