"""Owned native HTTP and Spark Connect server lifecycle helpers.

The launcher starts only the selected product's packaged executable or an
explicit executable path supplied by the caller. It never downloads, compiles,
or searches ``PATH`` for a server binary.
"""

from __future__ import annotations

from ._common import (
    SparklessServerError,
    UnsupportedServerPlatformError,
    UnsupportedSparkConnectError,
)
from ._sync import SparklessServer
from ._async import AsyncSparklessServer
from ._client import SparklessConnect

__all__ = [
    "AsyncSparklessServer",
    "SparklessConnect",
    "SparklessServer",
    "SparklessServerError",
    "UnsupportedServerPlatformError",
    "UnsupportedSparkConnectError",
]
