"""Base Polars server product and compatibility imports."""

from pathlib import Path
import os
from typing import Any

from sparkless_connect import (
    AsyncSparklessServer as _AsyncSparklessServer,
    SparklessConnect as _SparklessConnect,
    SparklessServer as _SparklessServer,
    SparklessServerError,
    UnsupportedServerPlatformError,
    UnsupportedSparkConnectError,
)


def executable_path() -> str:
    name = "sparkless-server.exe" if os.name == "nt" else "sparkless-server"
    path = Path(__file__).resolve().parent / name
    if not path.is_file():
        raise FileNotFoundError("sparkless-server has no binary for this platform")
    return str(path)


class SparklessServer(_SparklessServer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        if "product" in kwargs:
            raise TypeError("the server product is selected by the package")
        super().__init__(*args, product="base", **kwargs)


class AsyncSparklessServer(_AsyncSparklessServer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        if "product" in kwargs:
            raise TypeError("the server product is selected by the package")
        super().__init__(*args, product="base", **kwargs)


class SparklessConnect(_SparklessConnect):
    server_class = SparklessServer


__version__ = "4.20.0"
__all__ = [
    "AsyncSparklessServer",
    "SparklessConnect",
    "SparklessServer",
    "SparklessServerError",
    "UnsupportedServerPlatformError",
    "UnsupportedSparkConnectError",
    "executable_path",
]
