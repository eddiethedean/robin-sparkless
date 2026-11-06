"""DuckDB server product and its product-bound lifecycle helpers."""

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
    name = "sparkless-server-duckdb.exe" if os.name == "nt" else "sparkless-server-duckdb"
    path = Path(__file__).resolve().parent / name
    if not path.is_file():
        raise FileNotFoundError("sparkless-server-duckdb has no binary for this platform")
    return str(path)


def quack_extension_path() -> str:
    path = Path(__file__).resolve().parent / "quack.duckdb_extension"
    if not path.is_file():
        raise FileNotFoundError("sparkless-server-duckdb has no bundled Quack extension")
    return str(path)


class SparklessServer(_SparklessServer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        if "product" in kwargs:
            raise TypeError("the server product is selected by the package")
        super().__init__(*args, product="duckdb", **kwargs)


class AsyncSparklessServer(_AsyncSparklessServer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        if "product" in kwargs:
            raise TypeError("the server product is selected by the package")
        super().__init__(*args, product="duckdb", **kwargs)


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
    "quack_extension_path",
]
