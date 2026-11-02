"""Compatibility accessors for applications using the former server binary package."""


def executable_path() -> str:
    from sparkless_server import executable_path as base_executable_path

    return base_executable_path()


def quack_extension_path() -> str:
    try:
        from sparkless_server_duckdb import quack_extension_path as duckdb_extension_path
    except ImportError as error:
        raise FileNotFoundError(
            "install sparkless-server-duckdb to access the Quack extension"
        ) from error
    return duckdb_extension_path()


__all__ = ["executable_path", "quack_extension_path"]
