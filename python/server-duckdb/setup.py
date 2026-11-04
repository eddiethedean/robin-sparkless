"""Build the platform wheel containing the DuckDB server and matching Quack extension."""

from __future__ import annotations

import os
import re
import shutil
import stat
from pathlib import Path

from setuptools import Distribution, setup
from setuptools.command.build_py import build_py as _build_py
from wheel.bdist_wheel import bdist_wheel as _bdist_wheel


class BinaryDistribution(Distribution):
    def has_ext_modules(self):
        return True


class build_py(_build_py):
    def run(self):
        super().run()
        source = os.environ.get("SPARKLESS_SERVER_BINARY")
        extension = os.environ.get("SPARKLESS_QUACK_EXTENSION_BINARY")
        if not source or not extension:
            raise RuntimeError(
                "SPARKLESS_SERVER_BINARY and SPARKLESS_QUACK_EXTENSION_BINARY are required"
            )
        source_path = Path(source).expanduser().resolve()
        extension_path = Path(extension).expanduser().resolve()
        if not source_path.is_file():
            raise RuntimeError("SPARKLESS_SERVER_BINARY does not name a file")
        if not extension_path.is_file() or extension_path.name != "quack.duckdb_extension":
            raise RuntimeError("SPARKLESS_QUACK_EXTENSION_BINARY must name the matching Quack extension")
        package_dir = Path(self.build_lib) / "sparkless_server_duckdb"
        package_dir.mkdir(parents=True, exist_ok=True)
        binary_name = "sparkless-server-duckdb.exe" if os.name == "nt" else "sparkless-server-duckdb"
        binary_destination = package_dir / binary_name
        shutil.copy2(source_path, binary_destination)
        if os.name != "nt":
            binary_destination.chmod(binary_destination.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        shutil.copy2(extension_path, package_dir / "quack.duckdb_extension")
        notice = Path(__file__).parent / "sparkless_server_duckdb" / "NOTICE-quack.txt"
        if not notice.is_file():
            raise RuntimeError("the Quack extension license notice is missing")
        shutil.copy2(notice, package_dir / "NOTICE-quack.txt")


class bdist_wheel(_bdist_wheel):
    def get_tag(self):
        _, _, platform = super().get_tag()
        requested = os.environ.get("SPARKLESS_SERVER_WHEEL_PLATFORM")
        if requested:
            if not re.fullmatch(
                r"(?:macosx_\d+_\d+_(?:arm64|x86_64)|manylinux_\d+_\d+_(?:x86_64|aarch64)|musllinux_\d+_\d+_(?:x86_64|aarch64)|linux_(?:x86_64|aarch64)|win_(?:amd64|arm64))",
                requested,
            ):
                raise RuntimeError("invalid SPARKLESS_SERVER_WHEEL_PLATFORM")
            platform = requested
        return "py3", "none", platform


setup(cmdclass={"build_py": build_py, "bdist_wheel": bdist_wheel}, distclass=BinaryDistribution)
