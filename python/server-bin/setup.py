"""Build a platform wheel containing one precompiled server executable."""

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
        # Ask wheel to retain the current platform tag for the native payload.
        return True


class build_py(_build_py):
    def run(self):
        super().run()
        source = os.environ.get("SPARKLESS_SERVER_BINARY")
        if not source:
            raise RuntimeError(
                "SPARKLESS_SERVER_BINARY must point to the target platform's "
                "sparkless-server executable"
            )
        source_path = Path(source).expanduser()
        if not source_path.is_absolute():
            package_root = Path(__file__).resolve().parents[2]
            candidates = (Path.cwd() / source_path, package_root / source_path)
            source_path = next(
                (
                    candidate.resolve()
                    for candidate in candidates
                    if candidate.is_file()
                ),
                (Path.cwd() / source_path).resolve(),
            )
        if not source_path.is_file():
            raise RuntimeError("SPARKLESS_SERVER_BINARY does not name a file")
        name = "sparkless-server.exe" if os.name == "nt" else "sparkless-server"
        destination = Path(self.build_lib) / "sparkless_server_bin" / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(str(source_path), str(destination))
        if os.name != "nt":
            destination.chmod(
                destination.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH
            )


class bdist_wheel(_bdist_wheel):
    def get_tag(self):
        _, _, platform = super().get_tag()
        requested_platform = os.environ.get("SPARKLESS_SERVER_WHEEL_PLATFORM")
        if requested_platform:
            if not re.fullmatch(
                r"(?:macosx_\d+_\d+_(?:arm64|x86_64)|"
                r"manylinux_\d+_\d+_(?:x86_64|aarch64)|"
                r"musllinux_\d+_\d+_(?:x86_64|aarch64)|"
                r"linux_(?:x86_64|aarch64)|win_(?:amd64|arm64))",
                requested_platform,
            ):
                raise RuntimeError(
                    "SPARKLESS_SERVER_WHEEL_PLATFORM must be a concrete supported platform tag"
                )
            platform = requested_platform
        return "py3", "none", platform


setup(
    cmdclass={"build_py": build_py, "bdist_wheel": bdist_wheel},
    distclass=BinaryDistribution,
)
