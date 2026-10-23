"""Build a platform wheel containing a prebuilt sparkless-server binary."""

from __future__ import annotations

import os
import shutil
from pathlib import Path

from setuptools import Distribution, setup
from setuptools.command.bdist_wheel import bdist_wheel
from setuptools.command.build_py import build_py


class BinaryBuildPy(build_py):
    def run(self) -> None:
        super().run()
        binary = os.environ.get("SPARKLESS_SERVER_BINARY")
        if not binary:
            raise RuntimeError(
                "SPARKLESS_SERVER_BINARY must point to a compiled sparkless-server executable"
            )
        source = Path(binary).resolve()
        if not source.is_file():
            raise RuntimeError("SPARKLESS_SERVER_BINARY does not exist: {}".format(source))
        name = "sparkless-server.exe" if os.name == "nt" else "sparkless-server"
        destination = Path(self.build_lib) / "sparkless_server_bin" / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(str(source), str(destination))
        if os.name != "nt":
            destination.chmod(destination.stat().st_mode | 0o111)

    def get_outputs(self, include_bytecode: int = 1) -> list[str]:
        outputs = super().get_outputs(include_bytecode)
        name = "sparkless-server.exe" if os.name == "nt" else "sparkless-server"
        binary = str(Path(self.build_lib) / "sparkless_server_bin" / name)
        if os.path.isfile(binary):
            outputs.append(binary)
        return outputs


class BinaryDistribution(Distribution):
    def has_ext_modules(self) -> bool:
        # This is a platform wheel even though it contains no Python extension.
        return True


class PlatformWheel(bdist_wheel):
    def finalize_options(self) -> None:
        super().finalize_options()
        self.root_is_pure = False
        platform_override = os.environ.get("SPARKLESS_SERVER_WHEEL_PLAT_NAME")
        if platform_override:
            self.plat_name = platform_override
            self.plat_name_supplied = True

    def get_tag(self) -> tuple[str, str, str]:
        _, _, platform_tag = super().get_tag()
        # The package contains a native executable but no CPython extension or
        # ABI-specific code, so one wheel serves every supported Python 3 minor.
        return "py3", "none", platform_tag


setup(
    cmdclass={"build_py": BinaryBuildPy, "bdist_wheel": PlatformWheel},
    distclass=BinaryDistribution,
)
