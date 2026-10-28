"""Tests for portable native-wheel dependency checks."""

from __future__ import annotations

import unittest

from verify_wheel_runtime import _missing_shared_libraries


class LinuxDependencyCheckTests(unittest.TestCase):
    def test_ignores_python_api_symbols_reported_by_musl_ldd(self) -> None:
        output = """
        /lib/ld-musl-x86_64.so.1 (0x7fa8bee1f000)
        Error relocating sparkless/_native.abi3.so: PyTuple_GetItem: symbol not found
        """
        self.assertEqual(_missing_shared_libraries(output), [])

    def test_reports_missing_shared_libraries_on_glibc_and_musl(self) -> None:
        output = """
        libzstd.so.1 => not found
        Error loading shared library libarrow.so: No such file or directory (needed by _native.abi3.so)
        """
        self.assertEqual(
            _missing_shared_libraries(output),
            [
                "libzstd.so.1 => not found",
                "Error loading shared library libarrow.so: No such file or directory (needed by _native.abi3.so)",
            ],
        )


if __name__ == "__main__":
    unittest.main()
