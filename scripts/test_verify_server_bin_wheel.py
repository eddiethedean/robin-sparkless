"""Tests for installed server-product wheel verification."""

from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import verify_server_bin_wheel


class InstalledServerWheelTests(unittest.TestCase):
    def test_duckdb_product_is_forwarded_to_binary_verifier(self):
        with tempfile.TemporaryDirectory() as temporary:
            package_dir = Path(temporary)
            executable = package_dir / "sparkless-server-duckdb"
            extension = package_dir / "quack.duckdb_extension"
            executable.write_bytes(b"server executable")
            extension.write_bytes(b"pinned extension")

            package = Mock()
            package.executable_path.return_value = str(executable)
            package.quack_extension_path.return_value = str(extension)

            with (
                patch.object(
                    verify_server_bin_wheel.importlib.metadata,
                    "version",
                    return_value="4.19.0",
                ) as distribution_version,
                patch.object(
                    verify_server_bin_wheel.importlib,
                    "import_module",
                    return_value=package,
                ) as import_module,
                patch.object(verify_server_bin_wheel, "verify_binary") as verify_binary,
            ):
                verify_server_bin_wheel._smoke_installed_binary(
                    "4.19.0",
                    connect=True,
                    duckdb_source=True,
                    product="duckdb",
                )

            distribution_version.assert_called_once_with("sparkless-server-duckdb")
            import_module.assert_called_once_with("sparkless_server_duckdb")
            verify_binary.assert_called_once_with(
                executable,
                "4.19.0",
                connect=True,
                connect_client=False,
                client_python=None,
                product="duckdb",
            )


if __name__ == "__main__":
    unittest.main()
