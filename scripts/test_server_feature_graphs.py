from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from verify_server_feature_graphs import verify_feature_graphs


class ServerFeatureGraphsTests(unittest.TestCase):
    def test_checks_and_retains_all_four_product_and_transport_graphs(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "graphs"

            def run(command, **_kwargs):
                package = command[command.index("-p") + 1]
                features = command[-1]
                if package == "sparkless-server-duckdb":
                    graph = "sparkless-server-duckdb\nrobin-sparkless-duckdb\n"
                elif features == "http":
                    graph = "sparkless-server\nsparkless-server-http\n"
                elif features == "connect":
                    graph = "sparkless-server\nsparkless-server-connect\n"
                else:
                    graph = "sparkless-server\nsparkless-server-http\nsparkless-server-connect\n"
                return subprocess.CompletedProcess(command, 0, graph, "")

            with patch(
                "verify_server_feature_graphs.subprocess.run", side_effect=run
            ) as cargo:
                paths = verify_feature_graphs(Path.cwd(), output)

            self.assertEqual(len(cargo.call_args_list), 4)
            self.assertEqual(
                {path.name for path in paths},
                {"base.txt", "http-only.txt", "connect-only.txt", "duckdb.txt"},
            )
            self.assertIn("robin-sparkless-duckdb", (output / "duckdb.txt").read_text())

    def test_rejects_duckdb_in_base_graph(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "graphs"

            def run(command, **_kwargs):
                package = command[command.index("-p") + 1]
                if package == "sparkless-server-duckdb":
                    return subprocess.CompletedProcess(
                        command, 0, "robin-sparkless-duckdb", ""
                    )
                return subprocess.CompletedProcess(command, 0, "duckdb v1.5.1", "")

            with patch("verify_server_feature_graphs.subprocess.run", side_effect=run):
                with self.assertRaisesRegex(ValueError, "unexpectedly contains"):
                    verify_feature_graphs(Path.cwd(), output)


if __name__ == "__main__":
    unittest.main()
