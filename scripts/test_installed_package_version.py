"""Verify the installed release artifact, separately from backend-neutral tests."""

import ast
from importlib.metadata import version
from pathlib import Path
import unittest


class InstalledPackageVersionTests(unittest.TestCase):
    def test_installed_distribution_and_runtime_match_release_source(self):
        import sparkless

        source = (
            Path(__file__).resolve().parents[1]
            / "python/sparkless/sparkless/__init__.py"
        )
        assignments = [
            node
            for node in ast.parse(source.read_text()).body
            if isinstance(node, ast.Assign)
            and any(
                isinstance(target, ast.Name) and target.id == "__version__"
                for target in node.targets
            )
        ]
        self.assertEqual(len(assignments), 1)
        expected = ast.literal_eval(assignments[0].value)
        self.assertEqual(version("sparkless"), expected)
        self.assertEqual(sparkless.__version__, expected)


if __name__ == "__main__":
    unittest.main()
