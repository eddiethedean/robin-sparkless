"""Verify the installed release artifact, separately from backend-neutral tests."""

import ast
from importlib.metadata import version, distribution
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

    def test_api_and_engine_have_separate_files_and_compatible_imports(self):
        import sparkless._native
        import sparkless_native._native
        import tomllib

        root = Path(__file__).resolve().parents[1]
        engine = tomllib.loads((root / "python/native/pyproject.toml").read_text())[
            "project"
        ]["version"]
        self.assertEqual(version("sparkless-native"), engine)
        self.assertIs(sparkless._native, sparkless_native._native)
        api_files = {str(path) for path in distribution("sparkless").files}
        native_files = {str(path) for path in distribution("sparkless-native").files}
        self.assertFalse(api_files & native_files)
        self.assertFalse(
            any(path.endswith((".so", ".pyd", ".dll")) for path in api_files)
        )
        self.assertTrue(any(path.endswith((".so", ".pyd")) for path in native_files))


if __name__ == "__main__":
    unittest.main()
