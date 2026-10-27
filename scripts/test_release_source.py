"""Regression coverage for source archives with Cargo patches."""

import importlib.util
import io
from pathlib import Path
import sys
import tarfile
import tempfile
import types
import unittest
from unittest.mock import patch

from verify_release_source import extract_source, verify_graph


class SourceDistributionTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        spec = importlib.util.spec_from_file_location(
            "sparkless_build_test",
            Path(__file__).resolve().parents[1] / "python/_sparkless_build.py",
        )
        self.backend = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, {"maturin": types.SimpleNamespace()}):
            spec.loader.exec_module(self.backend)

    def archive(self, name="project/pyproject.toml"):
        archive = self.root / "project.tar.gz"
        with tarfile.open(archive, "w:gz") as output:
            member = tarfile.TarInfo(name)
            member.size = 4
            output.addfile(member, io.BytesIO(b"test"))
        return archive

    def test_local_patches_and_backend_survive_repeated_source_packaging(self):
        vendor = self.root / "vendor/fixed"
        vendor.mkdir(parents=True)
        (vendor / "Cargo.toml").write_text('[package]\nname="fixed"\nversion="1.0.0"\n')
        (vendor / "lib.rs").write_text("// fixed compression")
        (self.root / "Cargo.toml").write_text(
            '[workspace]\n[patch.crates-io]\nfixed={path="vendor/fixed"}\n'
        )
        archive = self.archive()
        self.backend._complete_sdist(archive, self.root)
        self.backend._complete_sdist(archive, self.root)
        unpacked = extract_source(archive, self.root / "consumer")
        self.assertEqual(
            (unpacked / "vendor/fixed/lib.rs").read_text(), "// fixed compression"
        )
        self.assertTrue((unpacked / "_sparkless_build.py").is_file())
        with tarfile.open(archive) as source:
            names = source.getnames()
            self.assertEqual(len(names), len(set(names)))

    def test_missing_patch_fails_source_packaging(self):
        (self.root / "Cargo.toml").write_text(
            '[patch.crates-io]\nfixed={path="vendor/missing"}\n'
        )
        with self.assertRaisesRegex(RuntimeError, "missing local Cargo patch"):
            self.backend._complete_sdist(self.archive(), self.root)

    def test_archive_cannot_escape_extraction_directory(self):
        with self.assertRaisesRegex(ValueError, "unsafe source archive"):
            extract_source(self.archive("../escape"), self.root / "consumer")
        self.assertFalse((self.root / "escape").exists())

    def test_old_compression_graph_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "unexpected zstd versions"):
            verify_graph(
                {"packages": [{"name": "zstd", "version": "0.13.3"}]}, self.root
            )

    def test_local_dependencies_must_come_from_archive(self):
        packages = [
            {"name": name, "version": version, "source": "registry"}
            for name, version in [
                ("zstd", "0.14.3"),
                ("zstd-safe", "8.2.0"),
                ("zstd-sys", "2.1.2+zstd.1.5.7"),
            ]
        ]
        packages.append(
            {
                "name": "outside",
                "version": "1",
                "source": None,
                "manifest_path": "/outside/Cargo.toml",
            }
        )
        with self.assertRaises(ValueError):
            verify_graph({"packages": packages}, self.root)


if __name__ == "__main__":
    unittest.main()
