import tomllib
import unittest

from verify_rust_consumer_graph import consumer_manifest


class DownstreamConsumerManifestTest(unittest.TestCase):
    def test_workspace_patches_cannot_leak_into_consumer_audit(self):
        source = """[workspace]
members = [".", "python"]
[package]
name = "consumer"
version = "4.17.0"
[patch.crates-io]
zstd = { path = "vendor/fixed" }
[dev-dependencies]
tempfile = "3"
"""
        manifest = tomllib.loads(consumer_manifest(source))
        self.assertNotIn("patch", manifest)
        self.assertNotIn("python", manifest["workspace"]["members"])
        self.assertIn("crates/sparkless-server", manifest["workspace"]["members"])
        self.assertEqual(manifest["dev-dependencies"], {"tempfile": "3"})
