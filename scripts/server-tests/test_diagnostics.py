from __future__ import annotations

import threading
import unittest

from server_test_utils import SparklessServer


def _server_without_process() -> SparklessServer:
    server = object.__new__(SparklessServer)
    server._diagnostics_lock = threading.Lock()
    server._diagnostics = bytearray()
    server._diagnostic_pending = bytearray()
    server._diagnostic_discarding_line = False
    return server


class DiagnosticsRedactionTests(unittest.TestCase):
    def test_redacts_secrets_split_across_stderr_chunks(self) -> None:
        server = _server_without_process()

        server._append_diagnostics(b"tok")
        server._append_diagnostics(b"en=credential-value\n", final=True)

        self.assertNotIn(b"credential-value", server._diagnostics)
        self.assertIn(b"token=[redacted]", server._diagnostics)

    def test_truncates_an_unbounded_line_without_disclosing_it(self) -> None:
        server = _server_without_process()

        server._append_diagnostics(b"token=" + b"x" * 9000)
        server._append_diagnostics(b"secret\nnext line\n", final=True)

        self.assertNotIn(b"x" * 100, server._diagnostics)
        self.assertNotIn(b"secret", server._diagnostics)
        self.assertIn(b"next line", server._diagnostics)
