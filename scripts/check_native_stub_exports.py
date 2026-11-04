#!/usr/bin/env python3
"""Ensure the sole native module stub matches the installed PyO3 exports."""

from __future__ import annotations

import ast
from pathlib import Path
import sys


def main() -> int:
    import sparkless_native._native as native

    if "sparkless" in sys.modules:
        raise SystemExit("importing the native module loaded the local API package")
    root = Path(__file__).resolve().parents[1]
    stub = root / "python/native/sparkless_native/_native.pyi"
    module = ast.parse(stub.read_text(encoding="utf-8"))
    declared = {
        node.name
        for node in module.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
    }
    actual = {name for name in dir(native) if not name.startswith("__")}
    missing = sorted(actual - declared)
    stale = sorted(declared - actual)
    if missing or stale:
        if missing:
            print("Missing native stub exports: " + ", ".join(missing), file=sys.stderr)
        if stale:
            print("Stale native stub exports: " + ", ".join(stale), file=sys.stderr)
        return 1
    api_stub = root / "python/sparkless/sparkless/_native.pyi"
    server_facade = root / "python/sparkless/sparkless/server.py"
    if api_stub.exists() or server_facade.exists():
        raise SystemExit("a removed compatibility stub or server facade remains")
    print("native stub matches all canonical extension exports")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
