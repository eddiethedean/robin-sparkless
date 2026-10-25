"""Locate the native server executable installed by this platform wheel."""

from __future__ import annotations

import os
import sys
from pathlib import Path

__version__ = "4.16.1"


def executable_path() -> str:
    name = "sparkless-server.exe" if os.name == "nt" else "sparkless-server"
    path = Path(__file__).resolve().parent / name
    if not path.is_file():
        platform = "{}-{}".format(sys.platform, getattr(os, "uname", lambda: None)())
        raise FileNotFoundError(
            "sparkless-server-bin has no precompiled executable for {}".format(platform)
        )
    return str(path)


__all__ = ["executable_path"]
