"""Keep local Sparkless benchmark runners from measuring each other."""

from contextlib import contextmanager
import os
from pathlib import Path
import tempfile
from typing import Iterator

if os.name == "nt":
    import msvcrt
else:
    import fcntl


@contextmanager
def benchmark_lock() -> Iterator[None]:
    # A per-user lock in the system temp directory covers separate checkouts and
    # output directories. Keep the file in place so all runs lock the same inode.
    user_id = (
        str(os.getuid())
        if hasattr(os, "getuid")
        else os.environ.get("USERNAME", "user")
    )
    path = Path(tempfile.gettempdir()) / f"sparkless-benchmark-{user_id}.lock"
    with path.open("a+b") as lock:
        try:
            if os.name == "nt":
                lock.seek(0)
                if not lock.read(1):
                    lock.seek(0)
                    lock.write(b"\0")
                    lock.flush()
                lock.seek(0)
                msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (BlockingIOError, OSError) as error:
            raise SystemExit(f"Another Sparkless benchmark holds {path}") from error
        yield
