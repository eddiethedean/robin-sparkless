"""Check the fixed Cargo graph and compile a freshly extracted Python sdist."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile


def extract_source(archive: Path, destination: Path) -> Path:
    roots = set()
    with tarfile.open(archive, "r:gz") as source:
        for member in source.getmembers():
            path = Path(member.name)
            if path.is_absolute() or ".." in path.parts or not path.parts:
                raise ValueError("unsafe source archive path: " + member.name)
            roots.add(path.parts[0])
            target = destination / path
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
            elif member.isfile():
                target.parent.mkdir(parents=True, exist_ok=True)
                with source.extractfile(member) as data, target.open("wb") as output:
                    shutil.copyfileobj(data, output)
                target.chmod(member.mode & 0o777)
            else:
                raise ValueError("source archive contains a link or special file")
    if len(roots) != 1:
        raise ValueError("source archive must have exactly one root directory")
    return destination / roots.pop()


def verify_graph(metadata: dict, root: Path) -> None:
    compression = {
        "zstd": "0.14.3",
        "zstd-safe": "8.2.0",
        "zstd-sys": "2.1.2+zstd.1.5.7",
    }
    for name, expected in compression.items():
        versions = {p["version"] for p in metadata["packages"] if p["name"] == name}
        if versions != {expected}:
            raise ValueError("unexpected {} versions: {}".format(name, versions))
    for package in metadata["packages"]:
        if package["source"] is None:
            Path(package["manifest_path"]).resolve().relative_to(root.resolve())


def verify(
    archive: Path,
    offline: bool = False,
    wheel_output_dir: Path | None = None,
) -> None:
    with tempfile.TemporaryDirectory(prefix="sparkless-source-") as scratch:
        root = extract_source(archive, Path(scratch))
        manifest = root / "python/Cargo.toml"
        command = [
            "cargo",
            "metadata",
            "--locked",
            "--format-version",
            "1",
            "--manifest-path",
            str(manifest),
        ]
        if offline:
            command.append("--offline")
        metadata = json.loads(subprocess.check_output(command, cwd=root))
        verify_graph(metadata, root)
        lockfile = root / "Cargo.lock"
        locked_bytes = lockfile.read_bytes()
        if wheel_output_dir is None:
            command = [
                "cargo",
                "check",
                "--locked",
                "--release",
                "--manifest-path",
                str(manifest),
            ]
            if offline:
                command.append("--offline")
            subprocess.run(command, cwd=root, check=True)
        else:
            wheel_output_dir = wheel_output_dir.resolve()
            wheel_output_dir.mkdir(parents=True, exist_ok=True)
            environment = os.environ.copy()
            environment["CARGO_NET_OFFLINE"] = "true"
            subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "build",
                    "--wheel",
                    "--outdir",
                    str(wheel_output_dir),
                ],
                # The published sdist is rooted at the Python project and has
                # its Rust extension manifest in python/Cargo.toml. Building
                # from that nested Rust directory loses the root pyproject.
                cwd=root,
                env=environment,
                check=True,
            )
            if lockfile.read_bytes() != locked_bytes:
                raise ValueError(
                    "isolated source build modified the locked dependency graph"
                )
    if wheel_output_dir is not None and len(list(wheel_output_dir.glob("*.whl"))) != 1:
        raise ValueError("isolated source build must produce exactly one wheel")
    print("isolated Python source build and fixed compression graph verified")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path)
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--wheel-output-dir", type=Path)
    args = parser.parse_args()
    verify(args.archive.resolve(), args.offline, args.wheel_output_dir)


if __name__ == "__main__":
    main()
