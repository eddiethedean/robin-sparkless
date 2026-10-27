"""PEP 517 backend: maturin builds plus the workspace's local Cargo patches.

Maturin follows path dependencies but does not collect [patch.crates-io] paths.
Keep these sources in the sdist so builds use the same fixed compression graph.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path
import tarfile
import tempfile

import maturin

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib


def __getattr__(name):
    return getattr(maturin, name)


def _workspace_root():
    for directory in (
        Path(__file__).resolve().parent,
        *Path(__file__).resolve().parents,
    ):
        manifest = directory / "Cargo.toml"
        if manifest.is_file() and "workspace" in tomllib.loads(manifest.read_text()):
            return directory
    raise RuntimeError("cannot locate the Cargo workspace for source packaging")


def _complete_sdist(archive_path, workspace):
    workspace = workspace.resolve()
    manifest = tomllib.loads((workspace / "Cargo.toml").read_text())
    additions = {}
    for patch in manifest.get("patch", {}).get("crates-io", {}).values():
        if "path" not in patch:
            continue
        source = (workspace / patch["path"]).resolve()
        relative = source.relative_to(workspace)
        if not (source / "Cargo.toml").is_file():
            raise RuntimeError("missing local Cargo patch: {}".format(source))
        for path in sorted(source.rglob("*")):
            if any(
                part in {".git", "target", "__pycache__", ".DS_Store"}
                for part in path.parts
            ):
                continue
            if path.is_symlink():
                raise RuntimeError(
                    "source patches must not contain symlinks: {}".format(path)
                )
            if path.is_file():
                additions[(relative / path.relative_to(source)).as_posix()] = path
    additions["_sparkless_build.py"] = Path(__file__).resolve()

    with tempfile.TemporaryDirectory(dir=archive_path.parent) as scratch:
        completed = Path(scratch) / archive_path.name
        with (
            tarfile.open(archive_path, "r:gz") as original,
            tarfile.open(completed, "w:gz") as output,
        ):
            members = original.getmembers()
            roots = {member.name.split("/")[0] for member in members}
            if len(roots) != 1:
                raise RuntimeError("sdist must have exactly one root directory")
            prefix = roots.pop() + "/"
            for member in members:
                if member.name[len(prefix) :] in additions:
                    continue
                output.addfile(
                    member, original.extractfile(member) if member.isfile() else None
                )
            for relative, source in sorted(additions.items()):
                output.add(source, arcname=prefix + relative, recursive=False)
        os.replace(completed, archive_path)


def _lock_sdist(archive_path):
    # Maturin removes workspace members that the Python extension does not need.
    # Let Cargo prune their lock entries before shipping a --locked consumer build.
    with tempfile.TemporaryDirectory(dir=archive_path.parent) as scratch:
        destination = Path(scratch) / "source"
        with tarfile.open(archive_path, "r:gz") as archive:
            members = archive.getmembers()
            roots = {member.name.split("/")[0] for member in members}
            if len(roots) != 1:
                raise RuntimeError("sdist must have exactly one root directory")
            prefix = roots.pop()
            for member in members:
                path = Path(member.name)
                if path.is_absolute() or ".." in path.parts:
                    raise RuntimeError("unsafe source archive path: " + member.name)
                target = destination / path
                if member.isdir():
                    target.mkdir(parents=True, exist_ok=True)
                elif member.isfile():
                    target.parent.mkdir(parents=True, exist_ok=True)
                    with (
                        archive.extractfile(member) as data,
                        target.open("wb") as output,
                    ):
                        shutil.copyfileobj(data, output)
                else:
                    raise RuntimeError("source archive contains a link or special file")
        root = destination / prefix
        subprocess.run(
            [
                "cargo",
                "metadata",
                "--offline",
                "--format-version",
                "1",
                "--manifest-path",
                str(root / "python/Cargo.toml"),
            ],
            cwd=root,
            stdout=subprocess.DEVNULL,
            check=True,
        )
        completed = Path(scratch) / archive_path.name
        with (
            tarfile.open(archive_path, "r:gz") as original,
            tarfile.open(completed, "w:gz") as output,
        ):
            for member in original.getmembers():
                if member.name != prefix + "/Cargo.lock":
                    output.addfile(
                        member,
                        original.extractfile(member) if member.isfile() else None,
                    )
            output.add(
                root / "Cargo.lock", arcname=prefix + "/Cargo.lock", recursive=False
            )
        os.replace(completed, archive_path)


def build_sdist(sdist_directory, config_settings=None):
    filename = maturin.build_sdist(sdist_directory, config_settings)
    archive = Path(sdist_directory).resolve() / filename
    _complete_sdist(archive, _workspace_root())
    _lock_sdist(archive)
    return filename
