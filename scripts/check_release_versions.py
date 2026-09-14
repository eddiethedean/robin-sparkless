"""Check release metadata without importing or building the Python extension."""

from __future__ import annotations

import argparse
import ast
import re
from datetime import date
from pathlib import Path

try:
    import tomllib
except ImportError:  # Python 3.8–3.10 developer environments.
    import tomli as tomllib


def validate_versions(
    package_versions: dict[str, str],
    dependency_versions: dict[str, str],
    lock_versions: dict[str, str],
    python_version: str,
    changelog: str,
    tag: str | None = None,
) -> list[str]:
    version = package_versions["robin-sparkless"]
    errors = []
    if not re.fullmatch(r"(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)", version):
        errors.append(f"release version is not a stable semantic version: {version}")
    for name, actual in package_versions.items():
        if actual != version:
            errors.append(f"{name}: version {actual} does not match {version}")
        if lock_versions.get(name) != actual:
            errors.append(
                f"{name}: Cargo.lock does not match manifest version {actual}"
            )
    for name, actual in dependency_versions.items():
        if actual != version:
            errors.append(
                f"{name}: dependency version {actual} does not match {version}"
            )
    if python_version != version:
        errors.append(
            f"sparkless.__version__: {python_version} does not match {version}"
        )
    if tag is not None and tag != f"v{version}":
        errors.append(f"tag {tag} does not match v{version}")
    section = re.search(
        rf"^## \[{re.escape(version)}\] - (\d{{4}}-\d{{2}}-\d{{2}})$",
        changelog,
        re.MULTILINE,
    )
    if section is None:
        errors.append(f"CHANGELOG.md has no dated [{version}] release section")
    else:
        try:
            date.fromisoformat(section.group(1))
        except ValueError:
            errors.append("CHANGELOG.md release date is invalid")
    return errors


def read_toml(path: Path) -> dict:
    with path.open("rb") as source:
        return tomllib.load(source)


def check_release(root: Path, tag: str | None = None) -> list[str]:
    facade = read_toml(root / "Cargo.toml")
    core = read_toml(root / "crates/robin-sparkless-core/Cargo.toml")
    polars = read_toml(root / "crates/robin-sparkless-polars/Cargo.toml")
    native = read_toml(root / "python/Cargo.toml")
    project = read_toml(root / "python/pyproject.toml")["project"]
    packages = {
        manifest["package"]["name"]: manifest["package"]["version"]
        for manifest in (facade, core, polars, native)
    }
    packages["sparkless"] = project["version"]
    lock_versions = {
        package["name"]: package["version"]
        for package in read_toml(root / "Cargo.lock")["package"]
    }
    # The Python distribution shares its version with the native lockfile entry.
    lock_versions["sparkless"] = lock_versions.get("sparkless-native", "")
    dependencies = {
        "facade/core": facade["dependencies"]["robin-sparkless-core"]["version"],
        "facade/polars": facade["dependencies"]["robin-sparkless-polars"]["version"],
        "polars/core": polars["dependencies"]["robin-sparkless-core"]["version"],
    }
    module = ast.parse((root / "python/sparkless/sparkless/__init__.py").read_text())
    python_version = next(
        node.value.value
        for node in module.body
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "__version__"
            for target in node.targets
        )
        and isinstance(node.value, ast.Constant)
        and isinstance(node.value.value, str)
    )
    return validate_versions(
        packages,
        dependencies,
        lock_versions,
        python_version,
        (root / "CHANGELOG.md").read_text(),
        tag,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tag", help="Tag being released, including its v prefix")
    args = parser.parse_args()
    try:
        errors = check_release(Path(__file__).resolve().parents[1], args.tag)
    except (OSError, KeyError, StopIteration, SyntaxError, ValueError) as error:
        errors = [f"cannot read release metadata: {error}"]
    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1
    print(
        "Release manifests, dependency versions, lockfile, Python version, and changelog agree"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
