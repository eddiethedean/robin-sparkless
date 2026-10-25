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
    server = read_toml(root / "crates/sparkless-server/Cargo.toml")
    native = read_toml(root / "python/Cargo.toml")
    project = read_toml(root / "python/pyproject.toml")["project"]
    packages = {
        manifest["package"]["name"]: manifest["package"]["version"]
        for manifest in (facade, core, polars, server, native)
    }
    expected = facade["package"]["version"]
    if _is_coordinated_release(expected):
        parser = read_toml(root / "crates/spark-sql-parser/Cargo.toml")
        packages[parser["package"]["name"]] = parser["package"]["version"]
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
        "server/facade": server["dependencies"]["robin-sparkless"]["version"],
    }
    if "robin-sparkless-core" in server["dependencies"]:
        dependencies["server/core"] = server["dependencies"][
            "robin-sparkless-core"
        ]["version"]
    if _is_coordinated_release(expected):
        dependencies["polars/parser"] = polars["dependencies"]["spark-sql-parser"][
            "version"
        ]
        dependencies["server/parser"] = server["dependencies"]["spark-sql-parser"][
            "version"
        ]
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
    errors = validate_versions(
        packages,
        dependencies,
        lock_versions,
        python_version,
        (root / "CHANGELOG.md").read_text(),
        tag,
    )
    errors.extend(validate_server_inventory(root, tag))
    return errors


def validate_server_inventory(root: Path, tag: str | None = None) -> list[str]:
    """Check the separately distributed native server and its package coupling."""
    facade = read_toml(root / "Cargo.toml")
    core = read_toml(root / "crates/robin-sparkless-core/Cargo.toml")
    polars = read_toml(root / "crates/robin-sparkless-polars/Cargo.toml")
    parser = read_toml(root / "crates/spark-sql-parser/Cargo.toml")
    server = read_toml(root / "crates/sparkless-server/Cargo.toml")
    lock_versions = {
        package["name"]: package["version"]
        for package in read_toml(root / "Cargo.lock")["package"]
    }
    python = read_toml(root / "python/pyproject.toml")
    server_binary = read_toml(root / "python/server-bin/pyproject.toml")["project"]

    expected = facade["package"]["version"]
    errors = []
    if server["package"]["version"] != expected:
        errors.append(
            "sparkless-server: version {} does not match {}".format(
                server["package"]["version"], expected
            )
        )
    if lock_versions.get("sparkless-server") != server["package"]["version"]:
        errors.append("sparkless-server: Cargo.lock does not match its manifest")
    for dependency, package_version in (
        ("robin-sparkless", expected),
        ("robin-sparkless-core", core["package"]["version"]),
        ("spark-sql-parser", parser["package"]["version"]),
    ):
        if dependency not in server["dependencies"]:
            continue
        actual = server["dependencies"][dependency]["version"]
        if actual != package_version:
            errors.append(
                "sparkless-server/{}: dependency version {} does not match {}".format(
                    dependency, actual, package_version
                )
            )
    if server_binary["version"] != expected:
        errors.append(
            "sparkless-server-bin: version {} does not match {}".format(
                server_binary["version"], expected
            )
        )
    server_extra = python["project"].get("optional-dependencies", {}).get("server", [])
    required_extra = "sparkless-server-bin=={}".format(expected)
    if required_extra not in server_extra:
        errors.append(
            "sparkless[server]: missing exact binary dependency {}".format(
                required_extra
            )
        )

    # From the 4.16 candidate line onward, the parser joins the coordinated first-party
    # release inventory. Determine this from either the candidate manifest version or the
    # requested tag so release-candidate branches receive the same checks as tags.
    requested_version = (
        tag[1:] if tag is not None and tag.startswith("v") else tag or expected
    )
    if _is_coordinated_release(expected) or _is_coordinated_release(requested_version):
        if parser["package"]["version"] != expected:
            errors.append(
                "spark-sql-parser: version {} does not match candidate {}".format(
                    parser["package"]["version"], expected
                )
            )
        if lock_versions.get("spark-sql-parser") != parser["package"]["version"]:
            errors.append("spark-sql-parser: Cargo.lock does not match its manifest")
        for dependency, actual in (
            (
                "robin-sparkless-polars/spark-sql-parser",
                polars["dependencies"]["spark-sql-parser"]["version"],
            ),
            (
                "sparkless-server/spark-sql-parser",
                server["dependencies"]["spark-sql-parser"]["version"],
            ),
        ):
            if actual != expected:
                errors.append(
                    "{}: dependency version {} does not match candidate {}".format(
                        dependency, actual, expected
                    )
                )
    return errors


def _is_coordinated_release(version: str) -> bool:
    match = re.fullmatch(r"v?(\d+)\.(\d+)\.(\d+)", version)
    return bool(match and (int(match.group(1)), int(match.group(2))) >= (4, 16))


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
