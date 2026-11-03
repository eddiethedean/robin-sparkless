"""Check release metadata without importing or building the Python extension."""

from __future__ import annotations

import argparse
import ast
import re
from datetime import date
from pathlib import Path

import tomllib


def validate_versions(
    package_versions: dict[str, str],
    dependency_versions: dict[str, str],
    lock_versions: dict[str, str],
    python_version: str,
    changelog: str,
    tag: str | None = None,
    release_version: str | None = None,
) -> list[str]:
    version = package_versions["robin-sparkless"]
    api_version = release_version or version
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
    if python_version != api_version:
        errors.append(
            f"sparkless.__version__: {python_version} does not match {api_version}"
        )
    if not re.fullmatch(r"(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)", api_version):
        errors.append(
            f"Python release version is not a stable semantic version: {api_version}"
        )
    if tag is not None and tag != f"v{api_version}":
        errors.append(f"tag {tag} does not match v{api_version}")
    section = re.search(
        rf"^## \[{re.escape(api_version)}\] - (\d{{4}}-\d{{2}}-\d{{2}})$",
        changelog,
        re.MULTILINE,
    )
    if section is None:
        errors.append(f"CHANGELOG.md has no dated [{api_version}] release section")
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
    duckdb = read_toml(root / "crates/robin-sparkless-duckdb/Cargo.toml")
    polars = read_toml(root / "crates/robin-sparkless-polars/Cargo.toml")
    server_core = read_toml(root / "crates/sparkless-server-core/Cargo.toml")
    server_runtime = read_toml(root / "crates/sparkless-server-runtime/Cargo.toml")
    server_http = read_toml(root / "crates/sparkless-server-http/Cargo.toml")
    server_connect = read_toml(root / "crates/sparkless-server-connect/Cargo.toml")
    server_polars = read_toml(root / "crates/sparkless-server-polars/Cargo.toml")
    server = read_toml(root / "crates/sparkless-server/Cargo.toml")
    server_duckdb = read_toml(root / "crates/sparkless-server-duckdb/Cargo.toml")
    native = read_toml(root / "python/native/Cargo.toml")
    project = read_toml(root / "python/pyproject.toml")["project"]
    packages = {
        manifest["package"]["name"]: manifest["package"]["version"]
        for manifest in (
            facade,
            core,
            duckdb,
            polars,
            server_core,
            server_runtime,
            server_http,
            server_connect,
            server_polars,
            server,
            server_duckdb,
            native,
        )
    }
    expected = facade["package"]["version"]
    if _is_coordinated_release(expected):
        parser = read_toml(root / "crates/spark-sql-parser/Cargo.toml")
        packages[parser["package"]["name"]] = parser["package"]["version"]
    lock_versions = {
        package["name"]: package["version"]
        for package in read_toml(root / "Cargo.lock")["package"]
    }
    dependencies = {
        "facade/core": facade["dependencies"]["robin-sparkless-core"]["version"],
        "facade/polars": facade["dependencies"]["robin-sparkless-polars"]["version"],
        "polars/core": polars["dependencies"]["robin-sparkless-core"]["version"],
        "facade/duckdb": facade["dependencies"]["robin-sparkless-duckdb"]["version"],
        "polars/duckdb": polars["dependencies"]["robin-sparkless-duckdb"]["version"],
        "server/polars-adapter": server["dependencies"]["sparkless-server-polars"]["version"],
        "server-polars/facade": server_polars["dependencies"]["robin-sparkless"]["version"],
        "server-polars/core": server_polars["dependencies"]["sparkless-server-core"]["version"],
        "server-polars/runtime": server_polars["dependencies"]["sparkless-server-runtime"]["version"],
        "server-polars/http": server_polars["dependencies"]["sparkless-server-http"]["version"],
        "server-polars/connect": server_polars["dependencies"]["sparkless-server-connect"]["version"],
        "server-core/engine-core": server_core["dependencies"]["robin-sparkless-core"]["version"],
        "server-runtime/server-core": server_runtime["dependencies"]["sparkless-server-core"]["version"],
        "server-runtime/engine-core": server_runtime["dependencies"]["robin-sparkless-core"]["version"],
        "server-http/runtime": server_http["dependencies"]["sparkless-server-runtime"]["version"],
        "server-connect/server-core": server_connect["dependencies"]["sparkless-server-core"]["version"],
        "server-connect/runtime": server_connect["dependencies"]["sparkless-server-runtime"]["version"],
        "server-connect/engine-core": server_connect["dependencies"]["robin-sparkless-core"]["version"],
        "server/runtime": server["dependencies"]["sparkless-server-runtime"]["version"],
        "server-duckdb/runtime": server_duckdb["dependencies"]["sparkless-server-runtime"]["version"],
        "server-duckdb/polars-adapter": server_duckdb["dependencies"]["sparkless-server-polars"]["version"],
        "server-duckdb/core": server_duckdb["dependencies"]["sparkless-server-core"]["version"],
    }
    if _is_coordinated_release(expected):
        dependencies["polars/parser"] = polars["dependencies"]["spark-sql-parser"][
            "version"
        ]
        dependencies["server-polars/parser"] = server_polars["dependencies"]["spark-sql-parser"][
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
        release_version=project["version"],
    )
    engine = read_toml(root / "python/native/pyproject.toml")["project"]
    if engine["version"] != native["package"]["version"]:
        errors.append("sparkless-native: Python and Cargo versions differ")
    if f"sparkless-native=={engine['version']}" not in project.get("dependencies", []):
        errors.append("sparkless must pin the exact sparkless-native version")
    if any(dep.startswith("sparkless-connect") for dep in project.get("dependencies", [])):
        errors.append("sparkless local API must not depend on the server/client launcher")
    errors.extend(validate_server_inventory(root, tag))
    if tuple(map(int, expected.split(".")[:2])) >= (4, 17):
        if project.get("requires-python") != ">=3.11":
            errors.append("sparkless must require Python >=3.11")
        if "abi3-py311" not in native["dependencies"]["pyo3"]["features"]:
            errors.append("sparkless native wheels must use abi3-py311")
    return errors


def validate_server_inventory(root: Path, tag: str | None = None) -> list[str]:
    """Check the base/DuckDB server products and their package coupling."""
    facade = read_toml(root / "Cargo.toml")
    core = read_toml(root / "crates/robin-sparkless-core/Cargo.toml")
    duckdb = read_toml(root / "crates/robin-sparkless-duckdb/Cargo.toml")
    polars = read_toml(root / "crates/robin-sparkless-polars/Cargo.toml")
    parser = read_toml(root / "crates/spark-sql-parser/Cargo.toml")
    server_core = read_toml(root / "crates/sparkless-server-core/Cargo.toml")
    server_runtime = read_toml(root / "crates/sparkless-server-runtime/Cargo.toml")
    server_http = read_toml(root / "crates/sparkless-server-http/Cargo.toml")
    server_connect = read_toml(root / "crates/sparkless-server-connect/Cargo.toml")
    server_polars = read_toml(root / "crates/sparkless-server-polars/Cargo.toml")
    server = read_toml(root / "crates/sparkless-server/Cargo.toml")
    server_duckdb = read_toml(root / "crates/sparkless-server-duckdb/Cargo.toml")
    lock_versions = {
        package["name"]: package["version"]
        for package in read_toml(root / "Cargo.lock")["package"]
    }
    python = read_toml(root / "python/pyproject.toml")
    connect_package = read_toml(root / "python/connect/pyproject.toml")["project"]
    base_package = read_toml(root / "python/server/pyproject.toml")["project"]
    duckdb_package = read_toml(root / "python/server-duckdb/pyproject.toml")["project"]
    server_compat = read_toml(root / "python/server-bin/pyproject.toml")["project"]

    expected = facade["package"]["version"]
    errors = []
    if server["package"]["version"] != expected:
        errors.append(
            "sparkless-server: version {} does not match {}".format(
                server["package"]["version"], expected
            )
        )
    for name, manifest in (
        ("robin-sparkless-duckdb", duckdb),
        ("sparkless-server-core", server_core),
        ("sparkless-server-runtime", server_runtime),
        ("sparkless-server-http", server_http),
        ("sparkless-server-connect", server_connect),
        ("sparkless-server-polars", server_polars),
        ("sparkless-server", server),
        ("sparkless-server-duckdb", server_duckdb),
    ):
        if lock_versions.get(name) != manifest["package"]["version"]:
            errors.append("{}: Cargo.lock does not match its manifest".format(name))
    dependency_sets = (
        ("sparkless-server/adapter", server, "sparkless-server-polars", expected),
        ("sparkless-server-polars/facade", server_polars, "robin-sparkless", expected),
        ("sparkless-server-polars/core", server_polars, "sparkless-server-core", expected),
        ("sparkless-server-polars/runtime", server_polars, "sparkless-server-runtime", expected),
        ("sparkless-server-polars/http", server_polars, "sparkless-server-http", expected),
        ("sparkless-server-polars/connect", server_polars, "sparkless-server-connect", expected),
        ("sparkless-server-polars/parser", server_polars, "spark-sql-parser", expected),
        ("sparkless-server-core/engine-core", server_core, "robin-sparkless-core", core["package"]["version"]),
        ("sparkless-server-runtime/server-core", server_runtime, "sparkless-server-core", expected),
        ("sparkless-server-runtime/engine-core", server_runtime, "robin-sparkless-core", core["package"]["version"]),
        ("sparkless-server-http/runtime", server_http, "sparkless-server-runtime", expected),
        ("sparkless-server-connect/core", server_connect, "sparkless-server-core", expected),
        ("sparkless-server-connect/runtime", server_connect, "sparkless-server-runtime", expected),
        ("sparkless-server-connect/engine-core", server_connect, "robin-sparkless-core", core["package"]["version"]),
        ("sparkless-server/runtime", server, "sparkless-server-runtime", expected),
        ("sparkless-server-duckdb/runtime", server_duckdb, "sparkless-server-runtime", expected),
        ("sparkless-server-duckdb/adapter", server_duckdb, "sparkless-server-polars", expected),
        ("sparkless-server-duckdb/core", server_duckdb, "sparkless-server-core", expected),
        ("robin-sparkless-polars/duckdb", polars, "robin-sparkless-duckdb", expected),
        ("robin-sparkless-duckdb/core", duckdb, "robin-sparkless-core", core["package"]["version"]),
    )
    for label, manifest, dependency, package_version in dependency_sets:
        actual = manifest["dependencies"][dependency]["version"]
        if actual != package_version:
            errors.append(
                "{}/{}: dependency version {} does not match {}".format(
                    label, dependency, actual, package_version
                )
            )
    if "duckdb-source" in server.get("features", {}):
        errors.append("sparkless-server base product must not expose a DuckDB-selecting feature")
    if "robin-sparkless-duckdb" in server.get("dependencies", {}):
        errors.append("sparkless-server base product must not depend on the DuckDB source crate")
    python_products = (
        ("sparkless-connect", connect_package),
        ("sparkless-server", base_package),
        ("sparkless-server-duckdb", duckdb_package),
        ("sparkless-server-bin", server_compat),
    )
    for name, package in python_products:
        if package.get("version") != expected:
            errors.append("{}: Python distribution version does not match {}".format(name, expected))
        if package.get("requires-python") != ">=3.11":
            errors.append("{} must require Python >=3.11".format(name))
    exact_connect = "sparkless-connect=={}".format(expected)
    for name, package in (("sparkless-server", base_package), ("sparkless-server-duckdb", duckdb_package)):
        if exact_connect not in package.get("dependencies", []):
            errors.append("{}: missing exact shared launcher dependency {}".format(name, exact_connect))
        if any(dep.startswith(("sparkless-native", "sparkless==")) for dep in package.get("dependencies", [])):
            errors.append("{} must not depend on the local API/native engine".format(name))
        if "pyspark-client==4.0.5" not in package.get("optional-dependencies", {}).get("connect", []):
            errors.append("{}[connect] must pin pyspark-client==4.0.5".format(name))
    if "sparkless-server=={}".format(expected) not in server_compat.get("dependencies", []):
        errors.append("sparkless-server-bin compatibility package must forward to the base product")
    extras = python["project"].get("optional-dependencies", {})
    expected_extras = {
        "server": "sparkless-server=={}".format(expected),
        "connect": "sparkless-server[connect]=={}".format(expected),
        "server-duckdb": "sparkless-server-duckdb=={}".format(expected),
        "connect-duckdb": "sparkless-server-duckdb[connect]=={}".format(expected),
    }
    for extra, requirement in expected_extras.items():
        if requirement not in extras.get(extra, []):
            errors.append("sparkless[{}]: missing exact product dependency {}".format(extra, requirement))
    if "pyspark-client==4.0.5" not in connect_package.get("optional-dependencies", {}).get("client", []):
        errors.append("sparkless-connect[client] must pin pyspark-client==4.0.5")

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
                "sparkless-server-polars/spark-sql-parser",
                server_polars["dependencies"]["spark-sql-parser"]["version"],
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
