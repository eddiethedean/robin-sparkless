"""Audit the downstream Cargo graph without inheriting workspace patches."""

from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import tomllib

from verify_release_source import verify_graph


def audit_decision(
    report: dict, returncode: int, policy: dict | None, release: str, today: date
) -> dict:
    """Keep raw findings visible; waive only explicitly accepted registry tuples."""
    vulnerabilities = report["vulnerabilities"]
    findings = vulnerabilities["list"]
    if (
        returncode not in (0, 1)
        or report.get("error")
        or vulnerabilities["count"] != len(findings)
        or vulnerabilities["found"] != bool(findings)
        or bool(returncode) != bool(findings)
    ):
        raise ValueError("Cargo audit failed or returned an inconsistent report")
    decision = {
        "release": release,
        "evaluated_on": today.isoformat(),
        "accepted": [],
        "unaccepted": [],
        "status": "clean",
    }
    if not findings:
        return decision
    allowed = set()
    if policy is not None:
        if (
            policy["schema_version"] != 1
            or policy["release"] != release
            or not policy["approved_on"] <= today <= policy["expires_on"]
            or not policy["rationale"].strip()
            or not policy["tracking"].strip()
        ):
            raise ValueError(
                "security exception expired or does not cover this release"
            )
        allowed = {
            (item["advisory"], item["package"], item["version"])
            for item in policy["exceptions"]
        }
        decision["expires_on"] = policy["expires_on"].isoformat()
        decision["tracking"] = policy["tracking"]
    for finding in findings:
        package = finding["package"]
        key = (finding["advisory"]["id"], package["name"], package["version"])
        entry = {"advisory": key[0], "package": key[1], "version": key[2]}
        accepted = key in allowed and package["source"] == (
            "registry+https://github.com/rust-lang/crates.io-index"
        )
        decision["accepted" if accepted else "unaccepted"].append(entry)
    decision["status"] = "blocked" if decision["unaccepted"] else "accepted_exception"
    return decision


def consumer_manifest(source: str) -> str:
    manifest = re.sub(r"(?ms)^\[patch\.crates-io\]\n.*?(?=^\[|\Z)", "", source)
    manifest = re.sub(
        r"(?m)^members\s*=\s*\[[^\]]*\]",
        'members = [".", "crates/robin-sparkless-core", '
        '"crates/robin-sparkless-polars", "crates/spark-sql-parser", '
        '"crates/sparkless-server"]',
        manifest,
        count=1,
    )
    if "patch" in tomllib.loads(manifest):
        raise ValueError("consumer manifest still contains Cargo patches")
    return manifest


def verify_consumer_features(metadata: dict) -> None:
    affected = {
        p["id"]
        for p in metadata["packages"]
        if p["name"] == "zstd-safe" and p["version"] == "7.3.0"
    }
    if any(
        node["id"] in affected and "seekable" in node["features"]
        for node in metadata["resolve"]["nodes"]
    ):
        raise ValueError("the zstd exception does not cover the seekable feature")


def release_exception_policy_path(root: Path, release: str) -> Path:
    """Return the exception policy scoped to this release series."""
    series = ".".join(release.split(".")[:2])
    return root / "security" / "{}-exceptions.toml".format("zstd-{}".format(series))


def verify(
    root: Path,
    output: Path,
    offline: bool = False,
    strict: bool = False,
    patch_config: Path | None = None,
) -> None:
    # Exceptions never cover our shipped binaries, wheels, containers or sdist.
    command = [
        "cargo",
        "metadata",
        "--locked",
        "--format-version",
        "1",
        "--all-features",
    ]
    if offline:
        command.append("--offline")
    verify_graph(json.loads(subprocess.check_output(command, cwd=root)), root)
    release = tomllib.loads((root / "Cargo.toml").read_text())["package"]["version"]
    with tempfile.TemporaryDirectory(prefix="sparkless-rust-consumer-") as scratch:
        consumer = Path(scratch)
        for name in ("src", "crates", "examples", "benches"):
            shutil.copytree(
                root / name,
                consumer / name,
                ignore=shutil.ignore_patterns("target", ".git"),
            )
        (consumer / "Cargo.toml").write_text(
            consumer_manifest((root / "Cargo.toml").read_text())
        )
        shutil.copyfile(root / "Cargo.lock", consumer / "Cargo.lock")
        command = ["cargo", "metadata", "--format-version", "1", "--all-features"]
        if patch_config is not None:
            command.extend(["--config", str(patch_config.resolve())])
        if offline:
            command.append("--offline")
        metadata = json.loads(subprocess.check_output(command, cwd=consumer))
        if patch_config is not None:
            verify_graph(metadata, consumer)
        command = ["cargo", "audit", "--file", str(consumer / "Cargo.lock"), "--json"]
        if offline:
            command.append("--no-fetch")
        result = subprocess.run(command, cwd=root, stdout=subprocess.PIPE, check=False)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(result.stdout)
        policy = None
        if not strict and patch_config is None:
            policy_path = release_exception_policy_path(root, release)
            if policy_path.is_file():
                policy = tomllib.loads(policy_path.read_text())
        decision_path = output.with_suffix(".exceptions.json")
        try:
            verify_consumer_features(metadata)
            decision = audit_decision(
                json.loads(result.stdout),
                result.returncode,
                policy,
                release,
                datetime.now(timezone.utc).date(),
            )
        except (ValueError, KeyError, TypeError) as error:
            decision_path.write_text(
                json.dumps({"status": "blocked", "error": str(error)}, indent=2) + "\n"
            )
            raise RuntimeError(
                "invalid downstream audit or exception policy"
            ) from error
        decision["audit_sha256"] = hashlib.sha256(result.stdout).hexdigest()
        decision_path.write_text(json.dumps(decision, indent=2) + "\n")
        if decision["status"] == "blocked":
            raise RuntimeError(
                "downstream Rust dependency audit failed; see " + str(output)
            )
    print("downstream Rust audit: " + decision["status"])
    if decision["accepted"]:
        print(
            "WARNING: accepted release-only security exceptions; see "
            + str(decision_path)
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--offline", action="store_true")
    parser.add_argument(
        "--strict", action="store_true", help="reject all release exceptions"
    )
    parser.add_argument(
        "--patch-config",
        type=Path,
        help="verify consumer-supplied Cargo patches without exceptions",
    )
    args = parser.parse_args()
    verify(
        Path(__file__).resolve().parents[1],
        args.output.resolve(),
        args.offline,
        args.strict,
        args.patch_config,
    )


if __name__ == "__main__":
    main()
