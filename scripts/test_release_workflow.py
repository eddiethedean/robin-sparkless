"""Check publication boundaries and execute the workflows' actual gate commands."""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import yaml

import release_context


ROOT = Path(__file__).resolve().parents[1]


def workflow(name: str) -> dict:
    return yaml.safe_load((ROOT / ".github/workflows" / name).read_text())


def run_script(script: str, **environment: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["bash", "-e", "-o", "pipefail", "-c", script],
        env={**os.environ, **environment},
        text=True,
        capture_output=True,
        timeout=10,
    )


class ReleaseContextTests(unittest.TestCase):
    def test_publication_targets(self):
        cases = [
            ("push", "refs/heads/codex/release-4.18", False, (False, False)),
            ("push", "refs/tags/v4.18.0", False, (True, True)),
            ("workflow_dispatch", "refs/heads/main", False, (False, False)),
            ("workflow_dispatch", "refs/heads/main", True, (False, True)),
            ("workflow_dispatch", "refs/heads/topic", False, (False, False)),
            ("workflow_dispatch", "refs/tags/v4.18.0", False, (False, False)),
        ]
        for event, ref, rust, expected in cases:
            with self.subTest(event=event, ref=ref, rust=rust):
                self.assertEqual(
                    release_context.publication_targets(event, ref, rust), expected
                )

    def test_unsupported_publication_is_rejected(self):
        cases = [
            ("push", "refs/tags/v4.18.0rc1", False),
            ("push", "refs/tags/v4.18", False),
            ("push", "refs/heads/main", False),
            ("push", "refs/tags/v4.18.0", True),
            ("workflow_dispatch", "refs/heads/topic", True),
            ("workflow_dispatch", "refs/tags/v4.18.0", True),
            ("pull_request", "refs/tags/v4.18.0", False),
        ]
        for case in cases:
            with self.subTest(context=case), self.assertRaises(ValueError):
                release_context.publication_targets(*case)

    def test_metadata_failure_emits_no_publication_outputs(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "output"
            environment = {
                "GITHUB_EVENT_NAME": "push",
                "GITHUB_REF": "refs/tags/v4.18.0",
                "GITHUB_OUTPUT": str(output),
            }
            with (
                patch.dict(os.environ, environment),
                patch.object(
                    release_context, "check_release", return_value=["version mismatch"]
                ),
                self.assertRaises(SystemExit),
            ):
                release_context.main()
            self.assertFalse(output.exists())


class ReleaseWorkflowTests(unittest.TestCase):
    def test_server_ci_job_allows_time_for_cold_builds(self):
        jobs = workflow("ci.yml")["jobs"]
        self.assertGreaterEqual(jobs["server-base-tests"]["timeout-minutes"], 90)
        self.assertGreaterEqual(jobs["server-duckdb-tests"]["timeout-minutes"], 90)
        base_steps = "\n".join(
            step.get("run", "") for step in jobs["server-base-tests"]["steps"]
        )
        duckdb_steps = "\n".join(
            step.get("run", "") for step in jobs["server-duckdb-tests"]["steps"]
        )
        self.assertIn("-p sparkless-server ", base_steps)
        self.assertNotIn("-p sparkless-server-duckdb ", base_steps)
        self.assertIn("-p sparkless-server-duckdb ", duckdb_steps)
        self.assertNotIn("-p sparkless-server ", duckdb_steps)

    def test_python_catalog_writer_test_jobs_install_pyarrow(self):
        test_jobs = (
            ("ci.yml", "python"),
            ("ci.yml", "python-full"),
            ("release.yml", "python-tests"),
        )
        for workflow_name, job_name in test_jobs:
            with self.subTest(workflow=workflow_name, job=job_name):
                steps = workflow(workflow_name)["jobs"][job_name]["steps"]
                install = next(
                    step["run"]
                    for step in steps
                    if "pip install" in step.get("run", "")
                    and "pytest" in step.get("run", "")
                )
                for dependency in ("pandas", "polars", "pyarrow"):
                    with self.subTest(dependency=dependency):
                        self.assertIn(dependency, install)

    def test_new_pypi_projects_use_token_and_existing_projects_use_oidc(self):
        jobs = workflow("release.yml")["jobs"]
        metadata_steps = jobs["release-metadata"]["steps"]
        token_preflight = next(
            step
            for step in metadata_steps
            if step.get("name") == "Require the PyPI API token for new projects"
        )
        self.assertEqual(
            token_preflight.get("if"),
            "steps.context.outputs.publish_python == 'true'",
        )
        self.assertEqual(
            token_preflight.get("env", {}).get("PYPI_API_TOKEN"),
            "${{ secrets.PYPI_API_TOKEN }}",
        )
        self.assertIn("-z \"${PYPI_API_TOKEN:-}\"", token_preflight["run"])

        token_publishers = (
            "publish-connect",
            "publish-server-products",
            "publish-testing",
        )
        for name in token_publishers:
            with self.subTest(publisher=name):
                job = jobs[name]
                self.assertNotIn("id-token", job["permissions"])
                publish_step = next(
                    step
                    for step in job["steps"]
                    if step.get("uses", "").startswith("pypa/gh-action-pypi-publish@")
                )
                self.assertEqual(
                    publish_step["with"].get("password"),
                    "${{ secrets.PYPI_API_TOKEN }}",
                )
                self.assertIs(publish_step["with"].get("attestations"), False)

        oidc_publishers = ("publish-native", "publish-sparkless")
        for name in oidc_publishers:
            with self.subTest(publisher=name):
                job = jobs[name]
                self.assertEqual(job["permissions"].get("id-token"), "write")
                publish_step = next(
                    step
                    for step in job["steps"]
                    if step.get("uses", "").startswith("pypa/gh-action-pypi-publish@")
                )
                self.assertNotIn("password", publish_step.get("with", {}))

    def test_419_server_products_are_built_and_verified_separately(self):
        jobs = workflow("release.yml")["jobs"]
        products = {
            "server-base-manylinux-x86_64": (
                "sparkless-server",
                "sparkless-server-base-manylinux-2-28-x86_64",
            ),
            "server-duckdb-manylinux-x86_64": (
                "sparkless-server-duckdb",
                "sparkless-server-duckdb-manylinux-2-28-x86_64",
            ),
            "server-base-macos-arm64": (
                "sparkless-server",
                "sparkless-server-base-macosx-14-0-arm64",
            ),
            "server-duckdb-macos-arm64": (
                "sparkless-server-duckdb",
                "sparkless-server-duckdb-macosx-14-0-arm64",
            ),
        }
        for job_name, (crate, artifact) in products.items():
            with self.subTest(job=job_name):
                job = jobs[job_name]
                steps = job["steps"]
                commands = "\n".join(step.get("run", "") for step in steps)
                builds = [
                    step
                    for step in steps
                    if (
                        step.get("name", "").startswith("Build the ")
                        and "server product" in step.get("name", "")
                    )
                    or step.get("name") == "Build the base server without DuckDB"
                ]
                self.assertEqual(len(builds), 1)
                build = builds[0]
                self.assertIn(f"-p {crate} ", build["run"])
                self.assertIn("--features http,connect", build["run"])
                product_flag = "duckdb" if crate.endswith("duckdb") else "base"
                project_path = (
                    "python/server-duckdb/pyproject.toml"
                    if crate.endswith("duckdb")
                    else "python/server/pyproject.toml"
                )
                self.assertIn(f"--product {product_flag}", commands)
                self.assertIn(project_path, commands)
                uploads = [
                    step.get("with", {}).get("name")
                    for step in steps
                    if step.get("uses", "").startswith("actions/upload-artifact@")
                ]
                self.assertIn(artifact, uploads)
                other_crate = (
                    "sparkless-server-duckdb"
                    if crate == "sparkless-server"
                    else "sparkless-server"
                )
                self.assertNotIn(f"-p {other_crate} ", commands)
                if crate.endswith("duckdb"):
                    self.assertIn(
                        "Fetch the signed DuckDB 2.0.0 Quack extension",
                        [step.get("name") for step in steps],
                    )
                    self.assertIn("SPARKLESS_QUACK_EXTENSION_BINARY", commands)
                    self.assertIn(
                        "Build the DuckDB source owner fixture",
                        [step.get("name") for step in steps],
                    )
                else:
                    self.assertNotIn("SPARKLESS_QUACK_EXTENSION_BINARY", commands)
                    self.assertNotIn("duckdb_quack_fixture_owner", commands)

        graph_job = jobs["server-feature-graphs"]
        graph_step = next(
            step
            for step in graph_job["steps"]
            if step.get("name") == "Record isolated product feature graphs"
        )
        self.assertEqual(graph_step.get("env", {}).get("RUSTUP_TOOLCHAIN"), "1.93.1")
        self.assertIn("server-feature-graphs", str(graph_job))
        self.assertNotIn(
            "server-feature-graphs", str(jobs["server-base-manylinux-x86_64"])
        )
        self.assertEqual(
            set(jobs["server-container"]["needs"]),
            {"server-base-manylinux-x86_64", "server-duckdb-manylinux-x86_64"},
        )
        self.assertEqual(
            set(jobs["server-product-consumer"]["needs"]),
            {
                "build-wheels",
                "source-dist",
                "server-base-manylinux-x86_64",
                "server-duckdb-manylinux-x86_64",
                "server-base-macos-arm64",
                "server-duckdb-macos-arm64",
            },
        )

        consumer = "\n".join(
            step.get("run", "") for step in jobs["server-product-consumer"]["steps"]
        )
        self.assertIn("duckdb-source-py311.lock", consumer)
        self.assertIn("smoke_phase419_duckdb_client.py", consumer)
        self.assertIn("smoke_phase419_duckdb_shutdown.py", consumer)
        self.assertIn("differential_phase419_duckdb.py", consumer)
        self.assertIn("oracle-py311.lock", consumer)
        self.assertIn("quack_extension_path", consumer)
        self.assertIn(
            "duckdb-quack-fixture-owner", str(jobs["server-product-consumer"])
        )
        self.assertIn("--profile coinstall", consumer)
        self.assertIn("--profile client-only", consumer)
        self.assertIn("--profile local-only", consumer)
        self.assertIn('"sparkless[testing]==${version}"', consumer)
        self.assertIn("check_testing_package_boundary.py", consumer)
        self.assertNotIn("--legacy-forwarder", consumer)
        container_build = "\n".join(
            step.get("run", "") for step in jobs["server-container"]["steps"]
        )
        self.assertIn("Dockerfile.release", container_build)
        self.assertIn("Dockerfile.duckdb.release", container_build)
        dockerfile = (ROOT / "crates/sparkless-server/Dockerfile.release").read_text()
        duckdb_dockerfile = (
            ROOT / "crates/sparkless-server/Dockerfile.duckdb.release"
        ).read_text()
        self.assertNotIn("quack.duckdb_extension", dockerfile)
        self.assertIn("COPY quack.duckdb_extension", duckdb_dockerfile)
        self.assertIn("NOTICE-quack.txt", duckdb_dockerfile)

    def test_every_validation_job_is_required_before_publishing(self):
        jobs = workflow("release.yml")["jobs"]
        gate = jobs["release-validation-gate"]
        publishers = {name for name in jobs if name.startswith("publish")}
        self.assertEqual(
            set(gate["needs"]), set(jobs) - publishers - {"release-validation-gate"}
        )
        self.assertEqual(gate["if"], "always()")
        for name in publishers:
            publisher = jobs[name]
            with self.subTest(publisher=name):
                self.assertIn("release-validation-gate", publisher["needs"])
                # Default success() must block publishers when a prerequisite fails.
                self.assertNotIn("always()", publisher["if"])
                self.assertIn("outputs.publish_", publisher["if"])

    def test_split_native_engine_is_verified_and_published_before_the_api(self):
        jobs = workflow("release.yml")["jobs"]
        self.assertIn("publish-native", jobs["publish-sparkless"]["needs"])
        self.assertIn("release-validation-gate", jobs["publish-native"]["needs"])
        gate_steps = jobs["release-validation-gate"]["steps"]
        downloads = [step.get("with", {}).get("name") for step in gate_steps]
        self.assertIn("sparkless-api-dist", downloads)
        self.assertIn("verified-native-candidate", downloads)
        verification = next(
            step["run"]
            for step in gate_steps
            if step.get("name")
            == "Verify complete wheel and source distribution inventory"
        )
        self.assertIn("verify_candidate_inventory.py", verification)
        self.assertIn("--root .", verification)
        self.assertIn(
            "verified-candidate-inventory",
            [
                step.get("with", {}).get("name")
                for step in gate_steps
                if step.get("uses", "").startswith("actions/upload-artifact@")
            ],
        )
        storage_check = next(
            step["run"]
            for step in gate_steps
            if step.get("name") == "Check current PyPI storage before publishing"
        )
        self.assertIn("--distribution sparkless=dist/sparkless", storage_check)
        self.assertIn("--distribution sparkless-native=dist/native", storage_check)
        for distribution in (
            "sparkless-server=dist/server-base",
            "sparkless-server-duckdb=dist/server-duckdb",
            "sparkless-connect=dist/connect",
            "sparkless-testing=dist/testing",
        ):
            self.assertIn("--distribution " + distribution, storage_check)

    def test_all_publishers_check_the_exact_candidate_inventory_hashes(self):
        jobs = workflow("release.yml")["jobs"]
        publishers = {
            "publish-native": ["sparkless-native"],
            "publish-connect": ["sparkless-connect"],
            "publish-server-products": ["sparkless-server", "sparkless-server-duckdb"],
            "publish-testing": ["sparkless-testing"],
            "publish-sparkless": ["sparkless"],
        }
        for name, projects in publishers.items():
            steps = jobs[name]["steps"]
            script = "\n".join(step.get("run", "") for step in steps)
            with self.subTest(publisher=name):
                self.assertIn("verified-candidate-inventory", str(steps))
                self.assertIn("verify_candidate_inventory.py", script)
                for project in projects:
                    self.assertIn(project, script)

    def test_rust_publisher_paces_new_crates_and_honors_rate_limits(self):
        jobs = workflow("release.yml")["jobs"]
        script = next(
            step["run"]
            for step in jobs["publish"]["steps"]
            if step.get("name") == "Publish to crates.io in dependency order"
        )
        self.assertIn("last_new_publish", script)
        self.assertIn("sleep \"$wait_seconds\"", script)
        self.assertIn("last_new_publish + 300", script)
        self.assertIn("429 Too Many Requests", script)
        self.assertIn("Please try again after", script)

    def test_testing_package_is_staged_and_published_before_the_api(self):
        jobs = workflow("release.yml")["jobs"]
        self.assertIn("publish-testing", jobs)
        self.assertIn("publish-testing", jobs["publish-sparkless"]["needs"])
        gate = jobs["release-validation-gate"]["steps"]
        downloads = [step.get("with", {}).get("name") for step in gate]
        self.assertIn("sparkless-testing-dist", downloads)
        self.assertIn(
            "verified-testing-candidate",
            [
                step.get("with", {}).get("name")
                for step in jobs["publish-testing"]["steps"]
            ],
        )
        self.assertNotIn("publish-server-compat", jobs)

    def test_release_gate_rejects_failed_cancelled_and_skipped_jobs(self):
        gate = workflow("release.yml")["jobs"]["release-validation-gate"]
        script = gate["steps"][0]["run"]
        statuses = ["success"] * len(gate["needs"])
        self.assertEqual(run_script(script, RESULTS=",".join(statuses)).returncode, 0)
        for index, job in enumerate(gate["needs"]):
            for status in ("failure", "cancelled", "skipped"):
                with self.subTest(job=job, status=status):
                    changed = statuses.copy()
                    changed[index] = status
                    self.assertNotEqual(
                        run_script(script, RESULTS=",".join(changed)).returncode, 0
                    )

    @unittest.skipUnless(shutil.which("jq"), "jq is required for CI gate tests")
    def test_ci_gate_allows_only_intentional_manual_skips(self):
        gate = workflow("ci.yml")["jobs"]["ci-acceptance"]
        script = gate["steps"][0]["run"]
        for event in ("push", "pull_request", "workflow_dispatch"):
            results = {name: {"result": "success"} for name in gate["needs"]}
            if event != "workflow_dispatch":
                for name in ("pyspark-smoke", "python-full"):
                    results[name]["result"] = "skipped"
            environment = {"CI_EVENT_NAME": event, "NEEDS_JSON": json.dumps(results)}
            self.assertEqual(run_script(script, **environment).returncode, 0)
            for name in gate["needs"]:
                for status in ("failure", "cancelled", "skipped"):
                    if status == results[name]["result"]:
                        continue
                    with self.subTest(event=event, job=name, status=status):
                        changed = {**results, name: {"result": status}}
                        self.assertNotEqual(
                            run_script(
                                script,
                                CI_EVENT_NAME=event,
                                NEEDS_JSON=json.dumps(changed),
                            ).returncode,
                            0,
                        )

    @unittest.skipUnless(shutil.which("jq"), "jq is required for CI reuse tests")
    def test_publication_requires_ci_success_for_the_same_commit(self):
        metadata = workflow("release.yml")["jobs"]["release-metadata"]
        step = metadata["steps"][-1]
        self.assertEqual(step["if"], "steps.context.outputs.publish_rust == 'true'")
        # Emulate gh's successful-run response and execute the actual jq filter.
        script = 'gh() { printf "%s" "$CI_RUNS"; }\n' + step["run"]
        cases = [
            ([], False),
            ([{"event": "push", "headSha": "other"}], False),
            ([{"event": "pull_request", "headSha": "candidate"}], False),
            ([{"event": "push", "headSha": "candidate"}], True),
            ([{"event": "workflow_dispatch", "headSha": "candidate"}], True),
        ]
        with tempfile.TemporaryDirectory() as directory:
            for runs, accepted in cases:
                with self.subTest(runs=runs):
                    result = run_script(
                        script,
                        CI_RUNS=json.dumps(runs),
                        GITHUB_SHA="candidate",
                        RUNNER_TEMP=directory,
                    )
                    self.assertEqual(result.returncode == 0, accepted, result.stderr)


if __name__ == "__main__":
    unittest.main()
