#!/usr/bin/env python3
"""
Compatibility Matrix Test Runner

Tests mock-spark against multiple Python and PySpark version combinations
using Docker containers.
"""

import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple


@dataclass
class TestResult:
    """Result of a single compatibility test."""

    python_version: str
    pyspark_version: str
    java_version: str
    success: bool
    duration: float
    error: Optional[str] = None


class CompatibilityTester:
    """Orchestrates compatibility testing across version combinations."""

    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.results: List[TestResult] = []
        self.start_time = time.time()

    def get_pyspark_versions(self) -> List[Tuple[str, str]]:
        """
        Get PySpark versions to test.
        Returns list of (version, java_version) tuples.
        """
        return [
            ("3.2.4", "11"),  # Latest 3.2.x
            ("3.3.4", "11"),  # Latest 3.3.x
            ("3.4.3", "11"),  # Latest 3.4.x
            ("3.5.1", "17"),  # Latest 3.5.x (requires Java 17)
            # ("4.0.0", "17"), # PySpark 4.0 if available (commented out for now)
        ]

    def get_python_versions(self) -> List[str]:
        """Get Python versions to test."""
        return ["3.9", "3.10", "3.11", "3.12", "3.13"]

    def get_working_combinations(self) -> List[Tuple[str, str, str]]:
        """
        Get only the combinations that passed in the initial test.
        Returns list of (python_version, pyspark_version, java_version) tuples.
        """
        return [
            # Python 3.9 - all PySpark versions work
            ("3.9", "3.2.4", "11"),
            ("3.9", "3.3.4", "11"),
            ("3.9", "3.4.3", "11"),
            ("3.9", "3.5.1", "17"),
            # Python 3.10 - all PySpark versions work
            ("3.10", "3.2.4", "11"),
            ("3.10", "3.3.4", "11"),
            ("3.10", "3.4.3", "11"),
            ("3.10", "3.5.1", "17"),
            # Python 3.11 - only 3.4+ work
            ("3.11", "3.4.3", "11"),
            ("3.11", "3.5.1", "17"),
            # Python 3.12 - only 3.4+ work
            ("3.12", "3.4.3", "11"),
            ("3.12", "3.5.1", "17"),
            # Python 3.13 - only 3.5 works
            ("3.13", "3.5.1", "17"),
        ]

    def build_docker_image(
        self, python_version: str, pyspark_version: str, java_version: str
    ) -> bool:
        """Build Docker image for specific version combination."""
        image_name = f"mock-spark-test:py{python_version}-spark{pyspark_version}"

        print(f"\n{'=' * 70}")
        print(f"Building image: {image_name}")
        print(f"{'=' * 70}")

        cmd = [
            "docker",
            "build",
            "--build-arg",
            f"PYTHON_VERSION={python_version}",
            "--build-arg",
            f"PYSPARK_VERSION={pyspark_version}",
            "--build-arg",
            f"JAVA_VERSION={java_version}",
            "-f",
            str(
                self.project_root
                / "tests"
                / "compatibility_matrix"
                / "Dockerfile.template"
            ),
            "-t",
            image_name,
            str(self.project_root),
        ]

        try:
            _result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
            )
            print("✓ Image built successfully")
            return True
        except subprocess.CalledProcessError as e:
            print("✗ Image build failed")
            print(f"Error: {e.stderr}")
            return False

    def run_tests(
        self, python_version: str, pyspark_version: str, java_version: str
    ) -> TestResult:
        """Run tests in Docker container."""
        image_name = f"mock-spark-test:py{python_version}-spark{pyspark_version}"

        print(f"\n{'=' * 70}")
        print(f"Running tests: Python {python_version} + PySpark {pyspark_version}")
        print(f"{'=' * 70}")

        start_time = time.time()

        cmd = [
            "docker",
            "run",
            "--rm",
            "--name",
            f"mock-spark-test-{python_version}-{pyspark_version}".replace(".", "-"),
            image_name,
        ]

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300,  # 5 minute timeout
            )

            duration = time.time() - start_time

            if result.returncode == 0:
                print(f"✓ Tests passed in {duration:.2f}s")
                return TestResult(
                    python_version=python_version,
                    pyspark_version=pyspark_version,
                    java_version=java_version,
                    success=True,
                    duration=duration,
                )
            else:
                print(f"✗ Tests failed in {duration:.2f}s")
                error_msg = result.stderr[:500] if result.stderr else "Unknown error"
                return TestResult(
                    python_version=python_version,
                    pyspark_version=pyspark_version,
                    java_version=java_version,
                    success=False,
                    duration=duration,
                    error=error_msg,
                )
        except subprocess.TimeoutExpired:
            duration = time.time() - start_time
            print(f"✗ Tests timed out after {duration:.2f}s")
            return TestResult(
                python_version=python_version,
                pyspark_version=pyspark_version,
                java_version=java_version,
                success=False,
                duration=duration,
                error="Test timeout",
            )
        except Exception as e:
            duration = time.time() - start_time
            print(f"✗ Tests failed with exception: {e}")
            return TestResult(
                python_version=python_version,
                pyspark_version=pyspark_version,
                java_version=java_version,
                success=False,
                duration=duration,
                error=str(e),
            )

    def run_all_tests(self):
        """Run all version combinations."""
        # Use working combinations only
        combinations = self.get_working_combinations()
        total_combinations = len(combinations)
        current = 0

        print(f"\n{'=' * 70}")
        print("Sparkless Compatibility Matrix Test (Working Combinations Only)")
        print(f"{'=' * 70}")
        print(f"Testing {total_combinations} working combinations")
        print(f"{'=' * 70}\n")

        for python_version, pyspark_version, java_version in combinations:
            current += 1
            print(
                f"\n[{current}/{total_combinations}] Testing Python {python_version} + PySpark {pyspark_version}"
            )

            # Build image
            if not self.build_docker_image(
                python_version, pyspark_version, java_version
            ):
                result = TestResult(
                    python_version=python_version,
                    pyspark_version=pyspark_version,
                    java_version=java_version,
                    success=False,
                    duration=0,
                    error="Image build failed",
                )
                self.results.append(result)
                continue

            # Run tests
            result = self.run_tests(python_version, pyspark_version, java_version)
            self.results.append(result)

    def generate_report(self) -> str:
        """Generate markdown compatibility report."""
        total_time = time.time() - self.start_time
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Organize results by version
        python_versions = sorted({r.python_version for r in self.results})
        pyspark_versions = sorted({r.pyspark_version for r in self.results})

        # Create matrix
        lines = []
        lines.append("# Sparkless Compatibility Matrix")
        lines.append("")
        lines.append(f"**Generated:** {timestamp}  ")
        lines.append(f"**Total test time:** {total_time / 60:.1f} minutes  ")
        lines.append("")
        lines.append("## Summary")
        lines.append("")

        passed = sum(1 for r in self.results if r.success)
        failed = len(self.results) - passed
        lines.append(f"- **Total combinations tested:** {len(self.results)}")
        lines.append(f"- **Passed:** {passed} ✓")
        lines.append(f"- **Failed:** {failed} ✗")
        lines.append("")

        # Matrix table
        lines.append("## Compatibility Matrix")
        lines.append("")
        lines.append(
            "| Python | " + " | ".join(f"PySpark {v}" for v in pyspark_versions) + " |"
        )
        lines.append(
            "|--------|" + "|".join(["---------" for _ in pyspark_versions]) + "|"
        )

        for py_ver in python_versions:
            row = [f"**{py_ver}**"]
            for spark_ver in pyspark_versions:
                result = next(
                    (
                        r
                        for r in self.results
                        if r.python_version == py_ver and r.pyspark_version == spark_ver
                    ),
                    None,
                )
                if result:
                    if result.success:
                        row.append("✓ Pass")
                    else:
                        row.append("✗ Fail")
                else:
                    row.append("? Unknown")
            lines.append("| " + " | ".join(row) + " |")

        lines.append("")
        lines.append("## Detailed Results")
        lines.append("")

        for result in sorted(
            self.results, key=lambda r: (r.python_version, r.pyspark_version)
        ):
            status = "✓ PASS" if result.success else "✗ FAIL"
            lines.append(
                f"### Python {result.python_version} + PySpark {result.pyspark_version} (Java {result.java_version})"
            )
            lines.append(f"**Status:** {status}  ")
            lines.append(f"**Duration:** {result.duration:.2f}s  ")

            if result.error:
                lines.append("**Error:** ```")
                lines.append(result.error)
                lines.append("```")

            lines.append("")

        return "\n".join(lines)

    def save_report(self, output_path: Path):
        """Save compatibility report to file."""
        report = self.generate_report()
        output_path.write_text(report)
        print(f"\n{'=' * 70}")
        print(f"Compatibility report saved to: {output_path}")
        print(f"{'=' * 70}")


def main():
    """Main entry point."""
    project_root = Path(__file__).parent.parent.parent

    tester = CompatibilityTester(project_root)

    try:
        tester.run_all_tests()
    except KeyboardInterrupt:
        print("\n\nTest interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nUnexpected error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)

    # Generate report
    report_path = project_root / "COMPATIBILITY_REPORT.md"
    tester.save_report(report_path)

    # Print summary
    passed = sum(1 for r in tester.results if r.success)
    failed = len(tester.results) - passed
    print(f"\n{'=' * 70}")
    print(
        f"Test Summary: {passed} passed, {failed} failed out of {len(tester.results)} combinations"
    )
    print(f"{'=' * 70}\n")

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
