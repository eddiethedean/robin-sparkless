#!/usr/bin/env python3
"""
Diagnostic script to identify which tests are hanging.

This script runs tests with timeouts and reports which ones hang.
"""

import subprocess
import sys
from pathlib import Path


def run_test_with_timeout(test_path, timeout=10):
    """Run a single test with a timeout and return the result."""
    print(f"Testing: {test_path}")
    sys.stdout.flush()

    try:
        result = subprocess.run(
            [sys.executable, "-m", "pytest", test_path, "-v", "--tb=line"],
            timeout=timeout,
            capture_output=True,
            text=True,
        )
        return {
            "path": test_path,
            "status": "completed" if result.returncode == 0 else "failed",
            "timeout": False,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "returncode": result.returncode,
        }
    except subprocess.TimeoutExpired:
        return {
            "path": test_path,
            "status": "timeout",
            "timeout": True,
            "stdout": "",
            "stderr": "",
            "returncode": -1,
        }


def find_test_files():
    """Find all test files."""
    test_files = []
    for path in Path("tests").rglob("test_*.py"):
        test_files.append(str(path))
    return sorted(test_files)


def main():
    """Main diagnostic function."""
    print("=" * 60)
    print("Diagnosing Hanging Tests")
    print("=" * 60)
    print()

    # Find all test files
    test_files = find_test_files()
    print(f"Found {len(test_files)} test files")
    print()

    # Test each file individually with timeout
    hanging_tests = []
    working_tests = []
    failed_tests = []

    for test_file in test_files:
        print(f"\n{'=' * 60}")
        print(f"Testing: {test_file}")
        print(f"{'=' * 60}")

        result = run_test_with_timeout(test_file, timeout=30)

        if result["timeout"]:
            hanging_tests.append(result)
            print(f"❌ HANGING: {test_file}")
        elif result["status"] == "failed":
            failed_tests.append(result)
            print(f"⚠️  FAILED: {test_file}")
        else:
            working_tests.append(result)
            print(f"✅ PASSED: {test_file}")

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total tests: {len(test_files)}")
    print(f"✅ Working: {len(working_tests)}")
    print(f"⚠️  Failed: {len(failed_tests)}")
    print(f"❌ Hanging: {len(hanging_tests)}")

    if hanging_tests:
        print("\n" + "=" * 60)
        print("HANGING TESTS:")
        print("=" * 60)
        for test in hanging_tests:
            print(f"  - {test['path']}")

    if failed_tests:
        print("\n" + "=" * 60)
        print("FAILED TESTS:")
        print("=" * 60)
        for test in failed_tests:
            print(f"  - {test['path']} (return code: {test['returncode']})")

    return 0 if not hanging_tests else 1


if __name__ == "__main__":
    sys.exit(main())
